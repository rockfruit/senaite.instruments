# -*- coding: utf-8 -*-
#
# This file is part of SENAITE.INSTRUMENTS.
#
# SENAITE.CORE is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, version 2.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#
# Copyright 2018-2019 by it's authors.
# Some rights reserved, see README and LICENSE.
import csv
import json
import traceback
from mimetypes import guess_type
from os.path import abspath
from os.path import splitext
from re import subn

from bika.lims.catalog import BIKA_CATALOG
from senaite.core.exportimport.instruments import IInstrumentAutoImportInterface
from senaite.core.exportimport.instruments import IInstrumentImportInterface
from senaite.core.exportimport.instruments.resultsimport import \
    AnalysisResultsImporter
from senaite.core.exportimport.instruments.resultsimport import \
    InstrumentResultsFileParser

from bika.lims import api
from bika.lims import bikaMessageFactory as _
from bika.lims.catalog import CATALOG_ANALYSIS_REQUEST_LISTING
from senaite.instruments.instrument import FileStub
from senaite.instruments.instrument import SheetNotFound
from senaite.instruments.instrument import xls_to_csv
from senaite.instruments.instrument import xlsx_to_csv
from zope.interface import implements
from zope.publisher.browser import FileUpload


class MultipleAnalysesFound(Exception):
    pass


class AnalysisNotFound(Exception):
    pass


class Winlab32(InstrumentResultsFileParser):
    ar = None

    def __init__(self, infile, worksheet=None, encoding=None, delimiter=None):
        self.delimiter = delimiter if delimiter else ','
        self.encoding = encoding
        self.infile = infile
        self.csv_data = None
        self.worksheet = worksheet if worksheet else 0
        self.sample_id = None
        mimetype = guess_type(self.infile.filename)
        InstrumentResultsFileParser.__init__(self, infile, mimetype)

    def parse(self):
        order = []
        filename = str(self.infile.filename)
        ext = splitext(filename.lower())[-1]
        self.csv_data = None
        if ext == '.xlsx':
            order = (xlsx_to_csv, xls_to_csv)
        elif ext == '.xls':
            order = (xls_to_csv, xlsx_to_csv)
        elif ext == '.csv':
            self.csv_data = self.infile
        else:
            self.err("%s is not an XLS, XLSX, or CSV document" % filename)
            return -1

        for importer in order:
            try:
                self.csv_data = importer(
                    infile=self.infile,
                    worksheet=self.worksheet,
                    delimiter=",")
                break
            except SheetNotFound:
                self.err("Sheet not found in workbook: %s" % self.worksheet)
                return -1
            except Exception as e:  # noqa
                pass

        if not self.csv_data:
            self.warn("Can't parse input file as XLS, XLSX, or CSV.")
            return -1

        stub = FileStub(file=self.csv_data, name=str(self.infile.filename))
        self.csv_data = FileUpload(stub)

        lines = self.csv_data.readlines()
        reader = csv.DictReader(lines)
        for row in reader:
            self.parse_row(reader.line_num, row)
        return 0

    def getAnalysisKeywords(self):
        return InstrumentResultsFileParser.getAnalysisKeywords(self)

    def parse_row(self, row_nr, row):
        # convert row to use interim field names
        try:
            value = float(row['Reported Conc (Calib)'])
        except (TypeError, ValueError):
            value = row['Reported Conc (Calib)']
        parsed = {'reading': value, 'DefaultResult': 'reading'}

        # sample ID
        sample_id = subn(r'[^\w\d\-_]*', '', row.get('Sample ID', ""))[0]
        if not sample_id:
            return 0

        # maybe AR
        ar = self.get_ar(sample_id)
        if ar:
            kw = subn(r"[^\w\d]*", "", row.get('Analyte Name', ""))[0]
            if not kw:
                return 0
            all_ans = ar.getAnalyses()
            kw_analyses = [a for a in all_ans if a.getKeyword.startswith(kw)]
            if not kw_analyses:
                self.warn("No analysis found matching Keyword '${kw}'",
                          mapping=dict(kw=kw))
            elif len(kw_analyses) > 1:
                self.warn('Multiple analyses found matching Keyword "${kw}"',
                          mapping=dict(kw=kw))
            else:
                analysis_keyword = kw_analyses[0].getKeyword
                self._addRawResult(sample_id, {analysis_keyword: parsed})
            return 0

        # maybe Reference Analysis
        ref_an = self.get_ref_an(sample_id)
        if ref_an:
            parsed = {'result': value, 'DefaultResult': 'result'}
            self._addRawResult(sample_id, parsed)
            return 0

        self.warn('Sample not found for ${sid}', mapping={'sid': sample_id})
        return 0

    def get_ar(self, sample_id):
        query = dict(portal_type="AnalysisRequest", getId=sample_id)
        brains = api.search(query, CATALOG_ANALYSIS_REQUEST_LISTING)
        try:
            return api.get_object(brains[0])
        except IndexError:
            pass

    def get_ref_an(self, an_id):
        query = dict(portal_type='ReferenceAnalysis',
                     getReferenceAnalysesGroupID=an_id)
        brains = api.search(query, 'bika_analysis_catalog')
        try:
            return api.get_object(brains[0])
        except IndexError:
            pass


class importer(object):
    implements(IInstrumentImportInterface, IInstrumentAutoImportInterface)
    title = "Perkin Elmer Winlab32"
    __file__ = abspath(__file__)  # noqa

    def __init__(self, context):
        self.context = context
        self.request = None

    @staticmethod
    def Import(context, request):

        infile = request.form['instrument_results_file']
        if not hasattr(infile, 'filename'):
            results = {'errors': [_('No file selected')],
                       'log': [],
                       'warns': []}
            return json.dumps(results)

        artoapply = request.form['artoapply']
        override = request.form['results_override']
        instrument = request.form.get('instrument', None)
        worksheet = request.form.get('worksheet', 0)

        parser = Winlab32(infile, worksheet=worksheet)

        status = ['sample_received', 'attachment_due', 'to_be_verified']
        if artoapply == 'received':
            status = ['sample_received']
        elif artoapply == 'received_tobeverified':
            status = ['sample_received', 'attachment_due', 'to_be_verified']

        over = [False, False]
        if override == 'nooverride':
            over = [False, False]
        elif override == 'override':
            over = [True, False]
        elif override == 'overrideempty':
            over = [True, True]

        importer = AnalysisResultsImporter(
            parser=parser,
            context=context,
            allowed_ar_states=status,
            allowed_analysis_states=None,
            override=over,
            instrument_uid=instrument)

        try:
            importer.process()
        except Exception as e:
            results = {'errors': [repr(e), traceback.format_exc()],
                       'log': [], 'warns': []}
            return json.dumps(results)

        results = {'errors': importer.errors,
                   'log': importer.logs,
                   'warns': importer.warns}
        return json.dumps(results)
