import csv
import json
import re
import traceback
from cStringIO import StringIO
from DateTime import DateTime
from bika.lims import api
from bika.lims.catalog import CATALOG_ANALYSIS_REQUEST_LISTING
from senaite.core.exportimport.instruments import IInstrumentAutoImportInterface
from senaite.core.exportimport.instruments import IInstrumentExportInterface
from senaite.core.exportimport.instruments import IInstrumentImportInterface
from senaite.core.exportimport.instruments.utils import \
    get_instrument_import_ar_allowed_states
from senaite.core.exportimport.instruments.utils import \
    get_instrument_import_override
from senaite.core.exportimport.instruments.resultsimport import AnalysisResultsImporter
from senaite.core.exportimport.instruments.resultsimport import \
    InstrumentCSVResultsFileParser
from plone.i18n.normalizer.interfaces import IIDNormalizer
from zope.component import getUtility
from zope.interface import implements


class xcaliburexport(object):
    implements(IInstrumentExportInterface)
    title = "XCalibur Exporter"

    def __init__(self, context):
        self.context = context
        self.request = None

    def Export(self, context, request):
        tray = 1
        now = DateTime().strftime('%Y%m%d-%H%M')
        uc = api.get_tool('uid_catalog')
        instrument = context.getInstrument()
        norm = getUtility(IIDNormalizer).normalize
        filename = '{}-{}.csv'.format(
            context.getId(), norm(instrument.getDataInterface()))
        listname = '{}_{}_{}'.format(
            context.getId(), norm(instrument.Title()), now)
        options = {
            'dilute_factor': 1,
            'method': 'F SO2 & T SO2'
        }
        for k, v in instrument.getDataInterfaceOptions():
            options[k] = v

        # for looking up "cup" number (= slot) of ARs
        parent_to_slot = {}
        layout = context.getLayout()
        for x in range(len(layout)):
            a_uid = layout[x]['analysis_uid']
            p_uid = uc(UID=a_uid)[0].getObject().aq_parent.UID()
            layout[x]['parent_uid'] = p_uid
            if p_uid not in parent_to_slot.keys():
                parent_to_slot[p_uid] = int(layout[x]['position'])

        # write rows, one per PARENT
        header = [listname, options['method']]
        rows = []
        rows.append(header)
        tmprows = []
        ARs_exported = []
        for x in range(len(layout)):
            # create batch header row
            c_uid = layout[x]['container_uid']
            p_uid = layout[x]['parent_uid']
            if p_uid in ARs_exported:
                continue
            cup = parent_to_slot[p_uid]
            tmprows.append([tray,
                            cup,
                            p_uid,
                            c_uid,
                            options['dilute_factor'],
                            ""])
            ARs_exported.append(p_uid)
        tmprows.sort(lambda a, b: cmp(a[1], b[1]))
        rows += tmprows

        ramdisk = StringIO()
        writer = csv.writer(ramdisk, delimiter=';')
        assert(writer)
        writer.writerows(rows)
        result = ramdisk.getvalue()
        ramdisk.close()

        # stream file to browser
        setheader = request.RESPONSE.setHeader
        setheader('Content-Length', len(result))
        setheader('Content-Type', 'text/comma-separated-values')
        setheader('Content-Disposition', 'inline; filename=%s' % filename)
        request.RESPONSE.write(result)


class xcaliburimport(object):
    implements(IInstrumentImportInterface, IInstrumentAutoImportInterface)
    title = "XCalibur"

    def __init__(self, context):
        self.context = context
        self.request = None

    def Import(self, context, request):
        """ Read Dimensional-CSV analysis results
        """
        form = request.form
        # TODO form['file'] sometimes returns a list
        infile = form['instrument_results_file'][0] if \
            isinstance(form['instrument_results_file'], list) else \
            form['instrument_results_file']
        artoapply = form['artoapply']
        override = form['results_override']

        instrument = form.get('instrument', None)
        errors = []
        logs = []

        # Load the most suitable parser according to file extension/options/etc...
        parser = None
        if not hasattr(infile, 'filename'):
            errors.append(_("No file selected"))
        parser = XCaliburCSVParser(infile)
        status = get_instrument_import_ar_allowed_states(artoapply)
        over = get_instrument_import_override(override)
        importer = XCaliburImporter(
            parser=parser,
            context=context,
            allowed_ar_states=status,
            allowed_analysis_states=None,
            override=over,
            instrument_uid=instrument,
            form=form)
        tbex = ''
        try:
            importer.process()
        except Exception as e:
            tbex = traceback.format_exc()
        errors = importer.errors
        logs = importer.logs
        warns = importer.warns
        if tbex:
            errors.append(tbex)

        results = {'errors': errors, 'log': logs, 'warns': warns}

        return json.dumps(results)


def is_keyword(kw):
    bsc = api.get_tool('bika_setup_catalog')
    return len(bsc(getKeyword=kw))


def find_analyses(ar_or_sample):
    """ This function is used to find keywords that are not on the analysis
        but keywords that are on the interim fields.

        This function and is is_keyword function should probably be in
        resultsimport.py or somewhere central where it can be used by other
        instrument interfaces.
    """
    bc = api.get_tool(CATALOG_ANALYSIS_REQUEST_LISTING)
    ar = bc(portal_type='AnalysisRequest', id=ar_or_sample)
    if len(ar) == 0:
        ar = bc(portal_type='AnalysisRequest', getClientSampleID=ar_or_sample)
    if len(ar) == 1:
        obj = ar[0].getObject()
        analyses = obj.getAnalyses(full_objects=True)
        return analyses
    return []


def get_interims_keywords(analysis):
    interims = api.safe_getattr(analysis, 'getInterimFields')
    return map(lambda item: item['keyword'], interims)


def find_analysis_interims(ar_or_sample):
    """ This function is used to find keywords that are not on the analysis
        but keywords that are on the interim fields.

        This function and is is_keyword function should probably be in
        resultsimport.py or somewhere central where it can be used by other
        instrument interfaces.
    """
    interim_fields = list()
    for analysis in find_analyses(ar_or_sample):
        keywords = get_interims_keywords(analysis)
        interim_fields.extend(keywords)
    return list(set(interim_fields))


def find_kw(ar_or_sample, kw):
    """ This function is used to find keywords that are not on the analysis
        but keywords that are on the interim fields.

        This function and is is_keyword function should probably be in
        resultsimport.py or somewhere central where it can be used by other
        instrument interfaces.
    """
    for analysis in find_analyses(ar_or_sample):
        if kw in get_interims_keywords(analysis):
            return analysis.getKeyword()
    return None


class XCaliburCSVParser(InstrumentCSVResultsFileParser):

    QUANTITATIONRESULTS_NUMERICHEADERS = ('Title8', 'Title9', 'Title31',
                                          'Title32', 'Title41', 'Title42',
                                          'Title43',)

    def __init__(self, csv):
        InstrumentCSVResultsFileParser.__init__(self, csv)
        self._end_header = False
        self._keywords = []
        self._quantitationresultsheader = []
        self._numline = 0

    def _parseline(self, line):
        if self._end_header:
            return self.parse_resultsline(line)
        return self.parse_headerline(line)

    def parse_headerline(self, line):
        """ Parses header lines

            Keywords example:
            Keyword1, Keyword2, Keyword3, ..., end
        """
        if self._end_header is True:
            # Header already processed
            return 0

        splitted = [token.strip() for token in line.split(',')]
        if splitted[-1] == 'end':
            self._keywords = splitted[1:-1]  # exclude the word end
            self._end_header = True
        return 0

    def parse_resultsline(self, line):
        """ Parses result lines
        """
        splitted = [token.strip() for token in line.split(',')]

        if splitted[0] == 'end':
            return 0

        blank_line = [i for i in splitted if i != '']
        if len(blank_line) == 0:
            return 0

        quantitation = {}
        list_of_interim_results = []
        # list_of_interim_results is a list that will have interim fields on
        # the current line so that we don't have to call self._addRawResult
        # for the same interim fields, ultimately we want a dict that looks
        # like quantitation = {'AR': 'AP-0001-R01', 'interim1': 83.12, 'interim2': 22.3}
        # self._addRawResult(quantitation['AR'],
        #                    values={kw: quantitation},
        #                    override=False)
        # We use will one of the interims to find the analysis in this case new_kw which becomes kw
        # kw is the analysis keyword which sometimes we have to find using the interim field
        # because we have the result of the interim field and not of the analysis

        found = False  # This is just a flag used to check values in list_of_interim_results
        clean_splitted = splitted[1:-1]  # First value on the line is AR
        for i in range(len(clean_splitted)):
            token = clean_splitted[i]
            if i < len(self._keywords):
                quantitation['AR'] = splitted[0]
                # quantitation['AN'] = self._keywords[i]
                quantitation['DefaultResult'] = 'resultValue'
                quantitation['resultValue'] = token
            elif token:
                self.err("Orphan value in column ${index} (${token})",
                         mapping={"index": str(i + 1),
                                  "token": token},
                         numline=self._numline, line=line)

            result = quantitation[quantitation['DefaultResult']]
            column_name = quantitation['DefaultResult']
            result = self.get_result(column_name, result, line)
            quantitation[quantitation['DefaultResult']] = result

            kw = re.sub(r"\W", "", self._keywords[i])
            if not is_keyword(kw):
                new_kw = find_kw(quantitation['AR'], kw)
                if new_kw:
                    quantitation[kw] = quantitation['resultValue']
                    del quantitation['resultValue']
                    for interim_res in list_of_interim_results:
                        if kw in interim_res:
                            # Interim field already in quantitation dict
                            found = True
                            break
                    if found:
                        continue
                    interims = find_analysis_interims(quantitation['AR'])
                    # pairing headers(keywords) and their values(results) per line
                    keyword_value_dict = dict(zip(self._keywords, clean_splitted))
                    for interim in interims:
                        if interim in keyword_value_dict:
                            quantitation[interim] = keyword_value_dict[interim]
                            list_of_interim_results.append(quantitation)
                    kw = new_kw
                    kw = re.sub(r"\W", "", kw)

            self._addRawResult(quantitation['AR'],
                               values={kw: quantitation},
                               override=False)
            quantitation = {}
            found = False

    def get_result(self, column_name, result, line):
        result = str(result)
        if result.startswith('--') or result == '' or result == 'ND':
            return 0.0

        if api.is_floatable(result):
            result = api.to_float(result)
            return result > 0.0 and result or 0.0
        self.err("No valid number ${result} in column (${column_name})",
                 mapping={"result": result,
                          "column_name": column_name},
                 numline=self._numline, line=line)
        return


class XCaliburImporter(AnalysisResultsImporter):

    def __init__(self, parser, context, override,
                 allowed_ar_states=None, allowed_analysis_states=None,
                 instrument_uid='', form=None):
        AnalysisResultsImporter.__init__(self, parser, context,
                                         override, allowed_ar_states,
                                         allowed_analysis_states,
                                         instrument_uid)
