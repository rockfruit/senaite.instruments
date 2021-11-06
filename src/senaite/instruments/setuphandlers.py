from senaite.core.setuphandlers import _run_import_step

from senaite.instruments.config import PROFILE_ID


def install(context):
    """Install handler
    """
    if context.readDataFile("senaite.instruments.txt") is None:
        return
    portal = context.getSite()
    _run_import_step(portal, "browserlayer", PROFILE_ID)


def uninstall(context):
    if context.readDataFile('cannabis_uninstall.txt') is None:
        return
