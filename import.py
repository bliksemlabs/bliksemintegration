import importers.gvb
import importers.vtn
import importers.ebs
import importers.htm
import importers.htmbuzz
import importers.syntus
import importers.arr
import importers.tec
import logging
import sys

logger = logging.getLogger("importer")
fh = logging.FileHandler('error.log')
fh.setLevel(logging.ERROR)
ch = logging.StreamHandler(stream=sys.stdout)
ch.setLevel(logging.INFO)

logger.addHandler(fh)
logger.addHandler(ch)

def sync():
    importers.arr.sync()
    importers.ebs.sync()
    importers.gvb.sync()
    importers.htm.sync()
    importers.htmbuzz.sync()
    importers.syntus.sync()
    importers.vtn.sync()
    importers.tec.sync()
if __name__ == '__main__':
    sync()
