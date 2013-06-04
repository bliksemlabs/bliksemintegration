import importers.cxx
import logging
import sys

logger = logging.getLogger("importer")
fh = logging.FileHandler('error.log')
fh.setLevel(logging.ERROR)
ch = logging.StreamHandler(stream=sys.stdout)
ch.setLevel(logging.INFO)

logger.addHandler(fh)
logger.addHandler(ch)

path = '/mnt/kv1/kv1feeds/connexxion/'
filename = '9292OV 2013-06-02a.zip'
meta = {'dataownercode' : 'CXX'}

importers.cxx.import_zip(path,filename,meta)
