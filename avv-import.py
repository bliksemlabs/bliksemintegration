import importers.avv
import logging
import sys

logger = logging.getLogger("importer")
fh = logging.FileHandler('error.log')
fh.setLevel(logging.ERROR)
ch = logging.StreamHandler(stream=sys.stdout)
ch.setLevel(logging.INFO)

logger.addHandler(fh)
logger.addHandler(ch)

path = '/home/thomas/rid/'
filename = sys.argv[1]
meta = {'dataownercode' : 'AVV'}

importers.avv.import_zip(path,filename,meta)
