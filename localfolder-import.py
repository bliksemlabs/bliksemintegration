import os
import sys
import importers.arr
import importers.avv
import importers.cxx
import importers.ebs
import importers.gvb
import importers.htm
import importers.htmbuzz
import importers.ns
import importers.qbuzz
import importers.ret
import importers.retbus
import importers.syntus
import importers.vtn
import importers.tec

rootdir = sys.argv[1]
for root, subFolders, files in os.walk(rootdir):
    for file in sorted(files):
        agency = root[len(sys.argv[1])+1:].split('/')[0]

        if agency == 'arr':
            importers.arr.import_zip(root,file,None)

        elif agency == 'avv':
            meta = {'dataownercode' : 'AVV'}
            importers.ret.import_zip(root,file,meta)

        elif agency == 'cxx':
            if len(files) > 1:
                print 'WARNING YOU WILL IMPORT MULTIPLE CXX FILES, THIS WILL LEAD TO UNDEFINED BEHAVIOR!'
            meta = {'dataownercode' : 'CXX', 'validfrom' : '2014-01-01'}
            importers.arr.import_zip(root,file,meta)

        elif agency == 'ebs':
            importers.ebs.import_zip(root,file,None)

        elif agency == 'htm':
            importers.htm.import_zip(root,file,None)

        elif agency == 'htmbuzz':
            importers.htm.import_zip(root,file,None)

        elif agency == 'ns':
            meta = {'dataownercode' : 'NS'}
            importers.ns.import_zip(root,file,meta)

        elif agency == 'qbuzz':
            meta = {'dataownercode' : 'QBUZZ'}
            importers.qbuzz.import_zip(root,file,meta)

        elif agency == 'ret':
            meta = {'dataownercode' : 'RET'}
            importers.ret.import_zip(root,file,meta)

        elif agency == 'retbus':
            meta = {'dataownercode' : 'RET'}
            importers.retbus.import_zip(root,file,meta)

        elif agency == 'syntus':
            importers.syntus.import_zip(root,file,None)

        elif agency == 'tec':
            importers.tec.import_zip(root,file,None)

        elif agency == 'vtn':
            importers.vtn.import_zip(root,file,None)

