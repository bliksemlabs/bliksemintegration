from kv1_810 import *
from inserter import insert,version_imported,reject
from bs4 import BeautifulSoup
import urllib2
from datetime import datetime,timedelta
import logging

logger = logging.getLogger("importer")

def getDataSource():
    return { '1' : {
                          'operator_id' : 'CXX',
                          'name'        : 'Connexxion KV1',
                          'description' : 'Connexxion KV1 leveringen,rijtijdgroepen',
                          'email'       : None,
                          'url'         : None}}

def getOperator():
    return {
              'BRENG' :       {'privatecode' : 'BRENG',
                               'operator_id' : 'BRENG',
                               'name'        : 'Breng',
                               'phone'       : '026-2142140',
                               'url'         : 'http://www.breng.nl',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'},
              'CXX' :          {'privatecode' : 'CXX',
                               'operator_id' : 'CXX',
                               'name'        : 'Connexxion',
                               'phone'       : '0900-2666399',
                               'url'         : 'http://www.connexxion.nl',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'},
              'GVU' :         {'privatecode' : 'GVU',
                               'operator_id' : 'GVU',
                               'name'        : 'GVU',
                               'phone'       : '0900-8998959',
                               'url'         : 'http://www.gvu.nl',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'},
              'NIAG' :        {'privatecode' : 'NIAG',
                               'operator_id' : 'NIAG',
                               'name'        : 'NIAG',
                               'phone'       : '+4901803504030',
                               'url'         : 'http://www.niag-online.de/',
                               'timezone'    : 'Europe/Berlin',
                               'language'    : 'nl'},
              'HERMES' :      {'privatecode' : 'HERMES',
                               'operator_id' : 'HERMES',
                               'name'        : 'Hermes',
                               'phone'       : '0800-0222277',
                               'url'         : 'http://www.hermes.nl',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'}
           }

def import_zip(path,filename,version):
    validfrom = '2013-06-22'
    validthru = '2014-01-04'
    meta,conn = load(path,filename)
    cur = conn.cursor()
    cur.execute("""create index on pool(userstopcodebegin,userstopcodeend);""")
    cur.close()
    try:
        data = {}
        data['OPERATOR'] = getOperator()
        data['MERGESTRATEGY'] = [{'type' : 'DATASOURCE', 'fromdate' : validfrom, 'datasourceref' : '1'}] 
        data['DATASOURCE'] = getDataSource()
        data['VERSION'] = {}
        data['VERSION']['1'] = {'privatecode'   : 'CXX:'+filename,
                             'datasourceref' : '1',
                              'operator_id'   : 'CXX:'+filename,
                              'startdate'     : validfrom,
                              'enddate'       : validthru,
                              'description'   : filename}
        data['DESTINATIONDISPLAY'] = getDestinationDisplays(conn)
        data['LINE'] = getLines(conn)
        data['STOPPOINT'] = getStopPoints(conn)
        data['STOPAREA'] = getStopAreas(conn)
        data['AVAILABILITYCONDITION'] = getAvailabilityConditionsFromCalendars(conn,validfrom)
        data['JOURNEY'] = []
        for journey in getJourneysFromPujo(conn):
            if journey['availabilityconditionref'] not in data['AVAILABILITYCONDITION']:
                logging.warning('Servicecalendar %s missing for %s' % (journey['availabilityconditionref'],journey['operator_id']))
            else:
                data['JOURNEY'].append(journey)
        data['PRODUCTCATEGORY'] = getBISONproductcategories()
        data['ADMINISTRATIVEZONE'] = getAdministrativeZones(conn)
        data['TIMEDEMANDGROUP'] = getTimeDemandGroups(conn)
        routeRefForPattern,data['ROUTE'] = clusterPatternsIntoRoute(conn,getPool805)
        data['JOURNEYPATTERN'] = getJourneyPatterns(routeRefForPattern,conn,data['ROUTE'])
        data['NOTICEASSIGNMENT'] = {}
        data['NOTICE'] = {}
        data['NOTICEGROUP'] = {}
        conn.close()
        insert(data)
    except:
        raise

def sync():
    raise Exception('Sync not yet supported')
