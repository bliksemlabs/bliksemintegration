from kv1_805 import *
from inserter import insert,version_imported
from bs4 import BeautifulSoup
import urllib2
from plugins.vtn_pool_fix import fix_pool
from datetime import datetime,timedelta
import logging

logger = logging.getLogger("importer")

def getDataSource():
    return { '1' : {
                          'operator_id' : 'VTN',
                          'name'        : 'Veolia KV1',
                          'description' : 'Veolia KV1 levering voor bus en Fast Ferry',
                          'email'       : None,
                          'url'         : None}}

def getOperator():
    return { 'VTN' :          {'privatecode' : 'VTN',
                               'operator_id' : 'VTN',
                               'name'        : 'Veolia',
                               'phone'       : '088-0761111',
                               'url'         : 'http://www.veolia.nl',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'}}

def getMergeStrategies(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT 'UNITCODE' as type,dataownercode||':'||organizationalunitcode as unitcode,min(validdate) as fromdate FROM operday GROUP BY dataownercode,organizationalunitcode
""")
    rows = cur.fetchall()
    cur.close()
    return rows

def import_zip(path,filename,version):
    meta,conn = load(path,filename)
    try:
        fix_pool(conn)
        data = {}
        data['OPERATOR'] = getOperator()
        data['MERGESTRATEGY'] = getMergeStrategies(conn)
        data['DATASOURCE'] = getDataSource()
        data['VERSION'] = {}
        data['VERSION']['1'] = {'privatecode'   : 'VTN:'+filename,
                             'datasourceref' : '1',
                              'operator_id'   : 'VTN:'+filename,
                              'startdate'     : meta['startdate'],
                              'enddate'       : meta['enddate'],
                              'description'   : filename}
        data['DESTINATIONDISPLAY'] = getDestinationDisplays(conn)
        data['LINE'] = getLines(conn)
        data['STOPPOINT'] = getStopPoints(conn)
        data['STOPAREA'] = getStopAreas(conn)
        data['AVAILABILITYCONDITION'] = getAvailabilityConditionsUsingOperday(conn)
        data['PRODUCTCATEGORY'] = getBISONproductcategories()
        timedemandGroupRefForJourney,data['TIMEDEMANDGROUP'] = calculateTimeDemandGroups(conn)
        routeRefForPattern,data['ROUTE'] = clusterPatternsIntoRoute(conn,getPool805)
        data['JOURNEYPATTERN'] = getJourneyPatterns(routeRefForPattern,conn,data['ROUTE'])
        data['JOURNEY'] = getJourneys(timedemandGroupRefForJourney,conn)
        data['NOTICEASSIGNMENT'] = {}
        data['NOTICE'] = {}
        data['NOTICEGROUP'] = {}
        insert(data)
        conn.close()
    except:
        raise

def download(url,filename):
    u = urllib2.urlopen(url)
    f = open('/tmp/'+filename, 'wb')

    meta = u.info()
    file_size = int(meta.getheaders("Content-Length")[0])
    print "Downloading: %s Bytes: %s" % (filename, file_size)

    file_size_dl = 0
    block_sz = 8192
    while True:
        buffer = u.read(block_sz)
        if not buffer:
            break
        file_size_dl += len(buffer)
        f.write(buffer)
        status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
        status = status + chr(8)*(len(status)+1)
        print status,
    print
    f.close()
    import_zip('/tmp',filename,None)

url = 'http://data.ndovloket.nl/vtn/'

def sync():
    f = urllib2.urlopen(url+'?order=d')
    soup = BeautifulSoup(f.read())
    for link in soup.find_all('a'):
        link = link.get('href')
        filename = urllib2.unquote(link)
        if '.zip' in link.lower():
            if not version_imported('VTN:'+filename):
              try:
                logger.info('Importing :'+filename)
                download(url+link,filename)
              except Exception as e:
                logger.error(filename,exc_info=True)
                pass
