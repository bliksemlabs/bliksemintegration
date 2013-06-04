from kv1_811 import *
from inserter import insert,version_imported,reject
from bs4 import BeautifulSoup
import urllib2
from datetime import datetime,timedelta
import logging

logger = logging.getLogger("importer")

def getDataSource():
    return { '1' : {
                          'operator_id' : 'HTMBUZZ',
                          'name'        : 'HTMbuzz KV1',
                          'description' : 'HTMbuzz KV1 leveringen',
                          'email'       : None,
                          'url'         : None}}

def getOperator():
    return { 'HTM' :          {'privatecode' : 'HTM',
                               'operator_id' : 'HTM',
                               'name'        : 'HTM',
                               'phone'       : '0900-4864636',
                               'url'         : 'http://www.htm.net',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'},
              'HTMBUZZ' :     {'privatecode' : 'HTMBUZZ',
                               'operator_id' : 'HTMBUZZ',
                               'name'        : 'HTMbuzz',
                               'phone'       : '0900-4864636',
                               'url'         : 'http://www.htmbuzz.nl',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'}
           }

def getMergeStrategies(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT 'DATASOURCE' as type,'1' as datasourceref,min(validfrom) as fromdate,max(validthru) as todate FROM schedvers GROUP BY dataownercode
""")
    rows = cur.fetchall()
    cur.close()
    return rows

def import_zip(path,filename,version):
    meta,conn = load(path,filename)
    if datetime.strptime(meta['enddate'].replace('-',''),'%Y%m%d') < (datetime.now() - timedelta(days=1)):
        data = {}
        data['DATASOURCE'] = getDataSource()
        data['VERSION'] = {}
        data['VERSION']['1'] = {'privatecode'   : 'HTMBUZZ:'+filename,
                              'datasourceref' : '1',
                              'operator_id'   : 'HTMBUZZ:'+filename,
                              'startdate'     : meta['startdate'],
                              'enddate'       : meta['enddate'],
                              'error'         : 'ALREADY_EXPIRED',
                              'description'   : filename}
        logger.info('Reject '+filename+'\n'+str(data['VERSION']['1']))
        reject(data)
        conn.commit()
        conn.close()
        return
    try:
        data = {}
        data['OPERATOR'] = getOperator()
        data['MERGESTRATEGY'] = getMergeStrategies(conn)
        data['DATASOURCE'] = getDataSource()
        data['VERSION'] = {}
        data['VERSION']['1'] = {'privatecode'   : 'HTMBUZZ:'+filename,
                             'datasourceref' : '1',
                              'operator_id'   : 'HTMBUZZ:'+filename,
                              'startdate'     : meta['startdate'],
                              'enddate'       : meta['enddate'],
                              'description'   : filename}
        data['DESTINATIONDISPLAY'] = getDestinationDisplays(conn)
        data['LINE'] = getLines(conn)
        data['STOPPOINT'] = getStopPoints(conn)
        data['STOPAREA'] = getStopAreas(conn)
        data['AVAILABILITYCONDITION'] = getAvailabilityConditionsUsingOperday(conn)
        data['PRODUCTCATEGORY'] = getBISONproductcategories()
        data['ADMINISTRATIVEZONE'] = getAdministrativeZones(conn)
        timedemandGroupRefForJourney,data['TIMEDEMANDGROUP'] = calculateTimeDemandGroups(conn)
        routeRefForPattern,data['ROUTE'] = clusterPatternsIntoRoute(conn,getPool811)
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

url = 'http://data.ndovloket.nl/htmbuzz/'

def sync():
    f = urllib2.urlopen(url+'?order=d')
    soup = BeautifulSoup(f.read())
    for link in soup.find_all('a'):
        link = link.get('href')
        filename = urllib2.unquote(link)
        if '.zip' in link.lower():
            if not version_imported('HTMBUZZ:'+filename):
              try:
                logger.info('Importing :'+filename)
                download(url+link,filename)
              except Exception as e:
                logger.exception(filename,exc_info=True)
                pass
