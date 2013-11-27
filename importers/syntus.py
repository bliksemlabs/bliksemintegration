from kv1_810 import *
from inserter import insert,version_imported
from bs4 import BeautifulSoup
import urllib2
from datetime import datetime,timedelta
import logging 

logger = logging.getLogger("importer")

def getDataSource():
    return { '1' : {
                          'operator_id' : 'SYNTUS',
                          'name'        : 'SYNTUS KV1',
                          'description' : 'SYNTUS KV1 leveringen',
                          'email'       : None,
                          'url'         : None}}

def getOperator():
    return { 'SYNTUS' :        {'privatecode' : 'SYNTUS',
                               'operator_id' : 'SYNTUS',
                               'name'        : 'Syntus',
                               'phone'       : '0314-350111',
                               'url'         : 'http://www.syntus.nl',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'},
             'TWENTS' :       {'privatecode' : 'TWENTS',
                               'operator_id' : 'TWENTS',
                               'name'        : 'Twents (Syntus)',
                               'phone'       : '088-0331360',
                               'url'         : 'http://www.syntustwente.nl',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'}}

def getMergeStrategies(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
DELETE FROM operday WHERE concat_ws(':',version,schedulecode,scheduletypecode,validdate) IN (
SELECT concat_ws(':',version,schedulecode,scheduletypecode,validdate) FROM operday JOIN schedvers USING (version,schedulecode,scheduletypecode) WHERE 
validdate < schedvers.validfrom);""")
    cur.execute("""
SELECT 'DATASOURCE' as type,'1' as datasourceref,min(validfrom) as fromdate FROM schedvers
""")
    rows = cur.fetchall()
    cur.close()
    return rows

def import_zip(path,filename,version):
    meta,conn = load(path,filename)
    try:
        data = {}
        data['OPERATOR'] = getOperator()
        data['MERGESTRATEGY'] = getMergeStrategies(conn)
        data['DATASOURCE'] = getDataSource()
        data['VERSION'] = {}
        data['VERSION']['1'] = {'privatecode'   : 'SYNTUS:'+filename,
                             'datasourceref' : '1',
                              'operator_id'   : 'SYNTUS:'+filename,
                              'startdate'     : meta['startdate'],
                              'enddate'       : meta['enddate'],
                              'description'   : filename}
        data['DESTINATIONDISPLAY'] = getDestinationDisplays(conn)
        data['LINE'] = getLineWithGeneratedNames(conn)
        data['STOPPOINT'] = getStopPoints(conn)
        data['STOPAREA'] = getStopAreas(conn)
        data['AVAILABILITYCONDITION'] = getAvailabilityConditionsUsingOperday(conn)
        data['PRODUCTCATEGORY'] = getBISONproductcategories()
        data['ADMINISTRATIVEZONE'] = getAdministrativeZones(conn)
        timedemandGroupRefForJourney,data['TIMEDEMANDGROUP'] = calculateTimeDemandGroups(conn)
        routeRefForPattern,data['ROUTE'] = clusterPatternsIntoRoute(conn,getPool805)
        data['JOURNEYPATTERN'] = getJourneyPatterns(routeRefForPattern,conn,data['ROUTE'])
        data['JOURNEY'] = getJourneys(timedemandGroupRefForJourney,conn)
        data['NOTICEASSIGNMENT'] = {}
        data['NOTICE'] = {}
        data['NOTICEGROUP'] = {}
        conn.close()
        insert(data)
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


url = 'http://kv1.openov.nl/syntus/'

def sync():
    f = urllib2.urlopen(url+'?order=d')
    soup = BeautifulSoup(f.read())
    for link in soup.find_all('a'):
        link = link.get('href')
        filename = urllib2.unquote(link)
        if '.zip' in link.lower():
            if not version_imported('SYNTUS:'+filename):
              try:
                logger.info('Importing :'+filename)
                download(url+link,filename)
              except Exception as e:
                logger.error(filename,exc_info=True)
                pass
