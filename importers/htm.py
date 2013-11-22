from kv1_811 import *
from inserter import insert,version_imported,reject
from bs4 import BeautifulSoup
import urllib2
from settings.const import *
from datetime import datetime,timedelta
import logging

logger = logging.getLogger("importer")
getPool = getFakePool811

def getDataSource():
    return { '1' : {
                          'operator_id' : 'HTM',
                          'name'        : 'HTM KV1',
                          'description' : 'HTM Rail KV1 leveringen',
                          'email'       : None,
                          'url'         : None}}

def setLineColors():
    conn = psycopg2.connect(database_connect)
    cur = conn.cursor()
    cur.execute("""
update line set name= replace(name,publiccode||' ','') where operator_id like 'HTM:%';
UPDATE line SET color_shield = 'e72419', color_text = 'ffffff' WHERE publiccode = '1' and transportmode = 'TRAM' and operator_id like 'HTM:%';
UPDATE line SET color_shield = 'ffc52d', color_text = '000000' WHERE publiccode = '2' and transportmode = 'TRAM' and operator_id like 'HTM:%';
UPDATE line SET color_shield = 'be1fa1', color_text = 'ffffff' WHERE publiccode = '3' and transportmode = 'TRAM' and operator_id like 'HTM:%';
UPDATE line SET color_shield = 'ef7100', color_text = '000000' WHERE publiccode = '4' and transportmode = 'TRAM' and operator_id like 'HTM:%';
UPDATE line SET color_shield = '59e759', color_text = '000000' WHERE publiccode = '5' and transportmode = 'TRAM' and operator_id like 'HTM:%';
UPDATE line SET color_shield = '009fe3', color_text = '000000' WHERE publiccode = '6' and transportmode = 'TRAM' and operator_id like 'HTM:%';
UPDATE line SET color_shield = '6040a0', color_text = 'ffffff' WHERE publiccode = '8' and transportmode = 'TRAM' and operator_id like 'HTM:%';
UPDATE line SET color_shield = '8dbb00', color_text = '000000' WHERE publiccode = '9' and transportmode = 'TRAM' and operator_id like 'HTM:%';
UPDATE line SET color_shield = '465c6b', color_text = 'ffffff' WHERE publiccode = '10' and transportmode = 'TRAM' and operator_id like 'HTM:%';
UPDATE line SET color_shield = 'b27f66', color_text = '000000' WHERE publiccode = '11' and transportmode = 'TRAM' and operator_id like 'HTM:%';
UPDATE line SET color_shield = 'e39fc9', color_text = '000000' WHERE publiccode = '12' and transportmode = 'TRAM' and operator_id like 'HTM:%';
UPDATE line SET color_shield = '8c7ad2', color_text = '000000' WHERE publiccode = '15' and transportmode = 'TRAM' and operator_id like 'HTM:%';
UPDATE line SET color_shield = '773d29', color_text = 'ffffff' WHERE publiccode = '16' and transportmode = 'TRAM' and operator_id like 'HTM:%';
UPDATE line SET color_shield = '003186', color_text = 'ffffff' WHERE publiccode = '17' and transportmode = 'TRAM' and operator_id like 'HTM:%';
UPDATE line SET color_shield = '00a788', color_text = '000000' WHERE publiccode = '19' and transportmode = 'TRAM' and operator_id like 'HTM:%';

UPDATE line SET color_shield = '009fe3', color_text = '000000' WHERE publiccode = '18' and transportmode = 'BUS' and operator_id like 'HTM:%';
UPDATE line SET color_shield = 'ef7100', color_text = '000000' WHERE publiccode = '21' and transportmode = 'BUS' and operator_id like 'HTM:%';
UPDATE line SET color_shield = 'e39fc9', color_text = '000000' WHERE publiccode = '22' and transportmode = 'BUS' and operator_id like 'HTM:%';
UPDATE line SET color_shield = '003186', color_text = 'ffffff' WHERE publiccode = '23' and transportmode = 'BUS' and operator_id like 'HTM:%';
UPDATE line SET color_shield = 'be1fa1', color_text = 'ffffff' WHERE publiccode = '24' and transportmode = 'BUS' and operator_id like 'HTM:%';
UPDATE line SET color_shield = '773d29', color_text = 'ffffff' WHERE publiccode = '25' and transportmode = 'BUS' and operator_id like 'HTM:%';
UPDATE line SET color_shield = '8dbb00', color_text = '000000' WHERE publiccode = '26' and transportmode = 'BUS' and operator_id like 'HTM:%';
UPDATE line SET color_shield = 'e72419', color_text = 'ffffff' WHERE publiccode = '28' and transportmode = 'BUS' and operator_id like 'HTM:%';
UPDATE line SET color_shield = '000000', color_text = 'f7ff00' WHERE publiccode like 'N%' and transportmode = 'BUS' and operator_id like 'HTM:%';
""")
    cur.close()
    conn.commit()
    conn.close()
   

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
        data['VERSION']['1'] = {'privatecode'   : 'HTM:'+filename,
                              'datasourceref' : '1',
                              'operator_id'   : 'HTM:'+filename,
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
        data['VERSION']['1'] = {'privatecode'   : 'HTM:'+filename,
                             'datasourceref' : '1',
                              'operator_id'   : 'HTM:'+filename,
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
        routeRefForPattern,data['ROUTE'] = clusterPatternsIntoRoute(conn,getPool811)
        data['JOURNEYPATTERN'] = getJourneyPatterns(routeRefForPattern,conn,data['ROUTE'])
        data['JOURNEY'] = getJourneys(timedemandGroupRefForJourney,conn)
        data['NOTICEASSIGNMENT'] = {}
        data['NOTICE'] = {}
        data['NOTICEGROUP'] = {}
        insert(data)
        conn.close()
        setLineColors()
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

url = 'http://data.ndovloket.nl/htm/'

def sync():
    f = urllib2.urlopen(url+'?order=d')
    soup = BeautifulSoup(f.read())
    for link in soup.find_all('a'):
        link = link.get('href')
        filename = urllib2.unquote(link)
        if '.zip' in link.lower():
            if not version_imported('HTM:'+filename):
              try:
                logger.info('Importing :'+filename)
                download(url+link,filename)
              except Exception as e:
                logger.error(filename,exc_info=True)
                pass
