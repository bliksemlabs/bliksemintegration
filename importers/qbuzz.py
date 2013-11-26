from kv1_811 import *
from inserter import insert,version_imported
from bs4 import BeautifulSoup
import urllib2
from datetime import datetime,timedelta
import logging 
from settings.const import *

logger = logging.getLogger("importer")

def getDataSource():
    return { '1' : {
                          'operator_id' : 'QBUZZ',
                          'name'        : 'Qbuzz KV1',
                          'description' : 'Qbuzz KV1 leveringen',
                          'email'       : None,
                          'url'         : None}}

def getOperator():
    return { 'QBUZZ' :          {'privatecode' : 'QBUZZ',
                               'operator_id' : 'QBUZZ',
                               'name'        : 'Qbuzz',
                               'phone'       : '0900-7289965',
                               'url'         : 'http://www.qbuzz.nl',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'},
             'UOV' :          {'privatecode' : 'UOV',
                               'operator_id' : 'UOV',
                               'name'        : 'U-OV',
                               'phone'       : '0900-7289965',
                               'url'         : 'http://www.u-ov.info',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'}}

def getMergeStrategies(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT 'DATASOURCE' as type,'1' as datasourceref,min(validdate) as fromdate FROM operday GROUP BY dataownercode
""")
    rows = cur.fetchall()
    cur.close()
    return rows

def setLineColors():
    conn = psycopg2.connect(database_connect)
    cur = conn.cursor()
    cur.execute("""
update line set color_shield = '2B9F54' where operator_id = 'QBUZZ:g001';
update line set color_text = '000000' where operator_id = 'QBUZZ:g001';

update line set color_shield = '2B9F54' where operator_id = 'QBUZZ:g002';
update line set color_text = '000000' where operator_id = 'QBUZZ:g002';

update line set color_shield = '68a0e4' where operator_id = 'QBUZZ:g003';
update line set color_text = '000000' where operator_id = 'QBUZZ:g003';

update line set color_shield = '187b99' where operator_id = 'QBUZZ:g004';
update line set color_text = 'ffffff' where operator_id = 'QBUZZ:g004';

update line set color_shield = 'EC5A5D' where operator_id = 'QBUZZ:g005';
update line set color_text = '000000' where operator_id = 'QBUZZ:g005';

update line set color_shield = 'ec5a5d' where operator_id = 'QBUZZ:g006';
update line set color_text = '000000' where operator_id = 'QBUZZ:g006';

update line set color_shield = 'f7df81' where operator_id = 'QBUZZ:g008';
update line set color_text = '474747' where operator_id = 'QBUZZ:g008';

update line set color_shield = 'f2a5f3' where operator_id = 'QBUZZ:g011';
update line set color_text = '000000' where operator_id = 'QBUZZ:g011';

update line set color_shield = 'dc82e6' where operator_id = 'QBUZZ:g015';
update line set color_text = '000000' where operator_id = 'QBUZZ:g015';

update line set color_shield = 'dc82e6' where operator_id = 'QBUZZ:g015';
update line set color_text = '000000' where operator_id = 'QBUZZ:g015';""")
    cur.close()
    conn.commit()
    conn.close()

def import_zip(path,filename,version):
    meta,conn = load(path,filename)
    try:
        data = {}
        data['OPERATOR'] = getOperator()
        data['MERGESTRATEGY'] = getMergeStrategies(conn)
        data['DATASOURCE'] = getDataSource()
        data['VERSION'] = {}
        data['VERSION']['1'] = {'privatecode'   : 'QBUZZ:'+filename,
                             'datasourceref' : '1',
                              'operator_id'   : 'QBUZZ:'+filename,
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


url = 'http://kv1.openov.nl/QBUZZ/'

def sync():
    f = urllib2.urlopen(url+'?order=d')
    soup = BeautifulSoup(f.read())
    for link in soup.find_all('a'):
        link = link.get('href')
        filename = urllib2.unquote(link)
        if '.zip' in link.lower():
            if not version_imported('QBUZZ:'+filename):
              try:
                logger.info('Importing :'+filename)
                download(url+link,filename)
              except Exception as e:
                logger.error(filename,exc_info=True)
                pass
