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
                               'phone'       : '0900-5252241',
                               'url'         : 'http://www.u-ov.info',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'}}

def getMergeStrategies(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT 'UNITCODE' as type,dataownercode||':'||organizationalunitcode as unitcode,min(validdate) as fromdate,max(validdate) as todate FROM operday 
GROUP BY dataownercode,organizationalunitcode
""")
    rows = cur.fetchall()
    cur.close()
    return rows

def setLineColors():
    conn = psycopg2.connect(database_connect)
    cur = conn.cursor()
    cur.execute("""
--QLINK

update line set color_shield = '00be5c' where operator_id = 'QBUZZ:g502';
update line set color_text = '000000' where operator_id = 'QBUZZ:g502';
update line set color_shield = '185099' where operator_id = 'QBUZZ:g503';
update line set color_text = 'ffffff' where operator_id = 'QBUZZ:g503';
update line set color_shield = '6ed1f6' where operator_id = 'QBUZZ:g554';
update line set color_text = '000000' where operator_id = 'QBUZZ:g554';
update line set color_shield = '7e1c90' where operator_id = 'QBUZZ:g505';
update line set color_text = 'ffffff' where operator_id = 'QBUZZ:g505';
update line set color_shield = 'd81118' where operator_id = 'QBUZZ:g506';
update line set color_text = 'ffffff' where operator_id = 'QBUZZ:g506';
update line set color_shield = 'fdd205' where operator_id = 'QBUZZ:g507';
update line set color_text = '000000' where operator_id = 'QBUZZ:g507';
update line set color_shield = 'dd9345' where operator_id = 'QBUZZ:g508';
update line set color_text = '000000' where operator_id = 'QBUZZ:g508';
update line set color_shield = 'f468bb' where operator_id = 'QBUZZ:g509';
update line set color_text = '000000' where operator_id = 'QBUZZ:g509';
update line set color_shield = 'ec008c' where operator_id = 'QBUZZ:g501';
update line set color_text = '000000' where operator_id = 'QBUZZ:g501';
update line set color_shield = 'ed028d' where operator_id = 'QBUZZ:g512';
update line set color_text = '000000' where operator_id = 'QBUZZ:g512';
update line set color_shield = 'f35e18' where operator_id = 'QBUZZ:g565';
update line set color_text = '000000' where operator_id = 'QBUZZ:g565';
update line set color_shield = 'f35e18' where operator_id = 'QBUZZ:g515';
update line set color_text = '000000' where operator_id = 'QBUZZ:g515';
update line set color_shield = 'd81118' where operator_id = 'QBUZZ:g516';
update line set color_text = 'ffffff' where operator_id = 'QBUZZ:g516';
update line set color_shield = 'f68512' where operator_id = 'QBUZZ:g517';
update line set color_text = '000000' where operator_id = 'QBUZZ:g517';
""")
    cur.close()
    conn.commit()
    conn.close()

def import_zip(path,filename,version):
    meta,conn = load(path,filename,point_from_pool=True)
    try:
        if pool_generation_enabled:
            cur = conn.cursor()
            cur.execute("""
UPDATE pool_utram set linkvalidfrom = (SELECT DISTINCT validfrom FROM LINK where transporttype = 'TRAM');
update point set locationx_ew = '135335', locationy_ns = '451223' where locationx_ew = '135639' and locationy_ns = '451663';
update point set locationx_ew = '134669', locationy_ns = '450853' where locationx_ew = '134591' and locationy_ns = '450911';
update point set locationx_ew = '133029', locationy_ns = '447900' where locationx_ew = '132473' and locationy_ns = '448026';
update point set locationx_ew = '132907', locationy_ns = '447965' where locationx_ew = '132672' and locationy_ns = '448044';
update point set locationx_ew = '135335', locationy_ns = '451314' where locationx_ew = '135533' and locationy_ns = '451628';
update point set locationx_ew = '134356', locationy_ns = '448631' where locationx_ew = '134318' and locationy_ns = '448697';
update point set locationx_ew = '131710', locationy_ns = '448728' where locationx_ew = '131731' and locationy_ns = '448705';
insert into POINT (SELECT * from point_utram);
insert into POOL (SELECT * FROM pool_utram WHERE userstopcodebegin||':'||userstopcodeend in (SELECT userstopcodebegin||':'||userstopcodeend));""")
        data = {}
        data['OPERATOR'] = getOperator()
        data['MERGESTRATEGY'] = []#getMergeStrategies(conn)
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
