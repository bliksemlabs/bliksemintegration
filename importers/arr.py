from kv1_810 import *
from inserter import insert,version_imported,reject
from bs4 import BeautifulSoup
import urllib2
from datetime import datetime,timedelta
import logging
from settings.const import *

logger = logging.getLogger("importer")

def getDataSource():
    return { '1' : {
                          'operator_id' : 'ARR',
                          'name'        : 'Arriva Bus/Veer/Trein KV1',
                          'description' : 'Arriva KV1 leveringen voor bus,trein,waterbus',
                          'email'       : None,
                          'url'         : None}}

def getOperator():
    return { 'ARR' :          {'privatecode' : 'ARR',
                               'operator_id' : 'ARR',
                               'name'        : 'Arriva',
                               'phone'       : '0900-2022022',
                               'url'         : 'http://www.arriva.nl',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'},
              'WATERBUS' :     {'privatecode' : 'WATERBUS',
                               'operator_id' : 'WATERBUS',
                               'name'        : 'Waterbus',
                               'phone'       : '0800-0232545',
                               'url'         : 'http://www.waterbus.nl',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'},
              'AQUALINER' :   {'privatecode' : 'AQUALINER',
                               'operator_id' : 'AQUALINER',
                               'name'        : 'Aqualiner',
                               'phone'       : '0800-0232545',
                               'url'         : 'http://www.aqualiner.nl',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'}
           }

def setLineColors():
    conn = psycopg2.connect(database_connect)
    cur = conn.cursor()
    cur.execute("""
UPDATE line set color_shield = '004990', color_text= 'ffffff' WHERE operator_id = 'ARR:15020';
UPDATE line set color_shield = 'ff0119', color_text= 'ffffff' WHERE operator_id = 'ARR:18017';
UPDATE line set color_shield = 'a474fe', color_text= 'ffffff' WHERE operator_id = 'ARR:18018';
UPDATE line set color_shield = '0fa30f', color_text= 'ffffff' WHERE operator_id = 'ARR:18019';
UPDATE line set color_shield = '659ad2', color_text= '000000' WHERE operator_id = 'ARR:15021';
UPDATE line set color_shield = 'fcaf17', color_text= '000000' WHERE operator_id = 'ARR:15022';
UPDATE line set color_shield = '5dbc56', color_text= '000000' WHERE operator_id = 'ARR:15023';
UPDATE line set color_shield = 'f36f2b', color_text= '000000' WHERE operator_id = 'ARR:15024';
""")
    cur.close()
    conn.commit()
    conn.close()

def getMergeStrategies(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT 'UNITCODE' as type,dataownercode||':'||organizationalunitcode as unitcode,min(validdate) as fromdate,max(validdate) as todate FROM operday GROUP BY dataownercode,organizationalunitcode
""")
    rows = cur.fetchall()
    cur.close()
    return rows

def containsTrain(conn):
   cur = conn.cursor()
   cur.execute("""
SELECT 1 FROM line where transporttype = 'TRAIN' or lineplanningnumber like '11___' or lineplanningnumber like '13___' or
lineplanningnumber like '12___' or lineplanningnumber like '14___'
""")
   rows = cur.fetchall()
   cur.close()
   return (len(rows) > 0)

def import_zip(path,filename,version):
    meta,conn = load(path,filename)
    if not import_arriva_trains and containsTrain(conn):
        data = {}
        data['DATASOURCE'] = getDataSource()
        data['VERSION'] = {}
        data['VERSION']['1'] = {'privatecode'   : 'ARR:'+filename,
                              'datasourceref' : '1',
                              'operator_id'   : 'ARR:'+filename,
                              'startdate'     : meta['startdate'],
                              'enddate'       : meta['enddate'],
                              'error'         : 'CONTAINS_TRAIN',
                              'description'   : filename}
        logger.info('Reject '+filename+'\n'+str(data['VERSION']['1']))
        reject(data)
        conn.commit()
        conn.close()
        return
    if datetime.strptime(meta['enddate'].replace('-',''),'%Y%m%d') < (datetime.now() - timedelta(days=1)):
        data = {}
        data['DATASOURCE'] = getDataSource()
        data['VERSION'] = {}
        data['VERSION']['1'] = {'privatecode'   : 'ARR:'+filename,
                              'datasourceref' : '1',
                              'operator_id'   : 'ARR:'+filename,
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
        data['VERSION']['1'] = {'privatecode'   : 'ARR:'+filename,
                             'datasourceref' : '1',
                              'operator_id'   : 'ARR:'+filename,
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
        routeRefForPattern,data['ROUTE'] = clusterPatternsIntoRoute(conn,getPool805)
        data['JOURNEYPATTERN'] = getJourneyPatterns(routeRefForPattern,conn,data['ROUTE'])
        data['JOURNEY'] = getJourneys(timedemandGroupRefForJourney,conn)
        data['NOTICEASSIGNMENT'] = {}
        data['NOTICE'] = {}
        data['NOTICEGROUP'] = {}
        conn.close()
        insert(data)
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

url = 'http://data.ndovloket.nl/arr/'

def sync():
    f = urllib2.urlopen(url+'?order=d')
    soup = BeautifulSoup(f.read())
    files = []
    for link in soup.find_all('a'):
        link = link.get('href')
        filename = urllib2.unquote(link)
        if '.zip' in link.lower():
            if not version_imported('ARR:'+filename):
                files.append((link,filename))
    for link,filename in sorted(files):
        try:
            logger.info('Importing :'+filename)
            download(url+link,filename)
        except Exception as e:
            logger.error(filename,exc_info=True)
            pass
