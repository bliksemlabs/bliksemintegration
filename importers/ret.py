from kv1_811 import *
from inserter import insert,version_imported,reject
from bs4 import BeautifulSoup
import urllib2
from datetime import datetime,timedelta
import logging
from settings.const import *


logger = logging.getLogger("importer")
getPool = getFakePool811

def getDataSource():
    return { '1' : {
                          'operator_id' : 'RET',
                          'name'        : 'RET KV1',
                          'description' : 'RET KV1 leveringen',
                          'email'       : None,
                          'url'         : None}}

def getOperator():
    return { 'RET' :          {'privatecode' : 'RET',
                               'operator_id' : 'RET',
                               'name'        : 'RET',
                               'phone'       : '0900-5006010',
                               'url'         : 'http://www.ret.nl',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'}
           }

def setLineColors():
    conn = psycopg2.connect(database_connect)
    cur = conn.cursor()
    cur.execute("""
UPDATE line set color_shield = '00b43f', color_text= 'ffffff' WHERE operator_id = 'RET:M006';
UPDATE line set color_shield = 'ffdd00', color_text= '000000' WHERE operator_id = 'RET:M007';
UPDATE line set color_shield = 'e32119', color_text= '000000' WHERE operator_id = 'RET:M008';
UPDATE line set color_shield = '003a8c', color_text= 'ffffff' WHERE operator_id = 'RET:M010';
UPDATE line set color_shield = '34b4e4', color_text= '000000' WHERE operator_id = 'RET:M009';
""")
    cur.close()
    conn.commit()
    conn.close()

def generatePool(conn):
    cur = conn.cursor()
    cur.execute("""
CREATE TEMPORARY TABLE temp_pool as (
SELECT dataownercode,userstopcodebegin,userstopcodeend,transporttype,row_number() OVER (PARTITION BY 
dataownercode,userstopcodebegin,userstopcodeend,transporttype ORDER BY index) as index,locationx_ew,locationy_ns
FROM 
((SELECT DISTINCT ON (userstopcodebegin,userstopcodeend,transporttype)
 dataownercode,userstopcodebegin,userstopcodeend,transporttype,0 as index,locationx_ew,locationy_ns
 FROM pool JOIN point using (version,dataownercode,pointcode)
 ORDER BY userstopcodebegin,userstopcodeend,transporttype,distancesincestartoflink ASC)
UNION
(SELECT DISTINCT ON (userstopcodebegin,userstopcodeend,transporttype)
 dataownercode,userstopcodebegin,userstopcodeend,transporttype,99999 as index,locationx_ew,locationy_ns
 FROM pool JOIN point using (version,dataownercode,pointcode)
 ORDER BY userstopcodebegin,userstopcodeend,transporttype,distancesincestartoflink DESC)
UNION
SELECT dataownercode,userstopcodebegin,userstopcodeend,transporttype,(dp).path[1] as index,st_x((dp).geom)::integer as 
locationx_ew,st_y((dp).geom)::integer as locationy_ns
FROM
(SELECT dataownercode,userstopcodebegin,userstopcodeend,transporttype,st_dumppoints(geom) as dp FROM ret_pool_geom) as x) as pool
ORDER BY dataownercode,userstopcodebegin,userstopcodeend,transporttype,index);

DELETE FROM temp_pool WHERE userstopcodebegin||':'||userstopcodeend||':'||transporttype NOT in (SELECT DISTINCT 
userstopcodebegin||':'||userstopcodeend||':'||transporttype FROM ret_pool_geom);

INSERT INTO POINT (
SELECT DISTINCT ON (locationx_ew,locationy_ns)
'POINT',1,'I' as implicit,'RET','OG'||row_number() OVER (ORDER BY locationx_ew,locationy_ns),current_date as validfrom,'PL' as pointtype,'RD' as 
coordinatesystemtype,locationx_ew,locationy_ns,0 as locationz, NULL as description
FROM
temp_pool where locationx_ew||':'||locationy_ns not in (select distinct locationx_ew||':'||locationy_ns from point where version = 1)
);
DELETE FROM pool WHERE userstopcodebegin||':'||userstopcodeend||':'||transporttype in (SELECT DISTINCT 
userstopcodebegin||':'||userstopcodeend||':'||transporttype FROM temp_pool) and version = 1;
INSERT INTO pool(
SELECT DISTINCT ON (version, dataownercode, userstopcodebegin, userstopcodeend, linkvalidfrom, pointcode, transporttype)
'POOL',l.version,'I',p.dataownercode,p.userstopcodebegin,p.userstopcodeend,l.validfrom as linkvalidfrom,p.dataownercode,pt.pointcode,
SUM(coalesce(st_distance(st_setsrid(st_makepoint(p.locationx_ew,p.locationy_ns),28992),st_setsrid(st_makepoint(prev.locationx_ew,prev.locationy_ns),28992))::integer,0))
OVER (PARTITION BY l.version,p.dataownercode,p.userstopcodebegin,p.userstopcodeend,p.transporttype
      ORDER BY p.index
      ROWS between UNBOUNDED PRECEDING and 0 PRECEDING) as distancesincestartoflink,
NULL as sgementspeed,NULL as localpointspeed,NULL as description,p.transporttype
FROM
temp_pool as p JOIN link as l USING (dataownercode,userstopcodebegin,userstopcodeend,transporttype)
               JOIN (SELECT DISTINCT ON (version,locationx_ew,locationy_ns) version,locationx_ew,locationy_ns,pointcode
                     FROM POINT ) AS pt USING (locationx_ew,locationy_ns)
               LEFT JOIN temp_pool as prev ON (p.index = prev.index +1 AND p.transporttype = prev.transporttype
                                               AND p.userstopcodebegin = prev.userstopcodebegin AND p.userstopcodeend = prev.userstopcodeend));
""")

def getMergeStrategies(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT 'DATASOURCE' as type,'1' as datasourceref,min(validdate) as fromdate FROM operday GROUP BY dataownercode
""")
    rows = cur.fetchall()
    cur.close()
    return rows

def fixBob(conn):
   cur = conn.cursor()
   cur.execute("""
update line set linepublicnumber = 'B'||linepublicnumber,linename = 'B'||linename where cast(linepublicnumber as integer) < 20 and transporttype = 'BUS';""")
   cur.close

def import_zip(path,filename,version):
    meta,conn = load(path,filename)
    if datetime.strptime(meta['enddate'].replace('-',''),'%Y%m%d') < (datetime.now() - timedelta(days=1)):
        data = {}
        data['DATASOURCE'] = getDataSource()
        data['VERSION'] = {}
        data['VERSION']['1'] = {'privatecode'   : 'RET:'+filename,
                              'datasourceref' : '1',
                              'operator_id'   : 'RET:'+filename,
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
        fixBob(conn)
        generatePool(conn)
        data = {}
        data['OPERATOR'] = getOperator()
        data['MERGESTRATEGY'] = getMergeStrategies(conn)
        data['DATASOURCE'] = getDataSource()
        data['VERSION'] = {}
        data['VERSION']['1'] = {'privatecode'   : 'RET:'+filename,
                             'datasourceref' : '1',
                              'operator_id'   : 'RET:'+filename,
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

url = 'http://data.ndovloket.nl/RET/'

def sync():
    f = urllib2.urlopen(url+'?order=d')
    soup = BeautifulSoup(f.read())
    for link in soup.find_all('a'):
        link = link.get('href')
        filename = urllib2.unquote(link)
        if '.zip' in link.lower():
            if not version_imported('RET:'+filename):
              try:
                logger.info('Importing :'+filename)
                download(url+link,filename)
              except Exception as e:
                logger.error(filename,exc_info=True)
                pass
