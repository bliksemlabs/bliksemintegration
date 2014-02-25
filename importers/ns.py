from iff import *
from iffreader import load
from inserter import insert,version_imported,reject,setRefsDict,simple_dict_insert
from bs4 import BeautifulSoup
import urllib2
from datetime import datetime,timedelta
import logging

logger = logging.getLogger("importer")

def getDataSource():
    return { '1' : {
                          'operator_id' : 'NS',
                          'name'        : 'NS IFF leveringen',
                          'description' : 'IFF NS levering voor dienstregeling op spoorwegen',
                          'email'       : None,
                          'url'         : None}}

def recycle_journeyids(conn,data):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
create temporary table NewJourney(
    id bigserial primary key NOT NULL,
    privatecode varchar(255) NOT NULL,
    operator_id varchar(255) NOT NULL,
    availabilityconditionRef integer NOT NULL,
    journeypatternref integer NOT NULL,
    timedemandgroupref integer NOT NULL,
    productCategoryRef integer,
    noticeassignmentRef integer,
    departuretime integer,
    blockref varchar(255),
    name varchar(255),
    lowfloor boolean,
    hasLiftOrRamp boolean,
    haswifi boolean,
    bicycleAllowed boolean,
    onDemand boolean
);
""")
    for key,journey in data['JOURNEY'].items():
        journey = deepcopy(journey)
        setRefsDict(journey,data['AVAILABILITYCONDITION'],'availabilityconditionref')
        setRefsDict(journey,data['JOURNEYPATTERN'],'journeypatternref')
        setRefsDict(journey,data['TIMEDEMANDGROUP'],'timedemandgroupref')
        setRefsDict(journey,data['NOTICEASSIGNMENT'],'noticeassignmentref',ignore_null=True)
        setRefsDict(journey,data['PRODUCTCATEGORY'],'productcategoryref')
        exists,id = simple_dict_insert(conn,'NEWJOURNEY',journey,check_existing=False,return_id=True)
    cur.execute("""
SELECT jn.operator_id,jo.id,jn.id as tmp_id
FROM
journey as jo LEFT JOIN (SELECT availabilityconditionref,array_agg(validdate ORDER BY validdate) as days
                         FROM availabilityconditionday GROUP BY availabilityconditionref) as ado USING (availabilityconditionref)
,newjourney as jn LEFT JOIN (SELECT availabilityconditionref,array_agg(validdate ORDER BY validdate) as days
                         FROM availabilityconditionday GROUP BY availabilityconditionref) as adn USING (availabilityconditionref)
WHERE
ado.days = adn.days AND
jo.name = jn.name AND
jo.departuretime = jn.departuretime
UNION
SELECT jn.operator_id,jo.id,jn.id as tmp_id
FROM
journey as jo,newjourney as jn
WHERE
jo.name = jn.name AND
jo.departuretime = jn.departuretime AND
jo.operator_id = jn.operator_id
UNION
SELECT jn.operator_id,jo.id,jn.id as tmp_id
FROM
journey as jo,newjourney as jn
WHERE
jo.name = jn.name AND
jo.departuretime = jn.departuretime AND
jo.operator_id = jn.operator_id
""")
    for row in cur.fetchall():
        data['JOURNEY'][row['operator_id']]['id'] = row['id']
        cur.execute("delete from newjourney where id = %s",[row['tmp_id']])
        cur.execute("delete from journeytransfers where journeyref = %s or onwardjourneyref = %s",[row['id']]*2)
def import_zip(path,filename,version):
    print (path,filename)
    meta,conn = load(path,filename)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT station||':'||coalesce(platform,'0') FROM passtimes WHERE station||':'||coalesce(platform,'0') not in (select id from quays)")
    for row in cur.fetchall():
        print row
    cur.close()
    try:
        data = {}
        data['OPERATOR'] = getOperator(conn)
        data['MERGESTRATEGY'] = [{'type' : 'DATASOURCE', 'datasourceref' : '1'}]
        data['DATASOURCE'] = getDataSource()
        data['VERSION'] = {}
        data['VERSION']['1'] = getVersion(conn,filename)
        data['DESTINATIONDISPLAY'] = getDestinationDisplays(conn)
        data['LINE'] = getLines(conn)
        data['STOPPOINT'] = getStopPoints(conn)
        data['STOPAREA'] = getStopAreas(conn)
        data['AVAILABILITYCONDITION'] = getAvailabilityConditions(conn)
        data['PRODUCTCATEGORY'] = getProductCategories(conn)
        data['ADMINISTRATIVEZONE'] = {}
        timedemandGroupRefForJourney,data['TIMEDEMANDGROUP'] = calculateTimeDemandGroups(conn)
        routeRefForPattern,data['ROUTE'] = clusterPatternsIntoRoute(conn,getPoolIFF)
        data['JOURNEYPATTERN'] = getJourneyPatterns(routeRefForPattern,conn,data['ROUTE'])
        data['JOURNEY'] = getJourneys(timedemandGroupRefForJourney,conn)
        data['JOURNEYTRANSFERS'] = getTripTransfers(conn)
        data['NOTICEASSIGNMENT'] = getNoticeAssignments(conn)
        data['NOTICE'] = getNotices(conn)
        data['NOTICEGROUP'] = getNoticeGroups(conn)
        conn.close()
        insert(data,recycle_journeyids=recycle_journeyids)
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
