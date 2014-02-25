from kv1_811 import *
from inserter import insert,version_imported
from bs4 import BeautifulSoup
import urllib2
from datetime import datetime,timedelta
import logging 

logger = logging.getLogger("importer")

def getDataSource():
    return { '1' : {
                          'operator_id' : 'EBS',
                          'name'        : 'EBS KV1',
                          'description' : 'EBS KV1 leveringen',
                          'email'       : None,
                          'url'         : None}}

def getOperator():
    return { 'EBS' :          {'privatecode' : 'EBS',
                               'operator_id' : 'EBS',
                               'name'        : 'EBS',
                               'phone'       : '0800-0327',
                               'url'         : 'http://www.ebs-ov.nl',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'}}

def fixLinenumbers(conn):
    cur = conn.cursor()
    cur.execute("""
update line set linepublicnumber = 'N'||linepublicnumber where lineplanningnumber like  '2201_';
update line set linepublicnumber = 'N0'||linepublicnumber where lineplanningnumber like '2200_';
""")
    cur.close()
    return

def setProductFormulas(conn):
    cur = conn.cursor()
    cur.execute("""
UPDATE jopatili SET productformulatype = '1' WHERE confinrelcode = 'wtlBB';
UPDATE jopatili SET productformulatype = '37' WHERE confinrelcode = 'wtlR' and lineplanningnumber not like '220%';""")
    cur.close()
    return

def getMergeStrategies(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT 'DATASOURCE' as type,'1' as datasourceref,min(validdate) as fromdate FROM operday GROUP BY dataownercode
""")
    rows = cur.fetchall()
    cur.close()
    return rows

def cleanDest(conn):
   cur = conn.cursor()
   cur.execute("""
UPDATE dest SET destnamefull = replace(destnamefull,'N01 ','') WHERE destnamefull like 'N01 %';
UPDATE dest SET destnamefull = replace(destnamefull,'N04 ','') WHERE destnamefull like 'N04 %';
UPDATE dest SET destnamefull = replace(destnamefull,'N10 ','') WHERE destnamefull like 'N10 %';
UPDATE dest SET destnamefull = replace(destnamefull,'N14 ','') WHERE destnamefull like 'N14 %';
""")

def import_zip(path,filename,version):
    meta,conn = load(path,filename)
    fixLinenumbers(conn)
    setProductFormulas(conn)    
    cleanDest(conn)
    try:
        data = {}
        data['OPERATOR'] = getOperator()
        data['MERGESTRATEGY'] = getMergeStrategies(conn)
        data['DATASOURCE'] = getDataSource()
        data['VERSION'] = {}
        data['VERSION']['1'] = {'privatecode'   : 'EBS:'+filename,
                             'datasourceref' : '1',
                              'operator_id'   : 'EBS:'+filename,
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
        routeRefForPattern,data['ROUTE'] = clusterPatternsIntoRoute(conn,getFakePool811)
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


url = 'http://data.ndovloket.nl/ebs/'
url = 'http://kv1.openov.nl/ebs/'

def sync():
    f = urllib2.urlopen(url+'?order=d')
    soup = BeautifulSoup(f.read())
    for link in soup.find_all('a'):
        link = link.get('href')
        filename = urllib2.unquote(link)
        if '.zip' in link.lower():
            if not version_imported('EBS:'+filename):
              try:
                logger.info('Importing :'+filename)
                download(url+link,filename)
              except Exception as e:
                logger.error(filename,exc_info=True)
                pass
