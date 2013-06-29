from iff import *
from iffreader import load
from inserter import insert,version_imported,reject
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
