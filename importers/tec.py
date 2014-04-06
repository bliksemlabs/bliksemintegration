from beltac import *
from beltacreader import load
from inserter import insert,version_imported,reject,setRefsDict,simple_dict_insert
import urllib2
from datetime import datetime,timedelta
import logging
import zipfile
from cStringIO import StringIO
from bs4 import BeautifulSoup

logger = logging.getLogger("importer")

url = 'http://beltac.tec-wl.be'
path = '/Current%20BLTAC/'

def getDataSource():
    return { '1' : {
                          'operator_id' : 'TEC',
                          'name'        : 'TEC beltec leveringen',
                          'description' : 'TEC beltec leveringen',
                          'email'       : None,
                          'url'         : None}}

def import_zip(path,filename,meta):
    zip = zipfile.ZipFile(path+'/'+filename,'r')
    count = 0
    for name in zip.namelist():
        unitcode = name.split('.')[0]
        import_subzip(StringIO(zip.read(name)),filename,unitcode,remove_old=count==0)
        count += 1

def import_subzip(zip,versionname,unitcode,remove_old=False):
    meta,conn = load(zip)
    conn.commit()
    try:
        data = {}
        data['OPERATOR'] = {'TEC' : {'url' : 'http://www.infotec.be',
                                     'language' : 'nl',
                                     'phone' : '0',
                                     'timezone' : 'Europe/Amsterdam',
                                     'operator_id' : 'TEC',
                                     'name' : 'TEC',
                                     'privatecode' : 'TEC'}}
        data['MERGESTRATEGY'] = []
        if remove_old:
            data['MERGESTRATEGY'].append({'type' : 'DATASOURCE', 'datasourceref' : '1'})
        data['DATASOURCE'] = getDataSource()
        data['VERSION'] = {}
        data['VERSION']['1'] = getVersion(conn,versionname,prefix='TEC')
        data['DESTINATIONDISPLAY'] = getDestinationDisplays(conn,prefix='TEC')
        data['LINE'] = getLines(conn,prefix='TEC',operatorref='TEC')
        data['STOPPOINT'] = getStopPoints(conn,prefix='TEC')
        data['STOPAREA'] = getStopAreas(conn,prefix='TEC')
        data['AVAILABILITYCONDITION'] = getAvailabilityConditions(conn,prefix='TEC',unitcode=unitcode)
        data['PRODUCTCATEGORY'] = getProductCategories(conn)
        data['ADMINISTRATIVEZONE'] = {}
        timedemandGroupRefForJourney,data['TIMEDEMANDGROUP'] = calculateTimeDemandGroups(conn,prefix='TEC')
        routeRefForPattern,data['ROUTE'] = clusterPatternsIntoRoute(conn,getFakePool,prefix='TEC')
        data['JOURNEYPATTERN'] = getJourneyPatterns(routeRefForPattern,conn,data['ROUTE'],prefix='TEC')
        data['JOURNEY'] = getJourneys(timedemandGroupRefForJourney,conn,prefix='TEC')
        data['NOTICEASSIGNMENT'] = getNoticeAssignments(conn,prefix='TEC')
        data['NOTICE'] = getNotices(conn,prefix='TEC')
        data['NOTICEGROUP'] = getNoticeGroups(conn,prefix='TEC')
        conn.close()
        insert(data)
    except:
        raise

def download(url,filename):
    print 'ATTEMPT '+ url
    return
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

def sync():
    f = urllib2.urlopen(url+'/'+path)
    soup = BeautifulSoup(f.read())
    files = []
    for link in soup.find_all('a'):
        link = link.get('href')
        filename = urllib2.unquote(link).split('/')[-1]
        if '.zip' in link.lower():
            if not version_imported('TEC:'+filename):
                files.append((link,filename))
    for link,filename in sorted(files):
        try:

            print 'FILE '+filename
            logger.info('Importing :'+filename)
            download(url+link,filename)
        except Exception as e:
            logger.error(filename,exc_info=True)
            pass
