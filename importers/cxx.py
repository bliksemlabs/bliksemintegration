from kv1_810 import *
from inserter import insert,version_imported,reject,setRefsDict,simple_dict_insert
from bs4 import BeautifulSoup
import urllib2
from datetime import datetime,timedelta
import logging

logger = logging.getLogger("importer")

def getDataSource():
    return { '1' : {
                          'operator_id' : 'CXX',
                          'name'        : 'Connexxion KV1',
                          'description' : 'Connexxion KV1 leveringen,rijtijdgroepen',
                          'email'       : None,
                          'url'         : None}}

def getOperator():
    return {
              'BRENG' :       {'privatecode' : 'BRENG',
                               'operator_id' : 'BRENG',
                               'name'        : 'Breng',
                               'phone'       : '026-2142140',
                               'url'         : 'http://www.breng.nl',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'},
              'CXX' :          {'privatecode' : 'CXX',
                               'operator_id' : 'CXX',
                               'name'        : 'Connexxion',
                               'phone'       : '0900-2666399',
                               'url'         : 'http://www.connexxion.nl',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'},
              'GVU' :         {'privatecode' : 'GVU',
                               'operator_id' : 'GVU',
                               'name'        : 'GVU',
                               'phone'       : '0900-8998959',
                               'url'         : 'http://www.gvu.nl',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'},
              'NIAG' :        {'privatecode' : 'NIAG',
                               'operator_id' : 'NIAG',
                               'name'        : 'NIAG',
                               'phone'       : '+4901803504030',
                               'url'         : 'http://www.niag-online.de/',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'},
              'HERMES' :      {'privatecode' : 'HERMES',
                               'operator_id' : 'HERMES',
                               'name'        : 'Hermes',
                               'phone'       : '0800-0222277',
                               'url'         : 'http://www.hermes.nl',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'}
           }

def recycle_journeyids(conn,data):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
create table NewJourney(
    id bigserial primary key NOT NULL,
    privatecode varchar(255) NOT NULL,
    operator_id varchar(255) NOT NULL,
    availabilityconditionRef integer references AvailabilityCondition(id) NOT NULL,
    journeypatternref integer references JourneyPattern(id) NOT NULL,
    timedemandgroupref integer references timedemandgroup(id) NOT NULL,
    productCategoryRef integer references productCategory (id) NOT NULL,
    noticeassignmentRef integer references noticeassignment(id),
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
journey as jo
,newjourney as jn
WHERE
jo.operator_id = jn.operator_id
""")
    for row in cur.fetchall():
        data['JOURNEY'][row['operator_id']]['id'] = row['id']
        cur.execute("delete from newjourney where id = %s",[row['tmp_id']])

def import_zip(path,filename,version):
    validthru = '2014-01-04'
    meta,conn = load(path,filename)
    validfrom = meta['validfrom']
    print validfrom
    cur = conn.cursor()
    cur.execute("""create index on pool(userstopcodebegin,userstopcodeend);""")
    cur.close()
    try:
        data = {}
        data['OPERATOR'] = getOperator()
        data['MERGESTRATEGY'] = [{'type' : 'DATASOURCE', 'fromdate' : validfrom, 'datasourceref' : '1'}] 
        data['DATASOURCE'] = getDataSource()
        data['VERSION'] = {}
        data['VERSION']['1'] = {'privatecode'   : 'CXX:'+filename,
                             'datasourceref' : '1',
                              'operator_id'   : 'CXX:'+filename,
                              'startdate'     : validfrom,
                              'enddate'       : validthru,
                              'description'   : filename}
        data['DESTINATIONDISPLAY'] = getDestinationDisplays(conn)
        data['LINE'] = getLines(conn)
        data['STOPPOINT'] = getStopPoints(conn)
        data['STOPAREA'] = getStopAreas(conn)
        data['AVAILABILITYCONDITION'] = getAvailabilityConditionsFromCalendars(conn)
        data['JOURNEY'] = {}
        for key,journey in getJourneysFromPujo(conn).items():
            if journey['availabilityconditionref'] not in data['AVAILABILITYCONDITION']:
                logging.warning('Servicecalendar %s missing for %s' % (journey['availabilityconditionref'],journey['operator_id']))
            else:
                data['JOURNEY'][key] = journey
        data['PRODUCTCATEGORY'] = getBISONproductcategories()
        data['ADMINISTRATIVEZONE'] = getAdministrativeZones(conn)
        data['TIMEDEMANDGROUP'] = getTimeDemandGroups(conn)
        routeRefForPattern,data['ROUTE'] = clusterPatternsIntoRoute(conn,getPool805)
        data['JOURNEYPATTERN'] = getJourneyPatterns(routeRefForPattern,conn,data['ROUTE'])
        data['NOTICEASSIGNMENT'] = {}
        data['NOTICE'] = {}
        data['NOTICEGROUP'] = {}
        conn.close()
        insert(data,recycle_journeyids=recycle_journeyids)
    except:
        raise

def sync():
    raise Exception('Sync not yet supported')
