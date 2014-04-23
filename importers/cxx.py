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
              'OVREGIOY' :    {'privatecode' : 'OVREGIOY',
                               'operator_id' : 'OVREGIOY',
                               'name'        : 'OV Regio IJsselmond',
                               'phone'       : '0900-2666399',
                               'url'         : 'http://www.ovregioijsselmond.nl',
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
create temporary table NewJourney(
    id bigserial primary key NOT NULL,
    privatecode varchar(255) NOT NULL,
    operator_id varchar(255) NOT NULL,
    availabilityconditionRef integer,
    journeypatternref integer,
    timedemandgroupref integer,
    productCategoryRef integer,
    noticeassignmentRef integer,
    departuretime integer,
    blockref varchar(255),
    name varchar(255),
    lowfloor boolean,
    hasLiftOrRamp boolean,
    haswifi boolean,
    bicycleAllowed boolean,
    onDemand boolean,
    isvirtual boolean default(false)
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
SELECT array_agg(distinct newjourney.availabilityconditionref) as availabilityconditions
FROM 
journey JOIN (SELECT availabilityconditionref,array_agg(validdate ORDER BY validdate) as days
              FROM availabilityconditionday GROUP BY availabilityconditionref) as jac USING (availabilityconditionref)
        JOIN timedemandgroup as oj ON (oj.id = journey.timedemandgroupref)
        JOIN journeypattern as ojp ON (ojp.id = journey.journeypatternref)
        JOIN route AS orr ON (orr.id = ojp.routeref)
        JOIN line AS ol ON (ol.id = orr.lineref)
        JOIN (SELECT journeypatternref,array_agg(pointref ORDER BY pointorder) as points
              FROM pointinjourneypattern GROUP BY journeypatternref) as jjp USING (journeypatternref)
,newjourney JOIN (SELECT availabilityconditionref,array_agg(validdate ORDER BY validdate) as days
                       FROM availabilityconditionday WHERE isavailable = true GROUP BY availabilityconditionref) as nac USING 
(availabilityconditionref)
        JOIN timedemandgroup as nt ON (nt.id = newjourney.timedemandgroupref)
        JOIN journeypattern as njpp ON (njpp.id = newjourney.journeypatternref)
        JOIN route AS nr ON (nr.id = njpp.routeref)
        JOIN line AS nl ON (nl.id = nr.lineref)
        JOIN (SELECT journeypatternref,array_agg(pointref ORDER BY pointorder) as points
              FROM pointinjourneypattern GROUP BY journeypatternref) as njp USING (journeypatternref)
WHERE 
journey.operator_id = newjourney.operator_id AND
--nac.days = jac.days AND
(jjp.points != njp.points OR oj.operator_id != nt.operator_id OR nl.operatorref != ol.operatorref);
""",[data['_validfrom']])
    availabilityConditionrefs = cur.fetchone()['availabilityconditions']
    print availabilityConditionrefs
    if availabilityConditionrefs is None:
        availabilityConditionrefs = []
    print str(len(availabilityConditionrefs)) + ' calendars dirty'
    cur.execute("""
UPDATE availabilityconditionday SET isavailable = false
WHERE availabilityConditionref != any(%s) AND validdate < %s AND availabilityconditionref in (SELECT id FROM availabilitycondition 
                                                                                              WHERE versionref = %s)
;
""",[availabilityConditionrefs,data['_validfrom'],data['VERSION']['1']])
    cur.execute("CREATE INDEX ON newjourney(operator_id)")
    cur.execute("""
SELECT journey.operator_id,journey.id,newjourney.id as tmp_id
FROM 
journey JOIN availabilitycondition as oac ON (oac.id = journey.availabilityconditionref)
,newjourney JOIN availabilitycondition as nac ON (nac.id = newjourney.availabilityconditionref)
WHERE 
journey.operator_id = newjourney.operator_id AND
(%s = 0 or newjourney.availabilityconditionref != any(%s))
""",[len(availabilityConditionrefs),availabilityConditionrefs])
    count = 0
    for row in cur.fetchall():
        count += 1
        data['JOURNEY'][row['operator_id']]['id'] = row['id']
        cur.execute("delete from newjourney where id = %s",[row['tmp_id']])
    print str(count) + ' journeys recycled'

def import_zip(path,filename,version):
    validthru = '2015-01-04'
    meta,conn = load(path,filename)
    validfrom = version['validfrom']
    print validfrom
    cur = conn.cursor()
    cur.execute("""create index on pool(userstopcodebegin,userstopcodeend);""")
    cur.close()
    try:
        data = {}
        data['_validfrom'] = version['validfrom']
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
        insert(data,recycle_journeyids=recycle_journeyids)
        conn.commit()
        conn.close()
    except:
        raise

def sync():
    raise Exception('Sync not yet supported')
