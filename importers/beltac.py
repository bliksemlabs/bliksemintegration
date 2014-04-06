import psycopg2
import psycopg2.extras
from datetime import datetime
import md5
from copy import deepcopy

cache = {}

def getFakePool(conn,stopbegin,stopend):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT * FROM (
SELECT
stop_id as privatecode,
1 as pointorder,
ST_Y(the_geom)::NUMERIC(9,6)::text AS latitude,
ST_X(the_geom)::NUMERIC(8,6)::text AS longitude,
0 as distancefromstart
FROM 
(SELECT *,st_transform(st_setsrid(st_makepoint(lambert72_x,lambert72_y),31370),4326) as the_geom from stops) as stops
WHERE stop_id = %s
UNION 
SELECT
s2.stop_id as privatecode,
2 as pointorder,
ST_Y(st_transform(s2.the_geom,4326))::NUMERIC(9,6)::text AS latitude,
ST_X(st_transform(s2.the_geom,4326))::NUMERIC(8,6)::text AS longitude,
st_distance(s1.the_geom,s2.the_geom)::integer as distancefromstart
FROM (SELECT *,st_setsrid(st_makepoint(lambert72_x,lambert72_y),31370) as the_geom from stops) as s1,
     (SELECT *,st_setsrid(st_makepoint(lambert72_x,lambert72_y),31370) as the_geom from stops) as s2
WHERE s1.stop_id = %s AND s2.stop_id = %s) as x
ORDER BY pointorder
""",[stopbegin,stopbegin,stopend])
    return cur.fetchall()

def calculateTimeDemandGroups(conn,prefix=None):
    cur = conn.cursor('timdemgrps',cursor_factory=psycopg2.extras.RealDictCursor)
    timdemgroup_ids = {}
    timdemgroups = {}
    journeyinfo = {}
    cur.execute("""
SELECT concat_ws(':',%s,trip_id) as JOURNEY_id, 
array_agg(cast(idx as integer) order by idx) as stoporders,array_agg(toseconds(coalesce(arrivaltime,departuretime),0) order by idx) as 
arrivaltimes,array_agg(toseconds(coalesce(departuretime,arrivaltime),0) order by idx) as departuretimes
FROM timetable_stop
GROUP BY JOURNEY_id
""",[prefix])
    for row in cur:
        points = [(row['stoporders'][0],0,0)]
        dep_time = row['departuretimes'][0]
        for i in range(len(row['stoporders'][:-1])):     
            cur_arr_time = row['arrivaltimes'][i+1]
            cur_dep_time = row['departuretimes'][i+1]
            points.append((row['stoporders'][i+1],cur_arr_time-dep_time,cur_dep_time-cur_arr_time))
        m = md5.new()
        m.update(str(points))
        timdemgrp = {'POINTS' : []}
        for point in points:
            point_dict = {'pointorder' : point[0],'totaldrivetime' : point[1], 'stopwaittime' : point[2]}
            timdemgrp['POINTS'].append(point_dict)
        if len(timdemgrp['POINTS']) == 0:
            raise exception('TIMEDEMAND GROUP EMPTY?')
        journeyinfo[row['journey_id']] = {'departuretime' : dep_time, 'timedemandgroupref' : m.hexdigest()}
        timdemgrp['operator_id'] = m.hexdigest()
        timdemgroups[m.hexdigest()] = timdemgrp
    cur.close()
    return (journeyinfo,timdemgroups)

def getStopPoints(conn,prefix=None):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if prefix is None:
        prefix = 'BELTAC'
    userstops = {}
    cur.execute("""
SELECT
%s||':'||stop_id as operator_id,
stop_id as privatecode,
%s||':'||stoparea_id as stoparearef,
description_nl as name,
municipality_nl as town,
(ispublic AND description_nl not ilike '%%fictif%%') as isscheduled,
ST_Y(the_geom)::NUMERIC(9,6)::text AS latitude,
ST_X(the_geom)::NUMERIC(8,6)::text AS longitude,
NULL as rd_x,
NULL as rd_y,
NULL as platformcode,
accessible as restrictedmobilitysuitable
FROM (SELECT *,st_transform(st_setsrid(st_makepoint(lambert72_x,lambert72_y),31370),4326) as the_geom from stops) as stops
LEFT JOIN (select stoparea_id from stopareas) as stopareas ON (substring(stop_id for char_length(stop_id)-1) = stoparea_id);
""",[prefix]*2)
    for row in cur.fetchall():
        userstops[row['operator_id']] = row
    cur.close()
    return userstops

def getStopAreas(conn,prefix=None):
    if prefix is None:
        prefix = 'BELTAC'
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    stopareas = {}
    cur.execute("""
SELECT 
'TEC'||':'||stoparea_id as operator_id,
stoparea_id as privatecode,
stoparea_id as publiccode,
name,
town,
ST_Y(the_geom)::NUMERIC(9,6)::text AS latitude,
ST_Y(the_geom)::NUMERIC(8,6)::text AS longitude,
'Europe/Amsterdam'::text as timezone
FROM (SELECT *,st_transform(st_setsrid(st_makepoint(lambert72_x,lambert72_y),31370),4326) as the_geom from stopareas) as stopareas
""",[prefix]*2)
    for row in cur.fetchall():
        stopareas[row['operator_id']] = row
    cur.close()
    return stopareas

def getAvailabilityConditions(conn,prefix=None,unitcode=None):
    if prefix is None:
        prefix = 'BELTAC'
    if unitcode is None:
        prefix = 1
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    availabilityconditions = {}
    cur.execute("""
SELECT 
%s||':'||calendar_id as operator_id,
CONCAT (
   CASE WHEN (monday    > total / 10) THEN 1 ELSE NULL END,
   CASE WHEN (tuesday   > total / 10) THEN 2 ELSE NULL END,
   CASE WHEN (wednesday > total / 10) THEN 3 ELSE NULL END,
   CASE WHEN (thursday  > total / 10) THEN 4 ELSE NULL END,
   CASE WHEN (friday    > total / 10) THEN 5 ELSE NULL END,
   CASE WHEN (saturday  > total / 10) THEN 6 ELSE NULL END,
   CASE WHEN (sunday    > total / 10) THEN 7 ELSE NULL END
) as dayflags,
fromdate,
todate,
weeks,
years,
'1' as versionref,
%s as unitcode
FROM (
SELECT 
calendar_id,
sum((extract(isodow from servicedate) = 1)::int4)::integer as monday,
sum((extract(isodow from servicedate) = 2)::int4)::integer as tuesday,
sum((extract(isodow from servicedate) = 3)::int4)::integer as wednesday,
sum((extract(isodow from servicedate) = 4)::int4)::integer as thursday,
sum((extract(isodow from servicedate) = 5)::int4)::integer as friday,
sum((extract(isodow from servicedate) = 6)::int4)::integer as saturday,
sum((extract(isodow from servicedate) = 7)::int4)::integer as sunday,
count(distinct servicedate) as total,
array_agg(extract(week from servicedate)::integer ORDER BY servicedate) as weeks,
array_agg(extract(year from servicedate)::integer ORDER BY servicedate) as years,
min(servicedate)::text as fromdate,
max(servicedate)::text as todate
FROM calendar
GROUP BY calendar_id) as x;
""",[prefix,unitcode])
    for row in cur.fetchall():
        signature = ''
        seen = set()
        seen_add = seen.add
        fromDate = datetime.strptime(row['fromdate'],"%Y-%m-%d")
        toDate = datetime.strptime(row['todate'],"%Y-%m-%d")
        now  = datetime.now()
        if len(row['weeks']) > 5 or abs((now - fromDate).days) > 14 or abs((toDate - now).days) > 40:
            signature = 'JD'+str(row['years'][-1])+'-'+str(row['weeks'][0])
        else:
            signature = 'WD'+'_'.join([ str(x) for x in row['weeks'] if x not in seen and not seen_add(x)])
        signature = signature+'_'+row['dayflags']
        row['name'] = signature
        row['privatecode'] = signature
        del(row['weeks'])
        del(row['dayflags'])
        del(row['years'])
        availabilityconditions[row['operator_id']] = row
    cur.execute("""
SELECT 
%s||':'||calendar_id as availabilityconditionRef,
array_agg(servicedate::text) as validdates,
true as isavailable
FROM calendar
GROUP BY calendar_id
ORDER BY calendar_id
;
""",[prefix])
    for row in cur.fetchall():
        availabilityconditions[row['availabilityconditionref']]['DAYS'] = row
    cur.close()
    return availabilityconditions


def getProductCategories(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    productcategories = {}
    productcategories['BELTAC:0'] = {'operator_id' : 'BELTAC:0',
                                     'privatecode' : None,
                                     'name' : None}
    productcategories['BELTAC:1'] = {'operator_id' : 'BELTAC:1',
                                     'privatecode' : None,
                                     'name' : 'Express'}
    productcategories['BELTAC:2'] = {'operator_id' : 'BELTAC:2',
                                     'privatecode' : None,
                                     'name' : 'Scholierenbus'}
    productcategories['BELTAC:3'] = {'operator_id' : 'BELTAC:3',
                                     'privatecode' : None,
                                     'name' : 'Industriebus'}
    productcategories['BELTAC:4'] = {'operator_id' : 'BELTAC:4',
                                     'privatecode' : None,
                                     'name' : 'Ondersteuningsbus'}
    return productcategories

def getNotices(conn,prefix=None):
    if prefix is None:
        prefix = 'BELTAC'
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    notices = {}
    cur.execute("""
SELECT
%s||':'||note_id as operator_id,
NULL as publiccode,
NULL as shortcode,
note_text as name,
4 as processingcode
FROM notes WHERE note_id in (select distinct note_id from timetable_note);
""",[prefix])
    for row in cur.fetchall():
        notices[row['operator_id']] = row
    cur.close()
    return notices

def getNoticeGroups(conn,prefix=None):
    if prefix is None:
        prefix = 'BELTAC'
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    noticegroups = {}
    cur.execute("""
SELECT DISTINCT ON (attrs)
%s||':'||attrs::text as operator_id,noticerefs
FROM
(SELECT array_agg(note_id ORDER BY note_id) as attrs,array_agg('TEC'||':'||note_id ORDER BY note_id) as noticerefs
FROM timetable_note
GROUP BY trip_id,idx) as x
""",[prefix])
    for row in cur.fetchall():
        noticegroups[row['operator_id']] = row
    cur.close()
    return noticegroups

def getNoticeAssignments(conn,prefix=None):
    if prefix is None:
        prefix = 'BELTAC'
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    noticeassignments = {}
    cur.execute("""
SELECT DISTINCT ON (attrs)
%s||':'||attrs::text as noticegroupref,
%s||':'||attrs::text as operator_id,
name
FROM
(SELECT array_agg(note_id ORDER BY note_id) as attrs,array_agg('TEC'||':'||note_id ORDER BY note_id) as noticerefs,string_agg(note_text,', ') as name
FROM timetable_note JOIN notes USING (note_id)
GROUP BY trip_id,idx) as x
""",[prefix]*2)
    for row in cur.fetchall():
        noticeassignments[row['operator_id']] = row
    cur.close()
    return noticeassignments

"""
SELECT
NULL as privatecode,
%s||':'||route_id||'.1' as operator_id,
trim(both from replace(direction_name1,'vers ','')) as name,
trim(both from replace(direction_name1,'vers ','')) as shortname
FROM routes
WHERE direction_name1 is not null
UNION
SELECT
NULL as privatecode,
%s||':'||route_id||'.2',
trim(both from replace(direction_name2,'vers ','')) as name,
trim(both from replace(direction_name2,'vers ','')) as shortname
FROM routes
WHERE direction_name2 is not null
"""

def getDestinationDisplays(conn,prefix=None):
    if prefix is None:
        prefix = 'BELTAC'
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    destinationdisplays = {}
    cur.execute("""
SELECT DISTINCT ON (last_stopid)
last_stopid as privatecode,
%s||':'||last_stopid as operator_id,
description_nl as name,
description_nl as shortname
FROM journeypattern JOIN stops ON (stop_id = last_stopid)
""",[prefix])
    for row in cur.fetchall():
        destinationdisplays[row['operator_id']] = row
    cur.close()
    return destinationdisplays

def clusterPatternsIntoRoute(conn,getPool,prefix=None):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if prefix is None:
        prefix = 'BELTAC'
    cur.execute("""
SELECT 
concat_ws(':',%s,route_id) as route_id,array_agg(patterncode ORDER BY char_length(pattern) DESC,patterncode) as 
patterncodes,array_agg(pattern ORDER BY char_length(pattern) DESC,patterncode) as patterns
FROM
(SELECT route_id,%s||':'||journeypatterncode as patterncode,string_agg(stop_id,'>') as pattern 
FROM (SELECT DISTINCT ON (route_id,journeypatterncode,idx) *
      From timetable_stop JOIN journeypattern USING (trip_id)
      order by route_id,journeypatterncode,idx) as passtimes
GROUP BY route_id,journeypatterncode) as y
GROUP BY route_id""",[prefix]*2)
    rows = cur.fetchall()
    patterncodeInRoute = {}
    for row in rows:
        if row['route_id'] not in patterncodeInRoute:
            patterncodeInRoute[row['route_id']] = [ (row['patterns'][0],[row['patterncodes'][0]]) ]
        for i in range(len(row['patterncodes'][1:])):
            pattern = row['patterns'][i+1]
            patterncode = row['patterncodes'][i+1]
            route_found = False
            for route in patterncodeInRoute[row['route_id']]:
                if pattern in route[0]:
                    route[1].append(patterncode)
                    route_found = True
                    break
            if not route_found:
                patterncodeInRoute[row['route_id']].append((pattern,[patterncode]))
    routes_result = {}
    routeRefForPattern = {}
    linecounter = 0
    for line,routes in patterncodeInRoute.items():
        linecounter += 1
        print '%s / %s' % (linecounter,len(patterncodeInRoute))
        for routes in routes:
            result = {'POINTS' : []}
            stops = routes[0].split('>')
            for i in range(len(stops)-1):
                stopbegin = stops[i]
                stopend = stops[i+1]
                if len(result['POINTS']) == 0:
                    order = 0
                    distance = 0
                else:
                    order = result['POINTS'][-1]['pointorder']
                    distance = result['POINTS'][-1]['distancefromstart']
                pool = getPool(conn,stopbegin,stopend)
                if len(pool) == 0:
                    raise Exception('KV1: Pool empty')
                for point in pool:
                    if len(result['POINTS']) > 0 and point['privatecode'] is not None and result['POINTS'][-1]['privatecode'] == point['privatecode']:
                        continue
                    point['pointorder'] += order
                    point['distancefromstart'] += distance 
                    result['POINTS'].append(point)
            m = md5.new()
            result['lineref'] = line
            m.update(str(result))
            result['operator_id'] = m.hexdigest()
            routes_result[m.hexdigest()] = result
            for patterncode in routes[1]:
                routeRefForPattern[patterncode] = m.hexdigest()
    cur.close()
    return (routeRefForPattern,routes_result)


def getJourneyPatterns(routeRefForPattern,conn,routes,prefix=None):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if prefix is None:
        prefix = 'BELTAC'
    journeypatterns = {}
    cur.execute("""
SELECT  DISTINCT ON (operator_id)
concat_ws(':','TEC',journeypatterncode)  as operator_id,
NULL as routeref,
trip_route_direction as directiontype,
'TEC'||':'||last_stopid as destinationdisplayref
FROM 
journeypattern JOIN trips USING (trip_id)
""")
    for row in cur.fetchall():
        journeypatterns[row['operator_id']] = row
        journeypatterns[row['operator_id']]['POINTS'] = []
        row['routeref'] = routeRefForPattern[row['operator_id']]
    cur.execute("""
SELECT DISTINCT ON (journeypatternref,pointorder)
concat_ws(':',%s,journeypatterncode) as journeypatternref,
s1.idx as pointorder,
NULL as privatecode,
NULL as operator_id,
concat_ws(':',%s,s1.stop_id) as pointref,
%s||':'||s2.stop_id as onwardpointref,
NULL as destinationdisplayref,
noticeassignmentRef,
NULL as administrativezoneref,
NULL as iswaitpoint,
0 as waittime,
NULL as requeststop,
true as foralighting,
true as forboarding,
NULL as distancefromstartroute,
NULL as fareunitspassed
FROM timetable_stop as s1 LEFT JOIN (SELECT trip_id,idx,%s||':'||array_agg(note_id ORDER BY note_id)::text as noticeassignmentRef
                                     FROM timetable_note
                                     WHERE idx IS NOT NULL
                                     GROUP BY trip_id,idx) as notes USING (trip_id,idx)
                          JOIN journeypattern USING (trip_id) JOIN trips USING (route_id,trip_id)
                          LEFT JOIN timetable_stop as s2 ON (s1.trip_id = s2.trip_id and s1.idx = s2.idx -1)
""",[prefix]*4)
    distance = 0
    patternref = None
    for row in cur.fetchall():
        if row['journeypatternref'] != patternref:
            distance = 0
            patternref = row['journeypatternref']
        row['distancefromstartroute'] = distance
        journeypatterns[row['journeypatternref']]['POINTS'].append(row)
    cur.close()
    return journeypatterns

def getVersion(conn,versionname,prefix=None):
    if prefix is None:
        prefix = 'BELTAC'
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT '1' as datasourceref,firstday as startdate, lastday as enddate, release as privatecode,%s as description,%s||':'||%s as operator_id
FROM version LIMIT 1;""",[versionname,prefix,versionname])
    return cur.fetchone()

def getJourneys(timedemandGroupRefForJourney,conn,prefix=None):
    if prefix is None:
        prefix = 'BELTAC'
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT DISTINCT ON (trip_id)
concat_ws(':',%s,trip_id) as privatecode,
concat_ws(':',%s,trip_id) as operator_id,
concat_ws(':', %s,coalesce(blocks.calendar_id,c.calendar_id)) as availabilityconditionRef,
block_id as blockref,
concat_ws(':',%s,journeypatterncode) as journeypatternref,
NULL as timedemandgroupref,
concat_ws(':','BELTAC',route_service_type) as productCategoryRef,
noticeassignmentRef,
NULL as departuretime,
%s||':'||block_id as blockref,
trip_id as name,
accessible as lowfloor,
accessible as hasLiftOrRamp,
NULL as haswifi,
NULL as bicycleAllowed,
NULL as ondemand
FROM trips JOIN journeypattern USING (trip_id)
           LEFT JOIN (SELECT trip_id,%s||':'||array_agg(note_id ORDER BY note_id)::text as noticeassignmentRef
                                     FROM timetable_note
                                     WHERE idx IS NULL
                                     GROUP BY trip_id) as notes USING (trip_id)
           JOIN timetable_calendar as c USING (trip_id)
           LEFT JOIN blocks USING (block_id);
""",[prefix]*6)
    journeys = {}
    for row in cur.fetchall():
        row.update(timedemandGroupRefForJourney[row['operator_id']])
        journeys[row['operator_id']] = row
    cur.close()
    return journeys

def getLines(conn,prefix=None,operatorref=None):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if prefix is None:
        prefix = 'BELTAC'
    if operatorref is None:
        operatorref = 'BELTAC'
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT DISTINCT ON (route_id,route_service_mode)
%s||':'||route_id as operator_id,
route_id as privatecode,
%s as operatorref,
routepubliccode as publiccode,
route_name as name,
FALSE as monitored,
CASE WHEN (route_service_mode = 0) THEN 'BUS'
     WHEN (route_service_mode = 1) THEN 'TRAM'
     WHEN (route_service_mode = 2) THEN 'METRO'
     WHEN (route_service_mode = 3) THEN 'TRAIN' END as transportmode
FROM routes JOIN trips USING (route_id)
""",[prefix,operatorref])
    lines = {}
    for row in cur.fetchall():
        if row['operator_id'] in lines:
            raise Exception('Double operator_id')
        lines[row['operator_id']] = row
    cur.close()
    return lines
