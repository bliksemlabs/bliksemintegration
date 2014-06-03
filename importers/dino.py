import psycopg2
import psycopg2.extras
import md5

def getOperator(conn,prefix=None,website=None):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if website is None:
        website = 'http://www.example.com'
    if prefix is None:
        prefix = 'DINO'
    cur.execute(""" 
SELECT DISTINCT ON (branch_nr)
branch_nr as privatecode,
%s||':'||branch_nr as operator_id,
branch_name as name,
%s as url,
'de' as language,
'Europe/Amsterdam' as timezone
FROM branch;""",[prefix,website])
    rows = cur.fetchall()
    operators = {}
    cur.close()
    for row in rows:
        operators[row['privatecode']] = row
    return operators

def getLines(conn,prefix=None):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if prefix is None:
        prefix = 'DINO'
    lines = {}
    cur.execute("""
SELECT DISTINCT ON (operator_id,transportmode)
rec_lin_ber.branch_nr as operatorref,
'AVV'||':'||version||':'||line_nr as operator_id,
line_nr as privatecode,
line_name as publiccode,
CASE WHEN (lower(veh_type_text) like '%bus%') THEN 'BUS'
     WHEN (lower(veh_type_text) like '%taxi%') THEN 'BUS'
     WHEN (lower(veh_type_text) like '%bahn%') THEN 'TRAIN'
     WHEN (lower(veh_type_text) like '%zug%') THEN 'TRAIN'
     WHEN (lower(veh_type_text) like '%chiff%') THEN 'BOAT'
     ELSE veh_type_text END as TransportMode,
null AS name,
false as monitored
FROM
rec_lin_ber LEFT JOIN rec_trip USING (version,line_nr)
            LEFT JOIN set_vehicle_type USING (version,veh_type_nr)
ORDER BY operator_id,transportmode
""")
    for row in cur.fetchall():
        lines[row['operator_id']] = row
    cur.close()
    return lines

def getDestinationDisplays(conn,prefix=None):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    destinationdisplays = {}
    if prefix is None:
        prefix = 'DINO'
    cur.execute("""
SELECT DISTINCT ON (arr.stop_nr)
arr.stop_nr as privatecode,
%s||':'||arr.stop_nr as operator_id,
arr.stop_name as name,
arr.stop_name as shortname
FROM 
rec_trip,rec_stop as arr,rec_stop as dep
WHERE
dep_stop_nr = dep.stop_nr AND
arr_stop_nr = arr.stop_nr AND
dep.place = arr.place
UNION
SELECT DISTINCT ON (arr.stop_nr)
arr.stop_nr as privatecode,
%s||':P'||arr.stop_nr as operator_id,
arr.place||', '||arr.stop_name,
arr.place as shortname
FROM 
rec_trip,rec_stop as arr,rec_stop as dep
WHERE
dep_stop_nr = dep.stop_nr AND
arr_stop_nr = arr.stop_nr AND
dep.place <> arr.place
""",[prefix]*2)
    for row in cur.fetchall():
        destinationdisplays[row['operator_id']] = row
    cur.close()
    return destinationdisplays

def getStopPoints(conn,prefix=None):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if prefix is None:
        prefix = 'DINO'
    stops = {}
    cur.execute("""
SELECT 
%s||':'||stop_nr||':'||stopping_point_nr as operator_id,
stop_nr||':'||stopping_point_nr as privatecode,
NULL as publiccode,
%s||':'||stop_nr as stoparearef,
place||', '||stop_name AS name,
place as town,
true as isScheduled,
CAST(CAST(ST_Y(the_geom) AS NUMERIC(9,6)) AS text) AS latitude,
CAST(CAST(ST_X(the_geom) AS NUMERIC(8,6)) AS text) AS longitude,
null as rd_x,
null as rd_y
FROM rec_stop JOIN 
              (SELECT *, st_transform(ST_setsrid(st_makepoint(stopping_point_pos_x,stopping_point_pos_y),31466),4326) AS the_geom 
               FROM rec_stopping_points) as stopping USING (stop_nr)""",[prefix]*2)
    for row in cur.fetchall():
        stops[row['operator_id']] = row
    cur.close()
    return stops

def getProductCategories(conn,prefix=None):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if prefix is None:
        prefix = 'DINO'
    productcategory = {}
    cur.execute("""
SELECT 
concat_ws(':',%s,version,str_veh_type) as operator_id,
str_veh_type as privatecode,
veh_type_text as name,
str_veh_type as shortname
FROM
set_vehicle_type""",[prefix])
    for row in cur.fetchall():
        productcategory[row['operator_id']] = row
    cur.close()
    return productcategory

def getAvailabilityConditions(conn,prefix=None):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if prefix is None:
        prefix = 'DINO'
    availabilityConditions = {}
    cur.execute("""
SELECT 
%s||':'||version||':'||restriction as operator_id,
%s||':'||version||':'||restriction as privatecode,
%s as unitcode,
%s||':'||version as versionref,
concat_ws(' ',restrict_text_1,restrict_text_2,restrict_text_3) as name,
cast(date_from as text) as fromdate,
cast(date_until as text) as todate,
bitcalendar(date_from,('x' || restriction_days) :: bit varying(1024))::text[] as days
FROM service_restriction;
""",[prefix]*4)
    for row in cur.fetchall():
        row['DAYS'] = {'validdates' : row['days'], 'isavailable' : True, 'availabilityconditionref' : row['operator_id']}
        del(row['days'])
        availabilityConditions[row['operator_id']] = row
    return availabilityConditions

def getStopAreas(conn,prefix=None):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if prefix is None:
        prefix = 'DINO'
    stops = {}
    cur.execute("""
SELECT DISTINCT ON (operator_id)
%s||':'||stop_nr as operator_id,
stop_nr as privatecode,
place||', '||stop_name AS name,
place as town,
(avg(ST_Y(the_geom)) OVER (PARTITION BY stop_nr))::NUMERIC(9,6)::text AS latitude,
(avg(ST_X(the_geom)) OVER (PARTITION BY stop_nr))::NUMERIC(8,6)::text AS longitude
FROM rec_stop JOIN 
              (SELECT *, st_transform(ST_setsrid(st_makepoint(stopping_point_pos_x,stopping_point_pos_y),31466),4326) AS the_geom 
               FROM rec_stopping_points) as stopping USING (stop_nr)
""",[prefix])
    for row in cur.fetchall():
        stops[row['operator_id']] = row
    cur.close()
    return stops

def clusterPatternsIntoRoute(conn,prefix=None):
    if prefix is None:
        prefix = 'DINO'
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    routes = {}
    cur.execute("""
SELECT DISTINCT ON (operator_id)
concat_ws(':',%s,version,line_nr,line_dir_nr,str_line_var) as operator_id,
%s||':'||version||':'||line_nr as lineref
FROM lid_course
""",[prefix]*2)
    for row in cur.fetchall():
        row['POINTS'] = []
        routes[row['operator_id']] = row
    cur.execute("""
SELECT
concat_ws(':',%s,l.version,l.line_nr,l.line_dir_nr,l.str_line_var) as routeref,
%s||':'||l.stop_nr||':'||l.stopping_point_nr as privatecode,
l.line_consec_nr as pointorder,
ST_Y(p.the_geom)::NUMERIC(8,5)::text AS latitude,
ST_X(p.the_geom)::NUMERIC(7,5)::text AS longitude,
coalesce(SUM(st_distance(st_transform(p.the_geom,31466),lp.the_geom)::integer) OVER (PARTITION BY l.version,l.line_nr,l.line_dir_nr,l.str_line_var
                                                                            ORDER BY l.line_consec_nr),0) as distancefromstart
FROM lid_course as l LEFT JOIN (SELECT *, st_transform(ST_setsrid(st_makepoint(stopping_point_pos_x,stopping_point_pos_y),31466),4326) AS the_geom FROM rec_stopping_points) as p USING (version,stop_nr,stop_type_nr,stopping_point_nr)
                     LEFT JOIN (SELECT * FROM lid_course as lp LEFT JOIN (SELECT *, ST_setsrid(st_makepoint(stopping_point_pos_x,stopping_point_pos_y),31466) AS the_geom FROM rec_stopping_points) as po USING (version,stop_nr,stop_type_nr,stopping_point_nr)) as lp
                               ON (l.version = lp.version AND l.line_nr = lp.line_nr AND l.line_dir_nr = lp.line_dir_nr AND l.str_line_var = lp.str_line_var AND lp.line_consec_nr = l.line_consec_nr-1)
ORDER BY routeref,pointorder
""",[prefix]*2)
    for row in cur.fetchall():
        route = routes[row['routeref']]
        route['POINTS'].append(row)
    return routes

def getTimeDemandGroups(conn,prefix=None):
    if prefix is None:
        prefix = 'DINO'
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    timedemandgroups = {}
    cur.execute("""
SELECT
concat_ws(':',%s,version,line_nr,line_dir_nr,str_line_var,line_dir_nr,timing_group_nr) as operator_id,
concat_ws(':',version,line_nr,line_dir_nr,str_line_var,timing_group_nr)  as privatecode,
cast(line_consec_nr  as integer) as pointorder,
tt_rel as drivingtime,
stopping_time as stopwaittime
FROM lid_travel_time_type
ORDER BY operator_id,pointorder
""",[prefix])
    totaldrivetime = 0
    stopwaittime = 0
    for row in cur.fetchall():
        if row['operator_id'] not in timedemandgroups:
            timedemandgroups[row['operator_id']] = {'operator_id' : row['operator_id'], 'privatecode' : row['privatecode'], 'POINTS' : [{'pointorder' : row['pointorder'],'totaldrivetime' : row['drivingtime'], 'stopwaittime' : row['stopwaittime']}]}
            totaldrivetime = row['drivingtime']
            stopwaittime = row['stopwaittime']
        else:
            points = timedemandgroups[row['operator_id']]['POINTS']
            totaldrivetime += row['drivingtime']
            point_dict = {'pointorder' : row['pointorder'],'totaldrivetime' : totaldrivetime, 'stopwaittime' : row['stopwaittime']}
            points.append(point_dict)
            totaldrivetime += row['stopwaittime']
    for key,row in timedemandgroups.items():
        m = md5.new()
        m.update(str(row['POINTS']))
        row['operator_id'] = m.hexdigest()
    return timedemandgroups

def getJourneyPatterns(conn,routes,prefix=None):
    if prefix is None:
        prefix = 'DINO'
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    journeypatterns = {}
    cur.execute("""
SELECT DISTINCT ON (rec_trip.version,line_nr,line_dir_nr,str_line_var,dep_stop_nr,arr_stop_nr,notice)
concat_ws(':',%s,rec_trip.version,rec_trip.line_nr,line_dir_nr,str_line_var,dep_stop_nr,arr_stop_nr,notice) as operator_id,
concat_ws(':',%s,rec_trip.version,rec_trip.line_nr,line_dir_nr,str_line_var) as routeref,
line_dir_nr as directiontype,
CASE WHEN (arr.place = dep.place) THEN 'AVV'||':'||arr.stop_nr
     ELSE %s||':P'||arr.stop_nr END as destinationdisplayref
FROM
rec_trip,rec_stop as arr,rec_stop as dep
WHERE
dep_stop_nr = dep.stop_nr AND
arr_stop_nr = arr.stop_nr
""",[prefix]*3)
    for row in cur.fetchall():
        journeypatterns[row['operator_id']] = row
        journeypatterns[row['operator_id']]['POINTS'] = []
    cur.execute("""
SELECT DISTINCT ON (journeypatternref,pointorder)
concat_ws(':',%s,t.version,jp.line_nr,jp.line_dir_nr,jp.str_line_var,dep_stop_nr,arr_stop_nr,notice) as journeypatternref,
jp.line_consec_nr::integer as pointorder,
null as privatecode,
concat_ws(':',%s,jp.line_nr,jp.line_dir_nr,jp.str_line_var,dep_stop_nr,arr_stop_nr) as operator_id,
%s||':'||jp.stop_nr||':'||jp.stopping_point_nr as pointref,
%s||':'||jpo.stop_nr||':'||jpo.stopping_point_nr as onwardpointref,
NULL as destinationdisplayref,
NULL as noticeassignmentRef,
%s as administrativezoneref,
NULL as iswaitpoint,
0 as waittime,
NULL as requeststop,
true as foralighting,
true as forboarding,
0 as distancefromstartroute,
0 as fareunitspassed
FROM
rec_trip as t LEFT JOIN lid_course as dep USING (version,line_nr,line_dir_nr,str_line_var)
              LEFT JOIN lid_course as arr USING (version,line_nr,line_dir_nr,str_line_var)
              LEFT JOIN lid_course as jp USING (version,line_nr,line_dir_nr,str_line_var)
              LEFT JOIN lid_course as jpo ON (jp.version = jpo.version AND jp.line_nr = jpo.line_nr 
                                              AND jp.line_dir_nr = jpo.line_dir_nr AND jp.str_line_var = jpo.str_line_var AND
                                              jp.line_consec_nr = jpo.line_consec_nr + 1)
WHERE
dep.stop_nr = dep_stop_nr AND
dep.stop_type_nr = dep_stop_type_nr AND
dep.stopping_point_nr = dep_stopping_point_nr AND
arr.stop_nr = arr_stop_nr AND
arr.stop_type_nr = arr_stop_type_nr AND
arr.stopping_point_nr = arr_stopping_point_nr AND
jp.line_consec_nr between dep.line_consec_nr AND arr.line_consec_nr
ORDER BY journeypatternref,pointorder
""",[prefix]*5)
    distance = 0
    patternref = None
    for row in cur.fetchall():
        if row['journeypatternref'] != patternref:
            distance = 0
            patternref = row['journeypatternref']
        for point in routes[journeypatterns[row['journeypatternref']]['routeref']]['POINTS']:
            if point['distancefromstart'] >= distance and point['privatecode'] == row['pointref']:
                distance = point['distancefromstart']
                row['distancefromstartroute'] = distance
                break
        row['distancefromstartroute'] = distance
        journeypatterns[row['journeypatternref']]['POINTS'].append(row)
    cur.close()
    return journeypatterns

def getJourneys(conn,prefix=None):
    if prefix is None:
        prefix = 'DINO'
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT
concat_ws(':',%s,version,line_nr,trip_id) as privatecode,
concat_ws(':',%s,version,trip_id) as operator_id,
concat_ws(':',%s,version,restriction) as availabilityconditionRef,
concat_ws(':',%s,version,line_nr,line_dir_nr,str_line_var,dep_stop_nr,arr_stop_nr,notice) as journeypatternref,
concat_ws(':',%s,version,line_nr,line_dir_nr,str_line_var,line_dir_nr,timing_group_nr) as timedemandgroupref,
concat_ws(':',%s,version,str_veh_type) as productCategoryRef,
NULL as noticeassignmentRef,
departuretime,
NULL as blockref,
coalesce(coalesce(trip_id_printing,train_nr),trip_id) as name,
NULL as lowfloor,
NULL as hasLiftOrRamp,
NULL as haswifi,
NULL as bicycleallowed,
lower(veh_type_text) like '%%taxi%%' as onDemand
FROM rec_trip LEFT JOIN set_vehicle_type USING (version,veh_type_nr)
""",[prefix]*6)
    journeys = {}
    for row in cur.fetchall():
        journeys[row['operator_id']] = row
    cur.close()
    return journeys

def getVersion(conn,prefix=None,filename=None):
    if prefix is None:
        prefix = 'DINO'
    if filename is None:
        filename = ''
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
select '1' as datasourceref, %s||':'||version as operator_id,period_date_from as startdate,period_date_to as enddate,version||':'||%s as privatecode, 
version_text as description
from set_version;""",[prefix,filename])
    version = {}
    for row in cur.fetchall():
        version[row['operator_id']] = row
    return version
