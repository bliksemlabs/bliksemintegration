import helper
import psycopg2
import psycopg2.extras
from copy import deepcopy
import md5
from schema.schema_805 import schema
import zipfile
from settings.const import kv1_database_connect

cache = {}

def getFakePool805(conn,lineplanningnumber,userstopcodebegin,userstopcodeend):
    print (lineplanningnumber,userstopcodebegin,userstopcodeend,'fakepool')
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT * FROM(
SELECT
link.dataownercode||':'||link.userstopcodebegin as privatecode,
cast(1 as integer) as pointorder,
cast(CAST(ST_Y(the_geom) AS NUMERIC(8,5)) as text) AS latitude,
cast(CAST(ST_X(the_geom) AS NUMERIC(7,5)) as text) AS longitude,
cast(0 as integer) as distancefromstart
FROM link,(select *,st_transform(st_setsrid(st_makepoint(locationx_ew,locationy_ns),28992),4326) as the_geom from point) as point
WHERE
link.version = point.version AND
link.userstopcodebegin = point.pointcode AND
link.dataownercode||':'||userstopcodebegin = %s AND
link.dataownercode||':'||userstopcodeend = %s
UNION
SELECT
link.dataownercode||':'||link.userstopcodeend as privatecode,
cast(2 as integer) as pointorder,
cast(CAST(ST_Y(the_geom) AS NUMERIC(8,5)) as text) AS latitude,
cast(CAST(ST_X(the_geom) AS NUMERIC(7,5)) as text) AS longitude,
cast(distance as integer) as distancefromstart
FROM link,(select *,st_transform(st_setsrid(st_makepoint(locationx_ew,locationy_ns),28992),4326) as the_geom from point) as point
WHERE
link.version = point.version AND
link.userstopcodeend = point.pointcode AND
link.dataownercode||':'||userstopcodebegin = %s AND
link.dataownercode||':'||userstopcodeend = %s
) as x
order by pointorder
""",[userstopcodebegin,userstopcodeend] * 2)
    try:
        return cur.fetchall()
    finally:
        cur.close()

def getStopAreas(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    stopareas = {}
    cur.execute("""
SELECT
a.dataownercode || ':' ||a.userstopareacode as operator_id,
a.dataownercode || ':' ||a.userstopareacode as privatecode,
CASE WHEN (a.name not like '%,%') THEN a.town||', '||a.name
     ELSE a.name END AS name,
a.town as town,
CAST(CAST(ST_Y(the_geom) AS NUMERIC(9,6)) AS text) AS latitude,
CAST(CAST(ST_X(the_geom) AS NUMERIC(8,6)) AS text) AS longitude
FROM (SELECT stopareacode,
               ST_Transform(ST_setsrid(ST_makepoint(AVG(locationx_ew), AVG(locationy_ns)), 28992), 4326) AS the_geom,
               version
        FROM (SELECT u.dataownercode || ':' ||u.userstopareacode AS stopareacode,
                       locationx_ew,
                       locationy_ns,
                       u.version
                FROM usrstop AS u,
                       point AS p
                WHERE u.dataownercode = p.dataownercode AND
                       u.version = p.version AND
                       u.userstopcode = p.pointcode AND
                       u.userstopareacode IS NOT NULL) AS x
        GROUP BY version,stopareacode) AS y,
        (SELECT DISTINCT ON (dataownercode,userstopareacode) * FROM usrstar ORDER BY dataownercode,userstopareacode,version DESC) AS a
WHERE
stopareacode = a.dataownercode || ':' || a.userstopareacode AND
a.version = y.version
""")
    for row in cur.fetchall():
        stopareas[row['operator_id']] = row
    cur.close()
    return stopareas

def getStopPoints(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    userstops = {}
    cur.execute("""
SELECT 
u.dataownercode||':'||userstopcode as operator_id,
userstopcode as privatecode,
coalesce(timingpointcode,userstopcode) as publiccode,
u.dataownercode||':'||userstopareacode as stoparearef,
CASE WHEN ((getin or getout) AND name not like '%,%') THEN town||', '||name
     ELSE name END AS name,
town,
(getin or getout) as isScheduled,
CAST(CAST(ST_Y(the_geom) AS NUMERIC(9,6)) AS text) AS latitude,
CAST(CAST(ST_X(the_geom) AS NUMERIC(8,6)) AS text) AS longitude,
locationx_ew as rd_x,
locationy_ns as rd_y
FROM usrstop as u, (select version,dataownercode,pointcode,locationx_ew, locationy_ns,ST_Transform(ST_setsrid(ST_makepoint(locationx_ew, 
locationy_ns), 28992), 4326) as the_geom from POINT) as p
WHERE u.version = p.version AND u.dataownercode = p.dataownercode AND u.userstopcode = p.pointcode
""")
    for row in cur.fetchall():
        userstops[row['operator_id']] = row
    cur.close()
    return userstops

def getLines(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    lines = {}
    cur.execute("""
SELECT 
dataownercode as operatorref,
dataownercode||':'||lineplanningnumber as operator_id,
lineplanningnumber as privatecode,
linepublicnumber as publiccode,
CASE WHEN (linepublicnumber = 'FF') THEN 'BOAT' 
     ELSE 'BUS' END  as TransportMode,
CASE WHEN (linepublicnumber <> linename) THEN linename ELSE null END as name
FROM
line
""")
    for row in cur.fetchall():
        lines[row['operator_id']] = row
    cur.close()
    return lines

def getDestinationDisplays(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    destinationdisplays = {}
    cur.execute("""
SELECT
dataownercode||':'||destcode as operator_id,
destcode as privatecode,
destnamefull as name,
destnamemain as shortname,
CASE WHEN destnamedetail is not null AND destnamedetail not in ('-','') THEN destnamedetail ELSE NULL end as vianame
FROM DEST
""")
    for row in cur.fetchall():
        destinationdisplays[row['operator_id']] = row
    cur.close()
    return destinationdisplays

def getAvailabilityConditionsFromSchedvers(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    availabilityConditions = {}
    cur.execute("""
SELECT 
dataownercode||':'||organizationalunitcode||':'||schedulecode||':'||scheduletypecode as operator_id,
dataownercode||':'||organizationalunitcode||':'||schedulecode||':'||scheduletypecode as privatecode,
dataownercode||':'||organizationalunitcode as unitcode,
'1' as versionref,
coalesce(description,dataownercode||':'||organizationalunitcode||':'||schedulecode||':'||scheduletypecode) as name,
cast(validfrom as text) as fromdate,
cast(validthru as text) as todate
FROM schedvers;
""")
    for row in cur.fetchall():
        availabilityConditions[row['operator_id']] = row
    cur.execute("""
SELECT 
dataownercode||':'||organizationalunitcode||':'||schedulecode||':'||scheduletypecode as availabilityconditionRef,
array_agg(cast(validdate as text)) as validdates,
true as isavailable
FROM operday
GROUP BY version,dataownercode,organizationalunitcode,schedulecode,scheduletypecode
;
""")
    for row in cur.fetchall():
        availabilityConditions[row['availabilityconditionref']]['DAYS'] = row
    cur.close()
    return availabilityConditions

def getAvailabilityConditionsUsingOperday(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    availabilityconditions = {}
    cur.execute("""
SELECT 
s.dataownercode||':'||s.organizationalunitcode||':'||s.schedulecode||':'||s.scheduletypecode as operator_id,
s.dataownercode||':'||s.organizationalunitcode||':'||s.schedulecode||':'||s.scheduletypecode as privatecode,
s.dataownercode||':'||s.organizationalunitcode as unitcode,
'1' as versionref,
coalesce(s.description,s.dataownercode||':'||s.organizationalunitcode||':'||s.schedulecode||':'||s.scheduletypecode) as name,
cast(min(validdate) as text) as fromdate,
cast(max(validdate) as text) as todate
FROM schedvers as s left join operday using (version,dataownercode,schedulecode,scheduletypecode)
GROUP BY s.version,s.dataownercode,s.organizationalunitcode,s.schedulecode,s.scheduletypecode
;
""")
    for row in cur.fetchall():
        availabilityconditions[row['operator_id']] = row
    cur.execute("""
SELECT 
dataownercode||':'||organizationalunitcode||':'||schedulecode||':'||scheduletypecode as availabilityconditionRef,
array_agg(validdate) as validdates,
true as isavailable
FROM operday
GROUP BY version,dataownercode,organizationalunitcode,schedulecode,scheduletypecode
;
""")
    for row in cur.fetchall():
        availabilityconditions[row['availabilityconditionref']]['DAYS'] = row
    cur.close()
    return availabilityconditions

def calculateTimeDemandGroups(conn):
    cur = conn.cursor('timdemgrps',cursor_factory=psycopg2.extras.RealDictCursor)
    timdemgroup_ids = {}
    timdemgroups = {}
    journeyinfo = {}
    cur.execute("""
SELECT concat_ws(':',version, dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber) as 
JOURNEY_id, 
array_agg(cast(stoporder as integer) order by stoporder) as stoporders,array_agg(toseconds(coalesce(targetarrivaltime,targetdeparturetime),0) order by stoporder) as 
arrivaltimes,array_agg(toseconds(coalesce(targetdeparturetime,targetarrivaltime),0) order by stoporder) as departuretimes
FROM pujopass
GROUP BY JOURNEY_id
""")
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
        journeyinfo[row['journey_id']] = {'departuretime' : dep_time, 'timedemandgroupref' : m.hexdigest()}
        timdemgrp['operator_id'] = m.hexdigest()
        timdemgroups[m.hexdigest()] = timdemgrp
    cur.close()
    return (journeyinfo,timdemgroups)

def getPool805(conn,lineplanningnumber,stopcodebegin,stopcodeend):
    print (lineplanningnumber,stopcodebegin,stopcodeend,'pool805')
    key = ':'.join([stopcodebegin,stopcodeend])
    if key in cache:
        print 'hit'
        return deepcopy(cache[key])
    userstopcodebegin = stopcodebegin.split(':')[-1]
    userstopcodeend   = stopcodeend.split(':')[-1]
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT
CASE WHEN (pointtype = 'SP') THEN pool.dataownercode||':'||pool.pointcode ELSE NULL END as privatecode,
cast(row_number() over (ORDER BY distancesincestartoflink) as integer) as pointorder,
cast(CAST(ST_Y(the_geom) AS NUMERIC(8,5)) as text) AS latitude,
cast(CAST(ST_X(the_geom) AS NUMERIC(7,5)) as text) AS longitude,
cast(distancesincestartoflink as integer) as distancefromstart
FROM pool,(select *,st_transform(st_setsrid(st_makepoint(locationx_ew,locationy_ns),28992),4326) as the_geom from point) as point
WHERE
pool.version = point.version AND
pool.pointcode = point.pointcode AND
pool.dataownercode||':'||userstopcodebegin = %s AND
pool.dataownercode||':'||userstopcodeend = %s AND
pool.userstopcodebegin = %s AND
pool.userstopcodeend = %s
ORDER BY pointorder
""",[stopcodebegin,stopcodeend,userstopcodebegin,userstopcodeend])
    try:
        cache[key] = cur.fetchall()
        return deepcopy(cache[key])
    finally:
        cur.close()

def clusterPatternsIntoRoute(conn,getPool):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT 
lineplanningnumber,array_agg(journeypatterncode ORDER BY char_length(pattern) DESC,journeypatterncode) as 
patterncodes,array_agg(pattern ORDER BY 
char_length(pattern) DESC,journeypatterncode) as patterns
FROM
(SELECT lineplanningnumber,journeypatterncode,string_agg(userstopcode,'>') as pattern
FROM (
SELECT dataownercode||':'||lineplanningnumber as lineplanningnumber,journeypatterncode,timinglinkorder as stoporder,dataownercode||':'||userstopcodebegin as userstopcode from jopatili
UNION
( SELECT DISTINCT ON (lineplanningnumber,journeypatterncode)
  dataownercode||':'||lineplanningnumber as lineplanningnumber,journeypatterncode,timinglinkorder+1 as stoporder,dataownercode||':'||userstopcodeend as userstopcode FROM jopatili
  ORDER BY lineplanningnumber ASC,journeypatterncode ASC,timinglinkorder DESC)
ORDER BY lineplanningnumber,journeypatterncode,stoporder) as x
GROUP BY lineplanningnumber,journeypatterncode) as y
GROUP BY lineplanningnumber""")
    rows = cur.fetchall()
    patterncodeInRoute = {}
    for row in rows:
        if row['lineplanningnumber'] not in patterncodeInRoute:
            patterncodeInRoute[row['lineplanningnumber']] = [ (row['patterns'][0],[row['patterncodes'][0]]) ]
        for i in range(len(row['patterncodes'][1:])):
            pattern = row['patterns'][i+1]
            patterncode = row['patterncodes'][i+1]
            route_found = False
            for route in patterncodeInRoute[row['lineplanningnumber']]:
                if pattern in route[0]:
                    route[1].append(patterncode)
                    route_found = True
                    break
            if not route_found:
                patterncodeInRoute[row['lineplanningnumber']].append((pattern,[patterncode]))
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
                pool = getPool(conn,line,stopbegin,stopend)
                if len(pool) == 0:
                    raise Exception('KV1: Pool empty')
                if pool[0]['privatecode'] != stopbegin and len(result['POINTS']) == 0:
                    pointbegin = deepcopy(pool[0])
                    pointbegin['privatecode'] = stopbegin
                    order += 1
                    result['POINTS'].append(pointbegin)
                if pool[-1]['privatecode'] != stopend:
                    pointend = deepcopy(pool[-1])
                    pointend['privatecode'] = stopend
                    pointend['pointorder'] += 1
                    pool.append(pointend)
                for point in pool:
                    if len(result['POINTS']) > 0 and point['privatecode'] is not None and result['POINTS'][-1]['privatecode'] == point['privatecode']:
                        continue
                    point['pointorder'] += order
                    point['distancefromstart'] += distance 
                    result['POINTS'].append(point)
            m = md5.new()
            if len(result['POINTS']) < 2:
                raise Exception('Routepoints empty %s ' % (line))
            result['lineref'] = line
            m.update(str(result))
            result['operator_id'] = m.hexdigest()
            routes_result[m.hexdigest()] = result
            for patterncode in routes[1]:
                routeRefForPattern[line+':'+patterncode] = m.hexdigest()
    cur.close()
    return (routeRefForPattern,routes_result)

def getBISONproductcategories():
    return {'0' : {'operator_id' : '0','privatecode' : 0,'name' : None, 'shortname' : None},
            '1' : {'operator_id' : '1','privatecode' : 1,'name' : 'Buurtbus', 'shortname' : None},
            '2' : {'operator_id' : '2','privatecode' : 2,'name' : 'Belbus', 'shortname' : None},
            '3' : {'operator_id' : '3','privatecode' : 3,'name' : 'Express-bus', 'shortname' : None},
            '4' : {'operator_id' : '4','privatecode' : 4,'name' : 'Fast Ferry', 'shortname' : 'FF'},
            '5' : {'operator_id' : '5','privatecode' : 5,'name' : 'Hanze-Liner', 'shortname' : None},
            '6' : {'operator_id' : '6','privatecode' : 6,'name' : 'Interliner', 'shortname' : None},
            '7' : {'operator_id' : '7','privatecode' : 7,'name' : 'Kamperstadslijn', 'shortname' : None},
            '8' : {'operator_id' : '8','privatecode' : 8,'name' : 'Lijntaxi', 'shortname' : None},
            '9' : {'operator_id' : '9','privatecode' : 9,'name' : 'Media express', 'shortname' : None},
            '10' : {'operator_id' : '10','privatecode' : 10,'name' : 'MAXX', 'shortname' : None},
            '11' : {'operator_id' : '11','privatecode' : 11,'name' : 'Natuurexpress', 'shortname' : None},
            '12' : {'operator_id' : '12','privatecode' : 12,'name' : 'Niteliner', 'shortname' : None},
            '13' : {'operator_id' : '13','privatecode' : 13,'name' : 'Q-liner', 'shortname' : None},
            '14' : {'operator_id' : '14','privatecode' : 14,'name' : 'Regioliner', 'shortname' : None},
            '15' : {'operator_id' : '15','privatecode' : 15,'name' : 'Servicebus', 'shortname' : None},
            '16' : {'operator_id' : '16','privatecode' : 16,'name' : 'Sneldienst', 'shortname' : None},
            '17' : {'operator_id' : '17','privatecode' : 17,'name' : 'Spitsbus', 'shortname' : None},
            '18' : {'operator_id' : '18','privatecode' : 18,'name' :  None, 'shortname' : None},
            '19' : {'operator_id' : '19','privatecode' : 19,'name' : 'Sternet', 'shortname' : None},
            '20' : {'operator_id' : '20','privatecode' : 20,'name' : 'Sneltram', 'shortname' : None},
            '21' : {'operator_id' : '21','privatecode' : 21,'name' : 'Tram', 'shortname' : None},
            '22' : {'operator_id' : '22','privatecode' : 22,'name' : 'Vierdaagse', 'shortname' : None},
            '23' : {'operator_id' : '23','privatecode' : 23,'name' : 'Waterbus', 'shortname' : None},
            '24' : {'operator_id' : '24','privatecode' : 24,'name' : 'Zuidtangent', 'shortname' : None},
            '25' : {'operator_id' : '25','privatecode' : 25,'name' : 'Stoptrein', 'shortname' : 'ST'},
            '26' : {'operator_id' : '26','privatecode' : 26,'name' : 'Sneltrein', 'shortname' : 'S'},
            '27' : {'operator_id' : '27','privatecode' : 27,'name' : 'Intercity', 'shortname' : 'IC'},
            '28' : {'operator_id' : '28','privatecode' : 28,'name' : 'Sprinter', 'shortname' : 'SPR'},
            '29' : {'operator_id' : '29','privatecode' : 29,'name' : 'Internationale Trein', 'shortname' : 'INT'},
            '30' : {'operator_id' : '30','privatecode' : 30,'name' : 'Fyra', 'shortname' : 'FYR'},
            '31' : {'operator_id' : '31','privatecode' : 31,'name' : 'ICE', 'shortname' : 'ICE'},
            '32' : {'operator_id' : '32','privatecode' : 32,'name' : 'Thalys', 'shortname' : 'THA'},
            '33' : {'operator_id' : '33','privatecode' : 33,'name' : 'Valleilijn', 'shortname' : None},
            '34' : {'operator_id' : '34','privatecode' : 34,'name' : 'Breng', 'shortname' : None},
            '35' : {'operator_id' : '35','privatecode' : 35,'name' : 'Opstapper', 'shortname' : None},
            '36' : {'operator_id' : '36','privatecode' : 36,'name' : 'Overstapper', 'shortname' : None},
            '37' : {'operator_id' : '37','privatecode' : 37,'name' : 'R-NET', 'shortname' : None},
            '38' : {'operator_id' : '38','privatecode' : 38,'name' : 'Parkshuttle', 'shortname' : None},
            '39' : {'operator_id' : '39','privatecode' : 39,'name' : 'FC-Utrecht Express', 'shortname' : None}
           }

def getJourneyPatterns(routeRefForPattern,conn,routes):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    journeypatterns = {}
    cur.execute("""
SELECT
jopa.dataownercode||':'||lineplanningnumber||':'||journeypatterncode as operator_id,
NULL as routeref,
direction as directiontype,
destinationdisplayref
FROM jopa left join ( SELECT DISTINCT ON (version, dataownercode, lineplanningnumber, journeypatterncode)
			version, dataownercode, lineplanningnumber, journeypatterncode,dataownercode||':'||destcode as destinationdisplayref
			FROM jopatili
			ORDER BY version, dataownercode, lineplanningnumber, journeypatterncode,timinglinkorder ) as jopatili 
                    USING (version, dataownercode, lineplanningnumber, journeypatterncode)""")
    for row in cur.fetchall():
        journeypatterns[row['operator_id']] = row
        journeypatterns[row['operator_id']]['POINTS'] = []
        row['routeref'] = routeRefForPattern[row['operator_id']]
    cur.execute("""
SELECT
j.dataownercode||':'||lineplanningnumber||':'||journeypatterncode as journeypatternref,
cast(timinglinkorder  as integer) as pointorder,
null as privatecode,
lineplanningnumber||':'||journeypatterncode as operator_id,
j.dataownercode||':'||userstopcodebegin as pointref,
j.dataownercode||':'||userstopcodeend as onwardpointref,
j.dataownercode||':'||destcode as destinationdisplayref,
NULL as noticeassignmentRef,
NULL as administrativezoneref,
istimingstop as iswaitpoint,
0 as waittime,
NULL as requeststop,
getout as foralighting,
CASE WHEN (lower(destnamefull) = 'niet instappen') THEN false 
     ELSE getin END as forboarding,
0 as distancefromstartroute,
coalesce(sum(distance) OVER (PARTITION BY j.version,j.dataownercode,lineplanningnumber,journeypatterncode
                                        ORDER BY j.version,j.dataownercode, lineplanningnumber, journeypatterncode, timinglinkorder
                                        ROWS between UNBOUNDED PRECEDING and 1 PRECEDING),0) as fareunitspassed
FROM jopatili as j LEFT JOIN link as l using (version,dataownercode,userstopcodebegin,userstopcodeend)
                   LEFT JOIN dest USING (version,destcode) LEFT JOIN usrstop as u ON (u.version = j.version AND u.userstopcode = 
j.userstopcodebegin)
UNION (
SELECT DISTINCT ON (j.version,j.dataownercode,lineplanningnumber,journeypatterncode)
j.dataownercode||':'||lineplanningnumber||':'||journeypatterncode as journeypatternref,
cast(timinglinkorder+1 as integer) as pointorder,
null as privatecode,
lineplanningnumber||':'||journeypatterncode as operator_id,
j.dataownercode||':'||userstopcodeend as pointref,
NULL as onwardpointref,
j.dataownercode||':'||destcode as destinationdisplayref,
NULL as noticeassignmentRef,
NULL as administrativezoneref,
istimingstop as iswaitpoint,
0 as waittime,
NULL as requeststop,
getout as foralighting,
false as forboarding,
0 as distancefromstartroute,
sum(distance) OVER (PARTITION BY j.version,j.dataownercode,lineplanningnumber,journeypatterncode) as fareunitspassed
FROM jopatili as j LEFT JOIN link as l using (version,dataownercode,userstopcodebegin,userstopcodeend)
                   LEFT JOIN usrstop as u ON (u.version = j.version AND u.userstopcode = j.userstopcodeend)
ORDER BY j.version,j.dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder DESC)
ORDER BY journeypatternref,pointorder
""")
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
        if distance == 0 and int(row['pointorder']) > 3:
            raise Exception('distancefromstartroute going wrong')
        row['distancefromstartroute'] = distance
        journeypatterns[row['journeypatternref']]['POINTS'].append(row)
    cur.close()
    return journeypatterns

def getJourneys(timedemandGroupRefForJourney,conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT DISTINCT ON (version, dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber)
concat_ws(':',dataownercode,lineplanningnumber,journeynumber) as privatecode,
concat_ws(':',version, dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber) as operator_id,
concat_ws(':', dataownercode, organizationalunitcode, schedulecode, scheduletypecode) as availabilityconditionRef,
concat_ws(':',dataownercode,lineplanningnumber,journeypatterncode) as journeypatternref,
NULL as timedemandgroupref,
CASE WHEN (prodformtype = 'belb') THEN '2'
     WHEN (prodformtype = 'buur') THEN '1'
     ELSE '0' end as productCategoryRef,
NULL as noticeassignmentRef,
NULL as departuretime,
NULL as blockref,
cast(journeynumber as integer) as name,
NULL as lowfloor,
NULL as hasLiftOrRamp,
NULL as haswifi,
(dataownercode = 'VTN' and lineplanningnumber = '26') as bicycleAllowed,
prodformtype in ('belb') as onDemand
FROM pujopass LEFT JOIN (SELECT DISTINCT ON (dataownercode,lineplanningnumber,journeypatterncode)
                                             dataownercode,lineplanningnumber,journeypatterncode,prodformtype FROM jopatili
                                             ORDER BY dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder) as pattern 
USING(dataownercode,lineplanningnumber,journeypatterncode)
ORDER BY version, dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber,stoporder ASC
""")
    journeys = {}
    for row in cur.fetchall():
        row.update(timedemandGroupRefForJourney[row['operator_id']])
        journeys[row['operator_id']] = row
    cur.close()
    return journeys

importorder = ['DEST','LINE','CONAREA','CONFINREL','POINT','USRSTAR','USRSTOP','TILI','LINK','POOL','JOPA','JOPATILI','ORUN','ORUNORUN','SPECDAY','PEGR','EXCOPDAY','PEGRVAL','TIVE','TIMDEMGRP','TIMDEMRNT','PUJO','SCHEDVERS','PUJOPASS','OPERDAY']
versionheaders = ['Version_Number','VersionNumber','VERSIONNUMBER']

def table(filename):
    filename = filename.split('.TMI')[0]
    return filename.rstrip('X').rstrip('.csv')

def filelist(zipfile):
    files = {}
    filenames = zipfile.namelist()
    for file in filenames:
        files[table(file.split('/')[-1])] = file
    return files

def encodingof(dataownercode):
    if dataownercode in ['QBUZZ','CXX','EBS']:
        return 'ISO-8859-15'
    else:
        return 'UTF-8'

def metadata(schedule):
    lines = schedule.split('\r\n')
    if lines[0].split('|')[1] in versionheaders:
        firstline = 1
    else:
        firstline = 0
    validfrom = '3000-01-01'
    validthru = '1900-01-01'
    for line in lines[firstline:-1]:
       values = line.split('|')
       dataowner = values[3]
       if values[0] != 'OPERDAY':
           return {'dataownercode' : dataowner}
       if values[7] < validfrom:
           validfrom = values[7]
       if values[7] > validthru:
           validthru = values[7]
    return {'startdate' : validfrom, 'enddate' : validthru, 'dataownercode' : dataowner}

def importzip(conn,zipfile):
    files = filelist(zipfile)
    cur = conn.cursor()
    if 'OPERDAY' in files:
        meta = metadata(zipfile.read(files['OPERDAY']))
    elif 'PUJO' in files:
        meta = metadata(zipfile.read(files['PUJO']))
    else:
        raise Exception('OPERDAY mist')
    header = (zipfile.read(files['DEST']).split('\r\n')[0].split('|')[1] in versionheaders)
    encoding = encodingof(meta['dataownercode'])
    del(meta['dataownercode'])
    for table in importorder:
        if table in files:
            f = zipfile.open(files[table])
            if header:
                cur.copy_expert("COPY %s FROM STDIN WITH DELIMITER AS '|' NULL AS '' CSV HEADER ENCODING '%s'" % (table,encoding),f)
            else:
                cur.copy_expert("COPY %s FROM STDIN WITH DELIMITER AS '|' NULL AS '' CSV ENCODING '%s'" % (table,encoding),f)
    cur.close()
    return meta

def checkUsrstopPoint(conn):
    cur = conn.cursor()
    cur.execute("""
select town,name,userstopcode from usrstop as u left join point as p on (u.version = p.version and u.userstopcode = p.pointcode) where pointcode is 
null;""")
    rows = cur.fetchall()
    if len(rows) == 0:
       return False
    res = ''
    for row in rows:
        res += ', '.join(row)+'\n'
    raise Exception('USRSTOPs without POINT\n'+res)

def load(path,filename):
    zip = zipfile.ZipFile(path+'/'+filename,'r')
    if 'Csv.zip' in zip.namelist():
        zipfile.ZipFile.extract(zip,'Csv.zip','/tmp')
        zip = zipfile.ZipFile('/tmp/Csv.zip','r')
    conn = psycopg2.connect(kv1_database_connect)
    cur =  conn.cursor()
    cur.execute(schema)
    meta = importzip(conn,zip)
    checkUsrstopPoint(conn)
    return (meta,conn)
