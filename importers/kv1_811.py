import psycopg2
import psycopg2.extras
from kv1_810 import *
from copy import deepcopy
from schema.schema_811 import schema

cache = {}

def getTransporttype(conn,lineplanningnumber):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""select transporttype from line where dataownercode||':'||lineplanningnumber = %s""",[lineplanningnumber])
    try:
        return cur.fetchone()['transporttype']
    finally:
        cur.close()

def getFakePool811(conn,lineplanningnumber,userstopcodebegin,userstopcodeend):
    print (lineplanningnumber,userstopcodebegin,userstopcodeend,'fakepool')
    transporttype = getTransporttype(conn,lineplanningnumber)
    key = ':'.join([transporttype,userstopcodebegin,userstopcodeend])
    if key in cache:
        return deepcopy(cache[key])
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
link.userstopcodebegin = point.pointcode AND
link.transporttype = %s AND
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
link.userstopcodeend = point.pointcode AND
link.transporttype = %s AND
link.dataownercode||':'||userstopcodebegin = %s AND
link.dataownercode||':'||userstopcodeend = %s 
) as x
order by pointorder
""",[transporttype,userstopcodebegin,userstopcodeend] * 2)
    try:
        cache[key] = cur.fetchall()
        return deepcopy(cache[key])
    finally:
        cur.close()

def getPool811(conn,lineplanningnumber,userstopcodebegin,userstopcodeend):
    print (lineplanningnumber,userstopcodebegin,userstopcodeend,'pool811')
    transporttype = getTransporttype(conn,lineplanningnumber)
    key = ':'.join([transporttype,userstopcodebegin,userstopcodeend])
    if key in cache:
        return deepcopy(cache[key])
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
pool.pointcode = point.pointcode AND
pool.pointdataownercode = point.dataownercode AND
pool.dataownercode||':'||userstopcodebegin = %s AND
pool.dataownercode||':'||userstopcodeend = %s AND
pool.transporttype = %s AND (pointtype = SP or coalesce(locationx_ew,0) != 0) 
ORDER BY pointorder
""",[userstopcodebegin,userstopcodeend,transporttype])
    try:
        cache[key] = cur.fetchall()
        return deepcopy(cache[key])
    finally:
        cur.close()

def getJourneyPatterns(routeRefForPattern,conn,routes):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    journeypatterns = {}
    cur.execute("""
SELECT
jopa.dataownercode||':'||lineplanningnumber||':'||journeypatterncode as operator_id,
NULL as routeref,
direction as directiontype,
destinationdisplayref
FROM jopa join ( SELECT DISTINCT ON (dataownercode, lineplanningnumber, journeypatterncode)
			dataownercode, lineplanningnumber, journeypatterncode,dataownercode||':'||destcode as destinationdisplayref
			FROM jopatili
			ORDER BY dataownercode, lineplanningnumber, journeypatterncode,timinglinkorder ) as jopatili 
                    USING (dataownercode, lineplanningnumber, journeypatterncode)""")
    for row in cur.fetchall():
        journeypatterns[row['operator_id']] = row
        journeypatterns[row['operator_id']]['POINTS'] = []
        row['routeref'] = routeRefForPattern[row['operator_id']]
    cur.execute("""
(SELECT
j.dataownercode||':'||lineplanningnumber||':'||journeypatterncode as journeypatternref,
cast(timinglinkorder  as integer) as pointorder,
null as privatecode,
lineplanningnumber||':'||journeypatterncode as operator_id,
j.dataownercode||':'||userstopcodebegin as pointref,
j.dataownercode||':'||userstopcodeend as onwardpointref,
j.dataownercode||':'||destcode as destinationdisplayref,
NULL as noticeassignmentRef,
j.dataownercode||':'||confinrelcode as administrativezoneref,
istimingstop as iswaitpoint,
0 as waittime,
NULL as requeststop,
getout as foralighting,
CASE WHEN (lower(destnamefull) = 'niet instappen') THEN false 
     ELSE getin END as forboarding,
0 as distancefromstartroute,
coalesce(sum(distance) OVER (PARTITION BY j.dataownercode,lineplanningnumber,journeypatterncode
                                        ORDER BY j.dataownercode, lineplanningnumber, journeypatterncode, timinglinkorder
                                        ROWS between UNBOUNDED PRECEDING and 1 PRECEDING),0) as fareunitspassed
FROM jopatili as j JOIN line USING (dataownercode,lineplanningnumber)
                   JOIN link as l USING (dataownercode,userstopcodebegin,userstopcodeend,transporttype)
                   JOIN dest USING (dataownercode,destcode) JOIN usrstop as u ON (u.userstopcode = j.userstopcodebegin)
)
UNION (
SELECT DISTINCT ON (j.dataownercode,lineplanningnumber,journeypatterncode)
j.dataownercode||':'||lineplanningnumber||':'||journeypatterncode as journeypatternref,
cast(timinglinkorder+1 as integer) as pointorder,
null as privatecode,
lineplanningnumber||':'||journeypatterncode as operator_id,
j.dataownercode||':'||userstopcodeend as pointref,
NULL as onwardpointref,
j.dataownercode||':'||destcode as destinationdisplayref,
NULL as noticeassignmentRef,
j.dataownercode||':'||confinrelcode as administrativezoneref,
istimingstop as iswaitpoint,
0 as waittime,
NULL as requeststop,
getout as foralighting,
false as forboarding,
0 as distancefromstartroute,
sum(distance) OVER (PARTITION BY j.dataownercode,lineplanningnumber,journeypatterncode) as fareunitspassed
FROM jopatili as j JOIN line using (dataownercode,lineplanningnumber)
                   JOIN link as l using (dataownercode,userstopcodebegin,userstopcodeend,transporttype)
                   JOIN usrstop as u ON (u.userstopcode = j.userstopcodeend)
ORDER BY j.dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder DESC)
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

def load(path,filename,point_from_pool=False):
    zip = zipfile.ZipFile(path+'/'+filename,'r')
    path_parts = zip.namelist()[0].split('/')
    validfrom = None
    if len(path_parts) > 0 and len(path_parts[0].split('-')) == 3:
        validfrom = path_parts[0]
    if 'Csv.zip' in zip.namelist():
        zipfile.ZipFile.extract(zip,'Csv.zip','/tmp')
        zip = zipfile.ZipFile('/tmp/Csv.zip','r')
    conn = psycopg2.connect(kv1_database_connect)
    cur =  conn.cursor()
    cur.execute(schema)
    meta = importzip(conn,zip)
    if validfrom is not None:
        meta['validfrom'] = validfrom
    if point_from_pool:
        fix_points(conn)
    checkUsrstopPoint(conn)
    return (meta,conn)
