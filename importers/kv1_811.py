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
link.version = point.version AND
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
link.version = point.version AND
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
pool.version = point.version AND
pool.pointcode = point.pointcode AND
pool.dataownercode||':'||userstopcodebegin = %s AND
pool.dataownercode||':'||userstopcodeend = %s AND
pool.transporttype = %s
ORDER BY pointorder
""",[userstopcodebegin,userstopcodeend,transporttype])
    try:
        cache[key] = cur.fetchall()
        return deepcopy(cache[key])
    finally:
        cur.close()

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
