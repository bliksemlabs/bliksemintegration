import psycopg2
"""create table transfers (from_stop_id bigint,to_stop_id bigint,distance integer,primary key(from_stop_id,to_stop_id));"""

conn = psycopg2.connect("dbname='rid'")
cur = conn.cursor()
cur.execute("""SELECT id FROM scheduledstoppoint""")
from_stops = cur.fetchall()
cur.close()
counter = 0
for stop in from_stops:
    counter +=1
    print (counter,len(from_stops))
    cur = conn.cursor()
    cur.execute("""
SELECT DISTINCT ON (from_stop_id,to_stop_id)
frompoint.id as from_stop_id,topoint.id as to_stop_id,st_distance(st_transform(frompoint.the_geom,28992),st_transform(topoint.the_geom,28992))::int 
as distance
FROM stoppoint as frompoint LEFT JOIN pointinjourneypattern as jpf ON (frompoint.id = jpf.pointref),
     stoppoint as topoint LEFT JOIN pointinjourneypattern as jpt ON (topoint.id = jpt.pointref)
WHERE 
frompoint.isscheduled = true AND
topoint.isscheduled = true AND
topoint.id <> frompoint.id AND
jpt.forboarding = true AND
jpf.foralighting = true AND
st_distance(st_transform(frompoint.the_geom,28992),st_transform(topoint.the_geom,28992)) < 3000
jpf.journeypatternref <> jpt.journeypatternref AND
frompoint.id = %s
ORDER BY from_stop_id,to_stop_id,frompoint.the_geom <-> topoint.the_geom""",[stop])
    for transfer in cur.fetchall():
        cur.execute("""INSERT INTO transfers VALUES (%s,%s,%s)""",transfer)
conn.commit()
