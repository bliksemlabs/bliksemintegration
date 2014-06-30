import psycopg2
import psycopg2.extras
import sys

#usage $from_date_iso8601 $to_date_iso8601 $OPERATORS

def writeout(f, cur):
    while True:
        out = cur.fetchone()
        if out is None:
            break

        f.write(','.join(out) + '\r\n')

conn = psycopg2.connect("dbname='ridprod'")
conn.set_client_encoding('WIN1252')
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

from_date = sys.argv[1]
to_date = sys.argv[2]

create temporary table iff_pointref_station as select DISTINCT pointref, coalesce(first_placecode, quaycode) as stationshortname from servicejourney JOIN pointinjourneypattern USING (journeypatternref) JOIN stoppoint ON (pointinjourneypattern.pointref = stoppoint.id) JOIN quays_wouter on (stoppoint.operator_id = dataownercode||':'||userstopcode);

create temporary table iff_transfer_table as select from_stationshortname, to_stationshortname, distance, ST_SetSRID(ST_MakeLine(ST_MakePoint(u.xcoordinate::int4, u.ycoordinate::int4), ST_MakePoint(w.xcoordinate::int4, w.ycoordinate::int4)), 28992) AS line FROM (select x.stationshortname as from_stationshortname, y.stationshortname as to_stationshortname, distance from (select min(pointref) as pointref, stationshortname from iff_pointref_station group by stationshortname) as x, (select min(pointref) as pointref, stationshortname from iff_pointref_station group by stationshortname) as y, generated_transfers where from_stop_id = x.pointref and to_stop_id = y.pointref) as z JOIN iff_stations AS u ON (from_stationshortname = u.stationshortname) JOIN iff_stations AS w ON (to_stationshortname = w.stationshortname);


cur.execute("""
create temporary table iff_servicecalendar as (
SELECT validfrom,validthru,bitcalendar,row_number() OVER () as service_id,unnest(array_agg(availabilityconditionref)) as availabilityconditionref
FROM (
   SELECT availabilityconditionref,
          bitcalendar(array_agg(validdate ORDER BY validdate)) as bitcalendar,
          min(validdate) as validfrom,
          max(validdate) as validthru
          FROM availabilityconditionday as ad JOIN availabilitycondition AS ac ON (availabilityconditionref = ac.id)
                                              JOIN version AS v ON (v.id = versionref)
                                              JOIN datasource AS d ON (d.id = datasourceref)
   WHERE ad.isavailable = true AND validdate between %s and %s and d.operator_id = any(%s)
   GROUP by availabilityconditionref) as x
GROUP BY validfrom,validthru,bitcalendar
ORDER BY service_id);
""",[from_date,to_date,sys.argv[3:]]);

cur.execute("""

""")

cur.execute("""
SELECT
'@'||to_char(0, 'FM000'),
to_char(min(validfrom), 'DDMMYYYY'),
to_char(max(validthru), 'DDMMYYYY'),
to_char(1, 'FM0000'),
'openOV and Friends 2014      '
FROM iff_servicecalendar
""");

delivery = ','.join(cur.fetchone().values()) + '\r\n'
for x in ['delivery.dat', 'changes.dat', 'trnsaqst.dat', 'trnsattr.dat', 'trnsmode.dat']:
    open('output/' + x, 'w').write(delivery)

cur.execute("""
SELECT DISTINCT ON (stationcode)
0 as stationoftrainchanges,
stationcode as stationshortname,
'00' as timenecessarytochangetrains,
'00' as maximumtimetochangetrains,
'NL  ' as countrycode,
'0000' as timezone,
'00' as obsolete,
lpad(AVG(rd_x) OVER (PARTITION BY stationcode)::integer::text,6) as xcoordinate,
lpad(AVG(rd_y) OVER (PARTITION BY stationcode)::integer::text,6) as ycoordinate,
substring(rpad(translate(stationame, ',', ';'),50,' ') for 50) as stationname
FROM (SELECT DISTINCT ON (s_pt.id)
             coalesce(coalesce(first_placecode,'opengeo.'||stoparearef),'opengeo.'||s_pt.id) as stationcode,
             coalesce(stoparea.name,quays_wouter.name) as stationame,
             coalesce(quays_wouter.rd_x,s_pt.rd_x) as rd_x,
             coalesce(quays_wouter.rd_y,s_pt.rd_y) as rd_y
      FROM servicejourney as j
      JOIN iff_servicecalendar USING (availabilityconditionref) -- This just makes sure the trips are in the exported IFF
      JOIN journeypattern as p on (j.journeypatternref = p.id)
      JOIN pointinjourneypattern as p_pt on (p_pt.journeypatternref = p.id)
      JOIN pointintimedemandgroup as t_pt on (j.timedemandgroupref = t_pt.timedemandgroupref AND p_pt.pointorder = t_pt.pointorder)
      JOIN scheduledstoppoint as s_pt ON (pointref = s_pt.id)
      LEFT JOIN stoparea on (stoparea.id = stoparearef)
      LEFT JOIN quays_wouter on (s_pt.operator_id = dataownercode||':'||userstopcode)) AS quays;
""")

f = open('output/stations.dat', 'w')
f.write(delivery)
writeout(f, cur)

'''
We don't have junctions at this moment. Super groups will be added later.

cur.execute("""
select to_char(stoparearef, 'FM000000') as groupshortname, left(translate(stoparea.name, ',', ';'), 29) || repeat(' ', 29 -
length(left(stoparea.name, 29))) as groupname,
stationshortname from (select stoparearef, array_agg(to_char(id, 'FM000000')) as stationshortname from stoppoint group by stoparearef) as x left join
stoparea on (stoparearef = id) where stoparea.name is not null;
""")

f = open('output/group.dat', 'w')
f.write(delivery)
while True:
    out = cur.fetchone()
    if out is None:
        break

    f.write('#' + ','.join([out['groupshortname'], out['groupname']]) + '\r\n')
    for x in out['stationshortname']:
        f.write('-' + x + '\r\n')
'''

open('output/timezone.dat', 'w').write(delivery + "#0000\r\n+00,09122012,14122013\r\n#0001\r\n-01,09122012,14122013\r\n")
open('output/country.dat', 'w').write(delivery + "NL  ,1,Nederland                    \r\n")
open('output/connmode.dat', 'w').write(delivery + "0002,2,Lopen                        \r\n")

cur.execute("""
SELECT to_char(from_stop_id, 'FM000000') as fromstationshortname,
to_char(to_stop_id, 'FM000000') as tostationshortname,
to_char(ceil(distance / 60.0), 'FM00') as connectiontime, '02' as connectionmodecode
FROM transfers;""")

f = open('output/contconn.dat', 'w')
f.write(delivery)
writeout(f, cur)

cur.execute("""
SELECT DISTINCT ON (o.id)
to_char(o.id, 'FM000') as companynumber,
left(o.privatecode, 9) || repeat(' ', 9 - length(left(o.privatecode, 9))) as companycode,
left(o.name, 9) || repeat(' ', 9 - length(left(o.name, 9))) as companyname,
'0400' as time
FROM servicejourney JOIN iff_servicecalendar USING (availabilityconditionref) -- This just makes sure the trips are in the exported IFF
                    JOIN journeypattern as jp ON (jp.id = journeypatternref)
                    JOIN route as r ON (r.id = routeref)
                    JOIN line as l ON (l.id = lineref)
                    JOIN operator as o ON (o.id = operatorref)
""")

f = open('output/company.dat', 'w')
f.write(delivery)
f.write('000,OPENGEO  ,OpenGeo  ,0000\r\n')
writeout(f, cur)

cur.execute("""
SELECT
to_char(row_number() over (), 'FM0000') as transportmodecode,
COALESCE(name, initcap(lower(transportmode))) as description
FROM (SELECT DISTINCT transportmode, pc.name FROM
      servicejourney JOIN iff_servicecalendar USING (availabilityconditionref) -- This just makes sure the trips are in the exported IFF
                     JOIN journeypattern as jp ON (jp.id = journeypatternref)
                     JOIN route as r ON (r.id = routeref)
                     JOIN line as l ON (l.id = lineref)
                     LEFT JOIN productcategory as pc ON (pc.id = productcategoryref)) as x;
""")

f = open('output/trnsmode.dat', 'w')
f.write(delivery)
writeout(f, cur)


cur.execute("""
SELECT
to_char(row_number() over (), 'FM0000') as transportmodecode,
COALESCE(name, initcap(lower(transportmode))) as description
FROM (SELECT DISTINCT transportmode, pc.name FROM
      servicejourney JOIN iff_servicecalendar USING (availabilityconditionref) -- This just makes sure the trips are in the exported IFF
                     JOIN journeypattern as jp ON (jp.id = journeypatternref)
                     JOIN route as r ON (r.id = routeref)
                     JOIN line as l ON (l.id = lineref)
                     LEFT JOIN productcategory as pc ON (pc.id = productcategoryref)) as x;
""")

trnsmode = {}
for x, y in cur.fetchall():
    trnsmode[y.upper()] = x



cur.execute("""
SELECT '#'||to_char(row_number() OVER (), 'FM00000') as footnotenumber,
repeat('0', validfrom - date 'yesterday') || bitcalendar || repeat('0', (select max(validdate) FROM availabilityconditionday) - validthru) as vector
FROM iff_servicecalendar
GROUP BY validfrom, validthru, bitcalendar ORDER BY footnotenumber;
""")

f = open('output/footnote.dat', 'w')
f.write(delivery)
while True:
    out = cur.fetchone()
    if out is None:
        break

    f.write('%s\r\n%s\r\n' % (out[0], out[1]))



# COALESCE(CASE WHEN (blockref like 'IFF:%') THEN substr(blockref, 5) ELSE NULL END, to_char(j.id, 'FM00000000')) AS serviceidentification,
cur.execute("""
SELECT
to_char(j.id, 'FM00000000') AS serviceidentification,
to_char(operatorref, 'FM000') as companynumber,
COALESCE(j.name, '0000') as servicenumber,
'      ' as variant,
left(d.name, 29) || repeat(' ', 29 - length(left(d.name, 29))) as servicename,
to_char(sc.service_id, 'FM00000') as footnotenumber,
COALESCE(cast(pc.name as text), transportmode) as trnsmode,
to32time(departuretime+totaldrivetime) as arrival_time,
to32time(departuretime+totaldrivetime+stopwaittime) as departure_time,
forboarding, foralighting,
stationshortname,
s_pt.platformcode as platformname
FROM servicejourney as j JOIN iff_servicecalendar as sc USING (availabilityconditionref)
                         JOIN journeypattern as p on (j.journeypatternref = p.id)
                         JOIN route as r on (p.routeref = r.id)
                         JOIN line as l on (r.lineref = l.id)
                         JOIN destinationdisplay as d ON (p.destinationdisplayref = d.id)
                         LEFT JOIN productcategory as pc on (j.productcategoryref = pc.id)
                         JOIN pointinjourneypattern as p_pt on (p_pt.journeypatternref = p.id)
                         JOIN pointintimedemandgroup as t_pt on (j.timedemandgroupref = t_pt.timedemandgroupref AND p_pt.pointorder =
t_pt.pointorder)
                         JOIN scheduledstoppoint as s_pt on (p_pt.pointref = s_pt.id)
                         JOIN iff_pointref_station ON (p_pt.pointref = iff_pointref.pointref)
ORDER BY companynumber, serviceidentification, servicenumber, p_pt.pointorder;
""");

f = open('output/timetbls.dat', 'w')
f.write(delivery)

trip = [dict(cur.fetchone())]

def render(f, trip):
    total = '%03d' % (len(trip) - 1)
    try:
        f.write( '#' + trip[0]['serviceidentification'] + '\r\n' ) # Service identification Record

        f.write( '%' + ','.join([trip[0]['companynumber'], trip[0]['servicenumber'], trip[0]['variant'], '001', total, trip[0]['servicename']]) +
'\r\n' ) # Service Record
        f.write( '-' + ','.join([trip[0]['footnotenumber'], '000', '999']) + '\r\n') # Validity
        f.write( '&' + ','.join([trnsmode[trip[0]['trnsmode'].upper()], '001', total]) + '\r\n') # Transport mode Record
        f.write( '>' + ','.join([trip[0]['stationshortname'], trip[0]['departure_time'][0:-3].replace(':', '')]) + '\r\n') # Start Record

        for i in range(1, len(trip) - 2):
            if not trip[i]['forboarding'] and not trip[i]['foralighting']:
                f.write( ';' + trip[i]['stationshortname'] + '\r\n') # Passing Record
            elif trip[i]['arrival_time'][0:-3] == trip[i]['departure_time'][0:-3]:
                f.write( '.' + ','.join([trip[i]['stationshortname'], trip[i]['arrival_time'][0:-3].replace(':', '')]) + '\r\n') # Continuation
            else:
                f.write( '+' + ','.join([trip[i]['stationshortname'], trip[i]['arrival_time'][0:-3].replace(':', ''),
trip[i]['departure_time'][0:-3].replace(':', '')]) + '\r\n') # Interval
        f.write( '<' + ','.join([trip[-2]['stationshortname'], trip[-2]['arrival_time'][0:-3].replace(':', '')]) + '\r\n') # Final Record

    except:
        print trip

while True:
    current = cur.fetchone()
    if current is None:
        break
    trip.append(current)
    if trip[-2]['serviceidentification'] != trip[-1]['serviceidentification']:
        render(f, trip)
        trip = [current]
    elif trip[-2]['servicenumber'] != trip[-1]['servicenumber']:
        print 'Waarschuwing'

render(f, trip)

