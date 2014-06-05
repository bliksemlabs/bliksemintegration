#!/usr/bin/env python2

import sys
from datetime import date, timedelta
from StringIO import StringIO
from copy import copy
import psycopg2
import codecs
import zipfile
#from settings.const import beltac_database_connect

#Can be run standalone as python beltacreader.py $path $filename
charset = 'cp1252' # yes, this isn't what the documentation suggests

def parse_time(time):
    return time[0:2] + ':' + time[2:4] + ':00'

def open_beltac(zip,filename, delivery):
    l_content = zip.read(filename).decode(charset).split('\r\n')
    return l_content[:-1]

def simple_list_writer(conn,filename, arguments, data):
    f = StringIO()
    f.write('\t'.join(arguments) + '\n')
    for y in data:
        f.write('\t'.join([unicode(y[z] or '') for z in arguments]) + '\n')
    f.seek(0)
    cur = conn.cursor()
    cur.copy_expert("COPY %s FROM STDIN USING DELIMITERS '	' CSV HEADER" % (filename),f)
    cur.close()
    f.close()

def simple_dict_writer(conn,filename, arguments, data):
    f = StringIO()
    f.write('\t'.join(arguments) + '\n')
    for x, y in data.items():
        f.write('\t'.join([unicode(x)] + [unicode(y[z] or '') for z in arguments[1:]]) + '\n')
    f.seek(0)
    cur = conn.cursor()
    cur.copy_expert("COPY %s FROM STDIN USING DELIMITERS '	' CSV HEADER" % (filename),f)
    cur.close()
    f.close()

def simple_dict_list_writer(conn,filename, arguments, data):
    f = StringIO()
    f.write('\t'.join(arguments) + '\n')
    for x, y in data.items():
        for u in y:
            f.write('\t'.join([unicode(x)] + [unicode(u[z] or '') for z in arguments[1:]]) + '\n')
    f.seek(0)
    cur = conn.cursor()
    cur.copy_expert("COPY %s FROM STDIN USING DELIMITERS '	' CSV HEADER" % (filename),f)
    cur.close()
    f.close()

def add24hours(time):
    hours = str(int(time[0:2])+24)
    return hours+time[2:]

def parse_timetables(zip,filename,validity):
    l_timetables = open_beltac(zip,filename, validity)

    timetables = {}
    current_id = None
    current_record = {}
    s_stationshort = None
    s_index = 0
    last_time = None
    
    for x in l_timetables:
        if x[0] == '#':
            if current_id is not None:
                timetables[current_id] = current_record
            s_index = 0
            current_id = int(x[1:])
            current_record = {'calendar': [], 'stop': [], 'note': []}
        elif x[0] == '%':
            s_range,s_cutoff = [int(y) for y in x[1:].split('|')]
        elif x[0] == '-':
            v_calendarid,v_blockid = x[1:].split('|')
            current_record['calendar'].append({'calendar_id': int(v_calendarid), 'block_id': v_blockid})
        elif x[0] == 'n':
            v_noteid = x[1:].strip()
            if s_index == 0:
                current_record['note'].append({'idx': None, 'note_id': v_noteid})
            else:
                current_record['note'].append({'idx': s_index, 'note_id': v_noteid})
        elif x[0] == '>':
            s_index += 1
            s_stationshort, s_departuretime = x[1:].split('|')
            s_stationshort = s_stationshort.strip()
            s_departuretime = parse_time(s_departuretime)
            current_record['stop'].append({'stop_id': s_stationshort, 'index': s_index, 'arrivaltime': None, 'departuretime': s_departuretime})
            last_time = s_departuretime
        elif x[0] == '.':
            s_index += 1
            s_stationshort, s_arrivaldeparturetime = x[1:].split('|')
            s_stationshort = s_stationshort.strip()
            both = parse_time(s_arrivaldeparturetime)
            if both < last_time:
                both = add24hours(both)
            current_record['stop'].append({'stop_id': s_stationshort, 'index': s_index, 'arrivaltime': both, 'departuretime': both})
            last_time = both
        elif x[0] == '+':
            s_index += 1
            s_stationshort, s_arrivaltime, s_departuretime = x[1:].split('|')
            s_stationshort = s_stationshort.strip()
            s_arrivaltime = parse_time(s_arrivaltime)
            if s_arrivaltime < last_time:
                s_arrivaltime = add24hours(s_arrivaltime)
            s_departuretime = parse_time(s_departuretime)
            if s_departuretime < s_arrivaltime:
                s_departuretime = add24hours(s_departuretime)
            current_record['stop'].append({'stop_id': s_stationshort, 'index': s_index, 'arrivaltime': s_arrivaltime, 'departuretime': s_departuretime})
            last_time = s_departuretime
        elif x[0] == '<':
            s_index += 1
            s_stationshort, s_arrivaltime = x[1:].split('|')
            s_stationshort = s_stationshort.strip()
            s_arrivaltime = parse_time(s_arrivaltime)
            if s_arrivaltime < last_time:
                s_arrivaltime = add24hours(s_arrivaltime)
            current_record['stop'].append({'stop_id': s_stationshort, 'index': s_index, 'arrivaltime': s_arrivaltime, 'departuretime': None})
    
    if current_id is not None:
        timetables[current_id] = current_record
    
    return timetables

def sql_timetables(conn,data):
	f = {}
	a = {}
	f['note'] = StringIO()
	f['stop'] = StringIO()
	f['calendar'] = StringIO()

	a['note'] = ['idx','note_id']
	a['stop'] = ['index', 'stop_id', 'arrivaltime', 'departuretime']
	a['calendar'] = ['calendar_id', 'block_id']
	for x in f.keys():
		f[x].write('\t'.join(['serviceid'] + a[x]) + '\n')

	for x, y in data.items():
		for z in f.keys():
			for u in y[z]:
				f[z].write('\t'.join([unicode(x)] + [unicode(u[w] or '') for w in a[z]]) + '\n')
        cur = conn.cursor()
	for filename,f in f.items():
            f.seek(0)
            cur.copy_expert("COPY timetable_%s FROM STDIN USING DELIMITERS '	' CSV HEADER" % (filename),f)
            f.close()
        cur.close()

def parse_notes(zip,filename,validity):
    l_notes = open_beltac(zip,filename, validity)
    note_id,note_shortname = None,None
    notes = {}
    for line in l_notes:
        if line[0] == '#':
            note_id,note_preferred = line[1:].split('|')
            if len(note_preferred) == 0:
                note_preferred = None
        if line[0] == '.':
            notes[note_id] = {'note_text' : line[1:], 'note_id' : note_id, 'note_code_preferred' : note_preferred}
    return notes

def parse_blocks(zip,filename,validity):
    l_blocks = open_beltac(zip,filename, validity)
    blocks = {}
    for line in l_blocks:
        if line[0] == '#':
            block_id,calendar_id,accessible = line[1:].split('|')
            blocks[block_id]= {'block_id' : block_id,
                           'calendar_id' : calendar_id,
                           'accessible' : int(accessible) == 1}
    return blocks


def parse_version(zip,filename,validity):
    version = copy(validity)
    l_version = open_beltac(zip,filename, validity)
    versions = {}
    v_version,v_release = l_version[0].split('|')
    version['version'] = int(v_version)
    version['release'] = int(v_release)
    versions[version['version']] = version
    return versions

def parse_stops(zip,filename,validity):
    l_stops = open_beltac(zip,filename, validity)
    stops = {}
    for line in l_stops:
        values = line.split('|')
        stop_id,description_nl,description_fr,municipality_nl,municipality_fr,country,streetname_nl = values[:7]
        streetname_fr,aricode,accessibile,lambert72_x,lambert72_y,ispublic,uic = values[7:]
        stops[stop_id] = {'stop_id' : stop_id,
                      'description_nl' : description_nl,
                      'description_fr' : description_fr,
                      'municipality_nl' : municipality_nl,
                      'municipality_fr' : municipality_fr,
                      'country' : country,
                      'streetname_nl' : streetname_nl,
                      'streetname_fr' : streetname_fr,
                      'aricode' : aricode,
                      'accessible' : int(accessibile) == 1,
                      'lambert72_x' : int(lambert72_x),
                      'lambert72_y' : int(lambert72_y),
                      'ispublic' : int(ispublic) == 1,
                      'uic' : uic}
    return stops

def parse_tripcharacteristics(zip,filename,validity):
    l_car = open_beltac(zip,filename, validity)
    v_prefixincluded = int(l_car[0]) == 1
    v_prefix = str(l_car[1]).strip()
    trips = {}
    routes = {}
    for line in l_car[2:]:
        if line[0] == '@':
            v_routeid,v_routename,v_directionname1,v_directionname2,v_routepubliccode,v_routerating,v_routereliability = line[1:].split('|')
            if len(v_routerating) == 0:
                v_routerating = None
            if len(v_routereliability) == 0:
                v_routerating = None
            routes[v_routeid] = {'route_id' : v_routeid,
                           'route_name' : v_routename,
                           'direction_name1' : v_directionname1,
                           'direction_name2' : v_directionname2,
                           'routepubliccode' : v_routepubliccode,
                           'route_rating' : v_routerating,
                           'route_reliability' : v_routereliability}
        else:
            v_tripid,v_routeid,v_trip_route_direction,v_routeservicemode,v_routeservicetype = line[1:].split('|')
            trips[v_tripid] = {'trip_id' : v_tripid,
                           'route_id' : v_routeid,
                           'trip_route_direction' : v_trip_route_direction,
                           'route_service_mode' : v_routeservicemode,
                           'route_service_type' : v_routeservicetype}
    return trips,routes

def parse_calendar(zip,filename,validity):
    l_calendar = open_beltac(zip,filename, validity)
    calendar = {}
    current_id = None

    for x in l_calendar:
        if x[0] == '#':
            current_id = int(x[1:])
            continue
        elif x[0] == '-':
            calendar[current_id] = [y == '1' for y in x[1:]]
    return calendar

def sql_calendar(conn,delivery, data):
    f = StringIO()
    f.write('\t'.join(['service_id', 'servicedate']) + '\n')
    for x, y in data.items():
        for z in range(0, len(y)):
            if y[z] == True:
                f.write('\t'.join([unicode(x), unicode(delivery['firstday'] + timedelta(days=z))]) + '\n')
    f.seek(0)
    cur = conn.cursor()
    cur.copy_expert("COPY calendar FROM STDIN USING DELIMITERS '	' CSV HEADER NULL AS '';",f)
    cur.close()
    f.close()

def create_schema(conn):
    cur = conn.cursor()
    cur.execute("""
drop table if exists version;
create table version(firstday date, lastdate date);
drop table if exists blocks;
create table blocks(block_id varchar(10), calendar_id integer, accessible boolean);
drop table if exists calendar;
create table calendar(calendar_id integer, servicedate date);
drop table if exists notes;
create table notes (note_id varchar(8), note_code_preferred boolean, note_text varchar(1000));
drop table if exists routes;
create table routes (route_id varchar(8), route_name varchar(50), direction_name1 varchar(60), direction_name2 varchar(60), routepubliccode varchar(5), route_rating integer, route_reliability integer);
drop table if exists trips;
create table trips (trip_id integer, route_id varchar(8), trip_route_direction integer, route_service_mode integer, route_service_type integer);
drop table if exists stops;
create table stops ( stop_id varchar(10), description_nl varchar(50), description_fr varchar(50),municipality_nl varchar(50), municipality_fr varchar(50), country varchar(2), streetname_nl varchar(50),
                     streetname_fr varchar(50),aricode varchar(4), accessible boolean, lambert72_x integer,lambert72_y integer,ispublic boolean,uic varchar(9));
drop table if exists timetable_stop;
create table timetable_stop (trip_id integer, idx integer, stop_id varchar(10), arrivaltime char(8), departuretime char(8), primary key(trip_id, idx));
drop table if exists timetable_note;
create table timetable_note (trip_id integer, idx integer, note_id varchar(8));
drop table if exists timetable_calendar;
create table timetable_calendar (trip_id integer, calendar_id integer NOT NULL, block_id varchar(8));
drop table if exists version;
create table version (id integer, release integer NOT NULL, firstday date, lastday date);

CREATE OR REPLACE FUNCTION 
toseconds(time24 text, shift24 integer) RETURNS integer AS $$
SELECT total AS time
FROM
(SELECT
  (cast(split_part($1, ':', 1) as int4) * 3600)      -- hours
+ (cast(split_part($1, ':', 2) as int4) * 60)        -- minutes
+ CASE WHEN $1 similar to '%:%:%' THEN (cast(split_part($1, ':', 3) as int4)) ELSE 0 END -- seconds when applicable
+ (shift24 * 86400) as total --Add 24 hours (in seconds) when shift occured
) as xtotal
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION 
to32time(secondssincemidnight integer) RETURNS text AS $$
SELECT lpad(floor((secondssincemidnight / 3600))::text, 2, '0')||':'||lpad(((secondssincemidnight % 3600) / 60)::text, 2, 
'0')||':'||lpad((secondssincemidnight % 60)::text, 2, '0') AS time
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION add32time(departuretime text, seconds integer) RETURNS text AS $$ SELECT lpad(floor((total / 3600))::text, 2,
'0')||':'||lpad(((total % 3600) / 60)::text, 2, '0')||':'||lpad((total % 60)::text, 2, '0') AS arrival_time FROM (SELECT (cast(split_part($1, ':', 1)
as int4) * 60 + cast(split_part($1, ':', 2) as int4)) * 60 + cast(split_part($1, ':', 3) as int4) + coalesce($2, 0) as total) as xtotal $$ LANGUAGE
SQL;
""")
    
def parse_day(day):
    day = day.split('|')
    return date(int(day[2]), int(day[1]), int(day[0]))

def filedict(zip):
    dict = {}
    for name in zip.namelist():
        dict[name.split('.')[-1]] = name
    return dict

def sql_trips(conn,data):
    simple_dict_writer(conn,'trips', ['trip_id','route_id','trip_route_direction','route_service_mode','route_service_type'], data)

def sql_routes(conn,data):
    simple_dict_writer(conn,'routes',['route_id','route_name','direction_name1','direction_name2','routepubliccode','route_rating','route_reliability'], data)

def sql_notes(conn,data):
    simple_dict_writer(conn,'notes', ['note_id','note_code_preferred','note_text'], data)

def sql_stops(conn,data):
    simple_dict_writer(conn,'stops', ['stop_id','description_nl','description_fr','municipality_nl','municipality_fr','country','streetname_nl','streetname_fr','aricode','accessible','lambert72_x','lambert72_y','ispublic','uic'], data)

def sql_blocks(conn,data):
    simple_dict_writer(conn,'blocks', ['block_id','calendar_id','accessible'], data)

def sql_version(conn,data):
    simple_dict_writer(conn,'version', ['id','release','firstday','lastday'], data)

def set_journeypatterns(conn):
    cur = conn.cursor()
    cur.execute("""
create index on stops (stop_id);
drop table if exists journeypattern;
CREATE TABLE journeypattern as (
SELECT route_id,
trip_id as trip_id,
route_id||':'||rank() OVER (PARTITION BY route_id ORDER BY route_id,pattern) as journeypatterncode,stops.stop_id as last_stopid,stops.description_nl as last_stopname
FROM
(SELECT trip_id,route_id,ARRAY_AGG(stop_id ORDER BY idx) as pattern
FROM timetable_stop JOIN trips using (trip_id) 
GROUP BY route_id,trip_id) as x LEFT JOIN stops ON (stops.stop_id = pattern[array_length(pattern,1)]) 
);""")
    cur.close()

def attempt_stopareas(conn):
    cur = conn.cursor()
    cur.execute("""
drop table if exists stopareas;
create table stopareas as (
SELECT DISTINCT ON (stoparea_id)
substring(stop_id for char_length(stop_id)-1) as stoparea_id,
description_nl as name,
municipality_nl as town,
avg(lambert72_x) OVER (PARTITION BY substring(stop_id for char_length(stop_id)-1))::integer as lambert72_x,
avg(lambert72_y) OVER (PARTITION BY substring(stop_id for char_length(stop_id)-1))::integer as lambert72_y
FROM stops
);

DELETE FROM stopareas WHERE stoparea_id in (
SELECT stoparea_id
FROM (SELECT *,substring(stop_id for char_length(stop_id)-1) as stoparea_id,
               st_setsrid(st_makepoint(lambert72_x,lambert72_y),31370) as geom FROM stops) as a1
         JOIN (SELECT *,substring(stop_id for char_length(stop_id)-1) as stoparea_id,
               st_setsrid(st_makepoint(lambert72_x,lambert72_y),31370) as geom FROM stops) as a2 USING (stoparea_id)
WHERE a1.stop_id != a2.stop_id AND st_distance(a1.geom,a2.geom) > 800 OR a1.description_fr != a2.description_fr
GROUP BY stoparea_id);""")
    

def load(zfiledata):
    zip = zipfile.ZipFile(zfiledata)
    files = filedict(zip)
    version = zip.read(files['VAL']).decode(charset).split('\r\n')[:-1]
    valid_from = parse_day(version[0])
    
    validity = {'firstday' : parse_day(version[0]), 'lastday' : parse_day(version[1])}
    version = parse_version(zip,files['VER'],validity)
    calendar = parse_calendar(zip,files['OPR'],validity)
    timetables = parse_timetables(zip,files['HRA'],validity)
    trips,routes = parse_tripcharacteristics(zip,files['CAR'],validity)
    notes = parse_notes(zip,files['NTE'],validity)
    stops = parse_stops(zip,files['STP'],validity)
    blocks = parse_blocks(zip,files['BLK'],validity)
    conn = psycopg2.connect("dbname='beltactmp'")
    create_schema(conn)

    sql_version(conn,version)
    sql_calendar(conn,validity, calendar)
    sql_timetables(conn, timetables)
    sql_trips(conn,trips)
    sql_routes(conn,routes)
    sql_notes(conn,notes)
    sql_stops(conn,stops)
    sql_blocks(conn,blocks)
    attempt_stopareas(conn)
    set_journeypatterns(conn)
    return (validity,conn)

if __name__ == '__main__':
    load(sys.argv[1],sys.argv[2])
