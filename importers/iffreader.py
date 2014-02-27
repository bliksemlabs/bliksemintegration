#!/usr/bin/env python2

import sys
from datetime import date, timedelta
from StringIO import StringIO
import psycopg2
import codecs
import zipfile
from settings.const import iff_database_connect

charset = 'cp1252' # yes, this isn't what the documentation suggests

def parse_date(day):
	return date(int(day[4:8]), int(day[2:4]), int(day[0:2]))

def parse_time(time):
	return time[0:2] + ':' + time[2:4] + ':00'

def open_iff(zip,filename, delivery):
	l_content = zip.read(filename+'.dat').decode(charset).split('\r\n')
	if l_content[0] == delivery:
		print "%s.dat matches delivery" % (filename)
	return l_content[1:-1]

def simple_list_writer(conn,filename, arguments, data):
	f = StringIO()
	f.write('\t'.join(arguments) + '\n')
	for y in data:
		f.write('\t'.join([unicode(y[z] or '') for z in arguments]) + '\n')
        f.seek(0)
        cur = conn.cursor()
        if filename == 'country':
            cur.copy_expert("COPY country FROM STDIN USING DELIMITERS '	' CSV HEADER NULL AS '';",f)
        else:
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

def parse_timetables(zip,delivery):
	l_timetables = open_iff(zip,'timetbls', delivery)

	timetables = {}
	current_id = None
	current_record = {}
	s_stationshort = None
	s_index = 0

	for x in l_timetables:
		if x[0] == '#':
			if current_id is not None:
				timetables[current_id] = current_record
			s_index = 0
			current_id = int(x[1:])
			current_record = {'service': [], 'validity': [], 'transport': [], 'attribute': [], 'stop': [], 'platform': []}
		elif x[0] == '%':
			s_companynumber, s_servicenumber, s_variant, s_firststop, s_laststop, s_servicename = x[1:].split(',')
			current_record['service'].append({'company': int(s_companynumber), 'service': int(s_servicenumber), 'variant': s_variant.strip(), 'first': int(s_firststop), 'last': int(s_laststop), 'name': s_servicename.strip()})
		elif x[0] == '-':
			v_footnote, v_firststop, v_laststop = x[1:].split(',')
			current_record['validity'].append({'footnote': v_footnote, 'first': int(v_firststop), 'last': int(v_laststop)})
		elif x[0] == '&':
			t_mode, t_firststop, t_laststop = x[1:].split(',')
			current_record['transport'].append({'mode': t_mode.strip(), 'first': int(t_firststop), 'last': int(t_laststop)})
		elif x[0] == '*':
			t_code, t_firststop, t_laststop, t_unknown = x[1:].split(',')
			current_record['attribute'].append({'code': t_code.strip(), 'first': int(t_firststop), 'last': int(t_laststop), 'unknown': int(t_unknown)})
		elif x[0] == '>':
			s_index += 1
			s_stationshort, s_departuretime = x[1:].split(',')
			s_stationshort = s_stationshort.strip()
			current_record['stop'].append({'station': s_stationshort, 'index': s_index, 'arrivaltime': None, 'departuretime': parse_time(s_departuretime)})
		elif x[0] == '.':
			s_index += 1
			s_stationshort, s_arrivaldeparturetime = x[1:].split(',')
			s_stationshort = s_stationshort.strip()
			both = parse_time(s_arrivaldeparturetime)
			current_record['stop'].append({'station': s_stationshort, 'index': s_index, 'arrivaltime': both, 'departuretime': both})
		elif x[0] == ';':
			s_index += 1
			s_stationshort = x[1:].split(',')
			s_stationshort = s_stationshort.strip()
			current_record['stop'].append({'station': s_stationshort, 'index': s_index, 'arrivaltime': None, 'departuretime': None})
		elif x[0] == '+':
			s_index += 1
			s_stationshort, s_arrivaltime, s_departuretime = x[1:].split(',')
			s_stationshort = s_stationshort.strip()
			current_record['stop'].append({'station': s_stationshort, 'index': s_index, 'arrivaltime': parse_time(s_arrivaltime), 'departuretime': parse_time(s_departuretime)})
		elif x[0] == '?':
			s_arrivalplatform, s_departureplatform, footnote = x[1:].split(',')
			current_record['platform'].append({'index': s_index,'station': s_stationshort,'arrival': s_arrivalplatform.strip(), 'departure': s_departureplatform.strip(), 'footnote': int(footnote)})
			if s_arrivalplatform[0] <> s_departureplatform[0]:
				print current_id, s_stationshort, x
		elif x[0] == '<':
			s_index += 1
			s_stationshort, s_arrivaltime = x[1:].split(',')
			s_stationshort = s_stationshort.strip()
			current_record['stop'].append({'station': s_stationshort, 'index': s_index, 'arrivaltime': parse_time(s_arrivaltime), 'departuretime': None})
	
	if current_id is not None:
		timetables[current_id] = current_record
	
	return timetables

def sql_timetables(conn,data):
	f = {}
	a = {}
	f['service'] = StringIO()
	f['validity'] = StringIO()
	f['transport'] = StringIO()
	f['attribute'] = StringIO()
	f['stop'] = StringIO()
	f['platform'] = StringIO()

	a['service'] = ['company', 'service', 'variant', 'first', 'last', 'name']
	a['validity'] = ['footnote', 'first', 'last']
	a['transport'] = ['mode', 'first', 'last']
	a['attribute'] = ['code', 'first', 'last']
	a['stop'] = ['index', 'station', 'arrivaltime', 'departuretime']
	a['platform'] = ['index','station','arrival', 'departure', 'footnote']

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

def parse_timezones(zip,delivery):
	l_timezones = open_iff(zip,'timezone', delivery)

	timezones = {}
	current_id = None
	current_values = []

	for x in l_timezones:
		if x[0] == '#':
			if current_id is not None:
				timezones[current_id] = current_values
			current_id = int(x[1:])
			current_values = []
		else:
			difference, firstday, lastday = x[1:].split(',')
			if x[0] == '-':
				difference = int(difference) * -1
			elif x[0] == '+':
				difference = int(difference)

			current_values.append({'difference': difference, 'firstday': parse_date(firstday), 'lastday': parse_date(lastday)})
	
	if current_id is not None:
		timezones[current_id] = current_values

	return timezones

def sql_timezones(conn,data):
	simple_dict_list_writer(conn,'timezone', ['tznumber', 'difference', 'firstday', 'lastday'], data)

def parse_transattributequestions(zip,delivery):
	l_aq = open_iff(zip,'trnsaqst', delivery)

	aq = {}
	current_id = None
	current_record = {}

	for x in l_aq:
		if x[0] == '#':
			if current_id is not None:
				aq[current_id] = current_record
			q_code, q_type, q_question = x[1:].split(',')
			current_id = q_code.strip()
			current_record = {'attributes': [], 'inclusive': bool(q_type == '1'), 'question': q_question.strip()}
		elif x[0] == '-':
			current_record['attributes'].append(x[1:].strip())
	
	if current_id is not None:
		aq[current_id] = current_record

	return aq


def sql_transattributequestions(conn,data):
	f = StringIO()
	f.write('\t'.join(['code', 'inclusive', 'question', 'transattr']) + '\n')
	for x, y in data.items():
		for z in y['attributes']:
			f.write('\t'.join([unicode(u or '') for u in [x, y['inclusive'], y['question'], z]]) + '\n')
        f.seek(0)
        cur = conn.cursor()
        cur.copy_expert("COPY trnsaqst FROM STDIN USING DELIMITERS '	' CSV HEADER NULL AS '';",f)
        cur.close()
	f.close()

def parse_footnotes(zip,delivery):
	l_footnotes = open_iff(zip,'footnote', delivery)

	footnotes = {}
	current_id = None

	for x in l_footnotes:
		if x[0] == '#':
			current_id = int(x[1:])
		else:
			footnotes[current_id] = [y == '1' for y in x]

	return footnotes

def sql_footnotes(conn,delivery, data):
	f = StringIO()
	f.write('\t'.join(['footnote', 'servicedate']) + '\n')
	for x, y in data.items():
		for z in range(0, len(y)):
			if y[z] == True:
				f.write('\t'.join([unicode(x), unicode(delivery['firstday'] + timedelta(days=z))]) + '\n')
        f.seek(0)
        cur = conn.cursor()
        cur.copy_expert("COPY footnote FROM STDIN USING DELIMITERS '	' CSV HEADER NULL AS '';",f)
        cur.close()
	f.close()

def parse_changes(zip,delivery):
	l_changes = open_iff(zip,'changes', delivery)

	changes = {}
	current_id = None
	current_records = None

	for x in l_changes:
		if x[0] == '#':
			current_id = x[1:].strip()
                        changes[current_id] = []
		else:
			c_from, c_to, c_change = [y for y in x[1:].split(',')]
			changes[current_id].append({'from': int(c_from), 'to': int(c_to), 'change': c_change})

	return changes

def sql_changes(conn,data):
	simple_dict_list_writer(conn,'changes', ['station', 'from', 'to', 'change'], data)

def parse_countries(zip,delivery):
	l_countries = open_iff(zip,'country', delivery)

	countries = {}
	for country in l_countries:
		c_code, c_inland, c_name = country.split(',')
		countries[c_code.strip()] = {'inland': bool(c_inland), 'name': c_name.strip()}

	return countries


def sql_countries(conn,countries):
	simple_dict_writer(conn,'country', ['code', 'inland', 'name'], countries)

def parse_transattributes(zip,delivery):
	l_transattributes = open_iff(zip,'trnsattr', delivery)

	transattributes = {}
	for attribute in l_transattributes:
		a_code, a_pcode, a_description = attribute.split(',')
		transattributes[a_code.strip()] = {'processingcode': int(a_pcode), 'description': a_description.strip()}

	return transattributes

def sql_transattributes(conn,data):
	simple_dict_writer(conn,'trnsattr', ['code', 'processingcode', 'description'], data)

def parse_companies(zip,delivery):
	l_companies = open_iff(zip,'company', delivery)

	companies = {}
	for company in l_companies:
		c_number, c_code, c_name, c_time = company.split(',')
		companies[int(c_number)] = {'code': c_code.strip(), 'name': c_name.strip(), 'timeturn': parse_time(c_time)}

	return companies

def sql_companies(conn,data):
	simple_dict_writer(conn,'company', ['number', 'code', 'name', 'timeturn'], data)

def parse_connectionmodes(zip,delivery):
	l_cms = open_iff(zip,'connmode', delivery)

	cms = {}
	for cm in l_cms:
		c_code, c_type, c_description = cm.split(',')
		cms[c_code.strip()] = {'type': int(c_type), 'description': c_description.strip()}

	return cms

def sql_connectionmodes(conn,data):
	simple_dict_writer(conn,'connmode', ['code', 'type', 'description'], data)

def parse_continuousconnections(zip,delivery):
	l_ccs = open_iff(zip,'contconn', delivery)

	ccs = []
	for cc in l_ccs:
		c_from, c_to, c_time, c_mode = cc.split(',')
		ccs.append({'from': c_from.strip(), 'to': c_to.strip(), 'time': int(c_time) * 60, 'mode': c_mode.strip()})

	return ccs

def sql_continuousconnections(conn,data):
	simple_list_writer(conn,'contconn', ['from', 'to', 'time', 'mode'], data)


def parse_transmodes(zip,delivery):
	l_transmode = open_iff(zip,'trnsmode', delivery)

	transmodes = {}
	for transmode in l_transmode:
		t_code, t_description = transmode.split(',')
		transmodes[t_code.strip()] = {'description': t_description.strip()}

	return transmodes

def sql_transmodes(conn,data):
	simple_dict_writer(conn,'trnsmode', ['code', 'description'], data)

def parse_stations(zip,delivery):
	l_stations = open_iff(zip,'stations', delivery)

	stations = {}
	for station in l_stations:
		changes, shortname, mintime, _maxtime, country, timezone, attribute, x, y, name = station.split(',')
		stations[shortname.strip()] = {'changes': changes, 'layovertime': int(mintime) * 60, 'country': country.strip(), 'timezone': int(timezone), 'x': int(x) * 10, 'y': int(y) * 10, 'name': name.strip()}

		# Multiply the RD coordinate by 10
		# Multiply the minimum transfer time by 60s
	
	return stations

def sql_stations(conn,data):
	simple_dict_writer(conn,'station', ['shortname', 'changes', 'layovertime', 'country', 'timezone', 'x', 'y', 'name'], data)

def create_schema(conn):
    cur = conn.cursor()
    cur.execute("""
create temporary table country(code varchar(4) primary key, inland boolean not null, name varchar(29) not null);
create temporary table company(company integer primary key, code varchar(9) not null, name varchar(29) not null, timeturn time);
create temporary table delivery(company integer references company, firstday date, lastday date, versionnumber integer, description varchar(29));
create temporary table timezone(tznumber integer primary key, difference integer, firstday date not null, lastday date not null);
create temporary table station(shortname varchar(6) primary key, trainchanges smallint, layovertime integer, country varchar(4) references country, timezone integer references timezone, x integer, y integer, name varchar(29));
create temporary table trnsattr (code varchar(4) primary key, processingcode smallint not null, description varchar(30));
create temporary table trnsaqst(code varchar(3), inclusive boolean, question varchar(29) not null, transattr varchar(4) references trnsattr, primary key (code, transattr));
-- documentation states 'mode' instead of attr
create temporary table trnsmode(code varchar(4) primary key, description varchar(29));
-- create table trnsmqst(code varchar(3), question varchar(29) not null, transmode varchar(4) references trnsmode, primary key(code, transmode));
create temporary table connmode(code varchar(4) primary key, connectiontype smallint not null, description varchar(29));
create temporary table contconn(fromstation varchar(6) references station, tostation varchar(6) references station, connectiontime integer not null, connectionmode varchar(4) references connmode not null, primary key(fromstation, tostation, connectionmode));
create temporary table footnote(footnote integer NOT NULL, servicedate date);
create temporary table timetable_service (serviceid integer not null, companynumber integer references company, servicenumber integer, variant integer, firststop numeric(3,0), laststop numeric(3,0), servicename varchar(29));
create temporary table timetable_validity (serviceid integer not null, footnote integer NOT NULL, firststop numeric(3,0), laststop numeric(3,0));
create temporary table timetable_transport (serviceid integer not null, transmode varchar(4) references trnsmode, firststop numeric(3,0), laststop numeric(3,0));
create temporary table timetable_attribute (serviceid integer not null, code varchar(4) references trnsattr, firststop numeric(3,0), laststop numeric(3,0));
create temporary table timetable_stop (serviceid integer, idx integer, station varchar(6) references station, arrivaltime char(8), departuretime char(8), primary key(serviceid, idx));
create temporary table timetable_platform (serviceid integer, idx integer, station varchar(6) references station, arrival varchar(4), departure varchar(4), footnote integer, primary key(serviceid, idx), foreign key (serviceid, idx) references timetable_stop);
create temporary table changes(station varchar(6) references station, fromservice integer not null, toservice integer not null, possiblechange smallint);
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

CREATE OR REPLACE FUNCTION 
route(servicenumber integer,variant integer) RETURNS integer AS $$
SELECT CASE WHEN (servicenumber = 0 and variant is null) THEN NULL
            WHEN (servicenumber = 0 and variant between 900000 and 999999) THEN NULL
            WHEN (coalesce(nullif(servicenumber,0),variant) between 0 and 99)    THEN (coalesce(nullif(servicenumber,0),variant)/10)*10
            WHEN (coalesce(nullif(servicenumber,0),variant) between 100 and 109) THEN 100
            WHEN (coalesce(nullif(servicenumber,0),variant) between 140 and 149) THEN 140
            WHEN (coalesce(nullif(servicenumber,0),variant) between 240 and 249) THEN 240
            WHEN (coalesce(nullif(servicenumber,0),variant) between 430 and 439) THEN 430
            WHEN (coalesce(nullif(servicenumber,0),variant) between 440 and 449) THEN 440
            WHEN (coalesce(nullif(servicenumber,0),variant) between 100 and 99999) THEN (coalesce(nullif(servicenumber,0),variant)/100)*100
            WHEN (coalesce(nullif(servicenumber,0),variant) between 690000 and 699999) THEN ((coalesce(nullif(servicenumber,0),variant)-690000)/100)*100
            WHEN (coalesce(nullif(servicenumber,0),variant) between 700000 and 709999) THEN ((coalesce(nullif(servicenumber,0),variant)-700000)/100)*100
            WHEN (coalesce(nullif(servicenumber,0),variant) between 710000 and 719999) THEN ((coalesce(nullif(servicenumber,0),variant)-710000)/100)*100
            WHEN (coalesce(nullif(servicenumber,0),variant) between 720000 and 729999) THEN ((coalesce(nullif(servicenumber,0),variant)-720000)/100)*100
            WHEN (coalesce(nullif(servicenumber,0),variant) between 730000 and 739999) THEN ((coalesce(nullif(servicenumber,0),variant)-730000)/100)*100
            WHEN (coalesce(nullif(servicenumber,0),variant) between 740000 and 749999) THEN ((coalesce(nullif(servicenumber,0),variant)-740000)/100)*100
            WHEN (coalesce(nullif(servicenumber,0),variant) between 750000 and 759999) THEN ((coalesce(nullif(servicenumber,0),variant)-750000)/100)*100
            WHEN (coalesce(nullif(servicenumber,0),variant) between 800000 and 809999) THEN ((coalesce(nullif(servicenumber,0),variant)-800000)/100)*100
            WHEN (coalesce(nullif(servicenumber,0),variant) between 860000 and 869999) THEN ((coalesce(nullif(servicenumber,0),variant)-860000)/100)*100
            WHEN (coalesce(nullif(servicenumber,0),variant) between 900000 and 999999) THEN ((coalesce(nullif(servicenumber,0),variant)-900000)/100)*100
            ELSE null END as trainnumber
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION 
line_id(companynumber integer, transmode varchar,servicenumber integer,variant integer, stops varchar[]) RETURNS varchar AS $$
SELECT CASE WHEN (transmode in ('NSB','NSS')) THEN concat_ws(':',companynumber,transmode,least(stops[array_lower(stops,1)],stops[array_upper(stops,1)]),greatest(stops[array_lower(stops,1)],stops[array_upper(stops,1)]))
            WHEN (route(servicenumber,variant) IS NOT NULL) THEN concat_ws(':',companynumber,transmode,route(servicenumber,variant))
            ELSE concat_ws(':',companynumber,transmode,least(stops[array_lower(stops,1)],stops[array_upper(stops,1)]),greatest(stops[array_lower(stops,1)],stops[array_upper(stops,1)]))
            END as line_id
$$ LANGUAGE SQL;""")

def configDB(conn):
    cur = conn.cursor()
    cur.execute("""

CREATE OR REPLACE FUNCTION add32time(departuretime text, seconds integer) RETURNS text AS $$ SELECT lpad(floor((total / 3600))::text, 2,
'0')||':'||lpad(((total % 3600) / 60)::text, 2, '0')||':'||lpad((total % 60)::text, 2, '0') AS arrival_time FROM (SELECT (cast(split_part($1, ':', 1)
as int4) * 60 + cast(split_part($1, ':', 2) as int4)) * 60 + cast(split_part($1, ':', 3) as int4) + coalesce($2, 0) as total) as xtotal $$ LANGUAGE
SQL;

DELETE FROM timetable_validity WHERE footnote not in (
SELECT footnote FROM (select footnote,max(servicedate) as enddate from footnote group by footnote) as x WHERE enddate >= current_date - interval '1 
days');
DELETE FROM timetable_service WHERE serviceid not in (SELECT serviceid FROM timetable_validity);

update timetable_stop set departuretime = add32time(departuretime,3600),arrivaltime = add32time(arrivaltime,3600) where station in (select shortname
from station where country = 'GB');

update trnsmode set description = 'Stoptrein' where description = 'stoptrein';
INSERT INTO company VALUES ('999','EUROSTAR','Eurostar','00:00:00');
UPDATE timetable_service
SET companynumber = 980
WHERE
companynumber = 960 AND
serviceid in (select serviceid from timetable_transport where transmode = 'TGV');
UPDATE timetable_service
SET companynumber = 300
WHERE
companynumber = 960 AND
serviceid in (select serviceid from timetable_transport where transmode = 'THA');
UPDATE timetable_service
SET companynumber = 999
WHERE
companynumber = 960 AND
serviceid in (select serviceid from timetable_transport where transmode = 'ES');

CREATE VIEW timetable as (
SELECT
line_id(companynumber,transmode,servicenumber,variant,array_agg(station) over (PARTITION BY serviceid,transmode,coalesce(servicenumber,variant) ORDER 
BY idx range between unbounded preceding and unbounded following)) as line_id,
companynumber,serviceid,footnote,transmode,servicenumber,variant,servicename,idx,row_number() over(PARTITION BY 
serviceid,transmode,coalesce(servicenumber,variant) ORDER BY idx ASC) as 
stoporder,arrivaltime,CASE WHEN (stoptype = 'LAST') THEN arrivaltime ELSE departuretime END as departuretime,station,arrivalplatform,departureplatform,
md5(string_agg(station||':'||coalesce(departureplatform,'0')||':'||coalesce(arrivalplatform,'0')||':'||attrs::text,'>') over (PARTITION BY 
serviceid,coalesce(servicenumber,variant))) as patterncode,attrs,
((not (ARRAY['NUIT']::varchar[] <@ attrs)) and stoptype <> 'FIRST') as foralighting,
((not (ARRAY['NIIN']::varchar[] <@ attrs)) and stoptype <> 'LAST') as forboarding
FROM (
SELECT companynumber,serviceid,v.footnote,servicenumber,variant,servicename,transmode,idx,departuretime as 
arrivaltime,departuretime,station,departure as arrivalplatform,departure as departureplatform,attrs,'FIRST' as stoptype
FROM timetable_service as s LEFT JOIN timetable_stop USING (serviceid) LEFT JOIN timetable_platform USING (serviceid,station,idx) LEFT JOIN 
timetable_validity as v USING (serviceid)
     LEFT JOIN (select serviceid,transmode,firststop,laststop,generate_series(firststop::int,laststop::int) as idx from timetable_transport) as 
timetable_transport USING (serviceid,idx)
     LEFT JOIN (select serviceid,array_agg(code) as attrs,generate_series(firststop::int,laststop::int) as idx from timetable_attribute GROUP BY 
serviceid,idx) as timetable_attribute USING (serviceid,idx)
WHERE idx = s.firststop AND idx != timetable_transport.laststop
UNION
SELECT 
companynumber,serviceid,v.footnote,servicenumber,variant,servicename,transmode,idx,coalesce(arrivaltime,'00:00:00'),departuretime,station,arrival as 
arrivalplatform,departure  as departureplatform,attrs,
CASE WHEN (timetable_transport.firststop < idx and timetable_transport.laststop  = idx) THEN 'LAST'
     WHEN (timetable_transport.laststop > idx and timetable_transport.firststop = idx) THEN 'FIRST'
     ELSE 'INTERMEDIATE' END as stoptype
FROM timetable_service as s LEFT JOIN timetable_stop USING (serviceid) LEFT JOIN timetable_platform USING (serviceid,station,idx) LEFT JOIN 
timetable_validity as v USING (serviceid)
     LEFT JOIN (select serviceid,transmode,firststop,laststop,generate_series(firststop::int,laststop::int) as idx from timetable_transport) as timetable_transport USING (serviceid,idx)
     LEFT JOIN (select serviceid,array_agg(code) as attrs,generate_series(firststop::int,laststop::int) as idx from timetable_attribute GROUP BY serviceid,idx) as timetable_attribute USING (serviceid,idx)
WHERE idx between s.firststop+1 and s.laststop-1
UNION
SELECT companynumber,serviceid,v.footnote,servicenumber,variant,servicename,transmode,idx,arrivaltime,arrivaltime as departuretime,station,arrival as 
arrivalplatform,arrival as departureplatform,attrs, 'LAST' as stoptype
FROM timetable_service as s LEFT JOIN timetable_stop USING (serviceid) LEFT JOIN timetable_platform USING (serviceid,station,idx) LEFT JOIN timetable_validity as v USING (serviceid)
     LEFT JOIN (select serviceid,transmode,firststop,laststop,generate_series(firststop::int,laststop::int) as idx from timetable_transport) as timetable_transport USING (serviceid,idx)
     LEFT JOIN (select serviceid,array_agg(code) as attrs,laststop as idx from timetable_attribute GROUP BY serviceid,idx) as timetable_attribute USING (serviceid,idx)
WHERE idx = s.laststop AND idx != timetable_transport.firststop
) as x
order by serviceid,servicenumber,footnote,variant,stoporder);

create temporary table passtimes as (
SELECT line_id,companynumber,serviceid,footnote,transmode,servicenumber,variant,servicename,idx,station,platform,
row_number() over(PARTITION BY line_id,serviceid,transmode,coalesce(servicenumber,variant) ORDER BY stoporder,arrivaltime ASC) as stoporder
,arrivaltime,departuretime,md5(string_agg(station||':'||coalesce(platform,'0'),'>') OVER (PARTITION BY 
line_id,serviceid,transmode,coalesce(servicenumber,variant))) as patterncode,attrs,foralighting,forboarding
FROM
(SELECT
line_id,companynumber,serviceid,footnote,transmode,servicenumber,variant,servicename,idx,stoporder,station,departureplatform as platform,
arrivaltime,departuretime,patterncode,CASE WHEN ('{NULL}' = attrs) THEN NULL ELSE attrs END as attrs,foralighting,forboarding
FROM 
timetable WHERE arrivalplatform = departureplatform or arrivalplatform is null
UNION
SELECT
line_id,companynumber,serviceid,footnote,transmode,servicenumber,variant,servicename,idx,stoporder,station,arrivalplatform as platform,
arrivaltime,arrivaltime as departuretime,patterncode,CASE WHEN ('{NULL}' = attrs) THEN NULL ELSE attrs END as attrs,foralighting,false as forboarding
FROM 
timetable WHERE arrivalplatform <> departureplatform
UNION
SELECT
line_id,companynumber,serviceid,footnote,transmode,servicenumber,variant,servicename,idx,stoporder,station,departureplatform as platform,
to32time(toseconds(arrivaltime,0)+15) as arrivaltime,departuretime,patterncode,CASE WHEN ('{NULL}' = attrs) THEN NULL ELSE attrs END as attrs,false as foralighting,forboarding
FROM 
timetable WHERE arrivalplatform <> departureplatform) as x
ORDER BY line_id,servicenumber,variant,serviceid,stoporder
);
""")
    
def sql_delivery(conn,delivery):
	f = StringIO()
	f.write('\t'.join(['companynumber', 'firstday', 'lastday', 'versionnumber', 'description']) + '\n')
	f.write('\t'.join([unicode(delivery[x] or '') for x in ['companynumber', 'firstday', 'lastday', 'versionnumber', 'description']]) + '\n')
        f.seek(0)
        cur = conn.cursor()
        cur.copy_expert("COPY delivery FROM STDIN USING DELIMITERS '	' CSV HEADER NULL AS ''",f)
        cur.close()
	f.close()
	f.close()

def load(path,filename):
    zip = zipfile.ZipFile(path+'/'+filename,'r')
    delivery = zip.read('delivery.dat').decode(charset).split('\r\n')[0]
    number, firstday, lastday, versionnumber, description = delivery[1:].split(',')

    delivery = {'companynumber': int(number), 'firstday': parse_date(firstday), 'lastday': parse_date(lastday), 'versionnumber': int(versionnumber), 'description': description.strip()}

    countries = parse_countries(zip,delivery)
    companies = parse_companies(zip,delivery)
    transmodes = parse_transmodes(zip,delivery)
    transattributes = parse_transattributes(zip,delivery)
    timezones = parse_timezones(zip,delivery)
    transattributequestions = parse_transattributequestions(zip,delivery)
    footnotes = parse_footnotes(zip,delivery)
    continuousconnections = parse_continuousconnections(zip,delivery)
    connectionmodes = parse_connectionmodes(zip,delivery)
    changes = parse_changes(zip,delivery)
    timetables = parse_timetables(zip,delivery)
    stations = parse_stations(zip,delivery)

    print "Received file from %s for period %s to %s (%s)" % (companies[delivery['companynumber']]['name'], delivery['firstday'], delivery['lastday'], delivery['description'])

    conn = psycopg2.connect(iff_database_connect)
    create_schema(conn)
    sql_countries(conn,countries)
    sql_timezones(conn,timezones)
    sql_footnotes(conn,delivery, footnotes)
    sql_stations(conn,stations)
    sql_companies(conn,companies)
    sql_delivery(conn,delivery)
    sql_transattributes(conn,transattributes)
    sql_transattributequestions(conn,transattributequestions)
    sql_transmodes(conn,transmodes)
    sql_connectionmodes(conn,connectionmodes)
    sql_continuousconnections(conn,continuousconnections)
    sql_changes(conn,changes)
    sql_timetables(conn,timetables)
    configDB(conn)
    return (delivery,conn)

if __name__ == '__main__':
    load(sys.argv[1],sys.argv[2])
