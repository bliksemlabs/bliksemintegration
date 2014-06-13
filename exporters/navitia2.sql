drop table if exists navitia.parameters;
create table navitia.parameters as (
SELECT
(date 'yesterday') as beginning_date,
(select max(todate) from activeavailabilitycondition WHERE operator_id like 'IFF:%') as end_date
); 

COPY (SELECT * FROM navitia.parameters) to '/tmp/navitia.parameters';

drop table if exists servicejourney_compressed;
create 
--temporary 
table servicejourney_compressed as (
SELECT unnest(array_agg(id)) as id,row_number() OVER () as compressed_id
FROM (
SELECT
servicejourney.id,
departuretime,
servicejourney.privatecode,
journey_dest.name as journey_headsign,
block_id,
array_agg(pjp.pointorder ORDER BY pointorder) as pointorders,
array_agg(pjp.pointref ORDER BY pointorder) as stoppoints,
array_agg(totaldrivetime ORDER BY pointorder) as drivetime,
array_agg(stopwaittime ORDER BY pointorder) as dwelltimes,
array_agg(forboarding ORDER BY pointorder) as forboarding,
array_agg(foralighting ORDER BY pointorder) as foralighting,
array_agg(stop_dest.name ORDER BY pointorder) as stop_headsigns,
array_agg(DISTINCT t.operator_id ORDER BY t.operator_id) journeytransfers
FROM (SELECT id,departuretime,journeypatternref,timedemandgroupref,
             CASE WHEN (split_part(blockref,':',1) IN ('IFF','TEC')) THEN blockref ELSE NULL END as block_id,
             CASE WHEN (split_part(privatecode,':',1) in ('AVV','TEC')) THEN NULL ELSE privatecode END as privatecode
             FROM servicejourney) as servicejourney
                   JOIN pointinjourneypattern as pjp USING (journeypatternref)
                    JOIN pointintimedemandgroup as tp USING (timedemandgroupref,pointorder)
                    JOIN scheduledstoppoint as sp ON (sp.id = pjp.pointref)
                    JOIN journeypattern as jp ON (jp.id = journeypatternref)
                    JOIN destinationdisplay as journey_dest ON (journey_dest.id = jp.destinationdisplayref)
                    LEFT JOIN destinationdisplay as stop_dest ON (stop_dest.id = pjp.destinationdisplayref)
                    LEFT JOIN journeytransfers as t ON (servicejourney.id = journeyref)
GROUP BY servicejourney.id,servicejourney.departuretime,servicejourney.privatecode,journey_dest.name,block_id) as x
GROUP BY 
privatecode,departuretime,pointorders,stoppoints,drivetime,dwelltimes,forboarding,foralighting,block_id,stop_headsigns,journey_headsign,journeytransfers);


CREATE TEMPORARY TABLE compressed_calendar AS (
SELECT compressed_id,validdate
                              FROM servicejourney_compressed JOIN servicejourney AS sj USING (id)
                                                             JOIN serviceday as sd USING (availabilityconditionref)
                                                             JOIN availabilitycondition as ac ON (availabilityconditionref = ac.id)
                                                             JOIN version as v ON (v.id = versionref)
                                                             JOIN datasource as d ON (d.id = datasourceref)
WHERE d.operator_id not in ('TEC')
);

create index on compressed_calendar(compressed_id,validdate);

create temporary table servicecalendar as (
SELECT unnest(array_agg(compressed_id)) as compressed_id,lpad(reverse(validitypattern_section),366,'0') as validitypattern,row_number() OVER ()-1 as 
service_id
FROM (
	SELECT compressed_id,string_agg('',(validdate is not null )::int4::text ORDER BY day) as validitypattern_section
	FROM (
	      SELECT p.compressed_id,validdate,day
	      FROM (SELECT DISTINCT ON (compressed_id,day) compressed_id,day
                    FROM servicejourney_compressed,(SELECT generate_series(beginning_date-interval '1 day',end_date,interval '1 day')::date as day
                                                    FROM navitia.parameters) as p) as p 
              LEFT JOIN compressed_calendar as c ON (c.compressed_id = p.compressed_id AND c.validdate = p.day)) as x
        GROUP BY compressed_id
     ) as x
WHERE validitypattern_section like '%1%'
GROUP BY validitypattern
);

DELETE FROM servicejourney_compressed WHERE compressed_id not in (SELECT DISTINCT compressed_id FROM servicecalendar);

drop table if exists navitia.validity_pattern;
CREATE TABLE navitia.validity_pattern as (
SELECT DISTINCT ON (service_id)
service_id as id,
validitypattern as days
FROM servicecalendar
);
COPY (SELECT * FROM navitia.validity_pattern) to '/tmp/navitia.validity_pattern';

drop table if exists navitia_timetable;
create 
--temporary 
table navitia_timetable as (
SELECT
j.id,
j.departuretime+tpt.totaldrivetime as arrival_time,
j.departuretime+tpt.totaldrivetime+tpt.stopwaittime as departure_time,
jpt.pointref as stop_id,
l.id as lineref,
forboarding,
requeststop,
foralighting,
iswaitpoint as timepoint,
d.name as trip_headsign,
ds.name as stop_headsign,
productcategoryref,
ondemand as ondemand,
pointorder,
compressed_id,
iswaitpoint,
row_number() OVER (PARTITION BY j.id ORDER BY pointorder)-1 as stopidx,
blockref
FROM (SELECT DISTINCT ON (compressed_id) compressed_id,id FROM servicejourney_compressed ORDER BY compressed_id,id) as jc
                  JOIN servicejourney as j USING (id)
                  JOIN servicecalendar as c USING (compressed_id)
                  JOIN pointinjourneypattern as jpt USING (journeypatternref)
                  JOIN scheduledstoppoint as sp ON (sp.id = pointref)
                  JOIN pointintimedemandgroup as tpt USING (timedemandgroupref,pointorder)
                  JOIN journeypattern as jp ON (journeypatternref = jp.id)
                  JOIN route as r ON (routeref = r.id)
                  JOIN line as l ON (lineref = l.id)
                  JOIN destinationdisplay as d ON (jp.destinationdisplayref = d.id)
                  LEFT JOIN destinationdisplay as ds ON (jpt.destinationdisplayref = ds.id)
                  LEFT JOIN productcategory as p ON (productcategoryref = p.id)
WHERE
tpt.totaldrivetime is not null
ORDER BY j.id,pointorder);

--Clear empty blocks
UPDATE navitia_timetable nt
SET blockref = null
FROM ( SELECT blockref
       FROM navitia_timetable
       WHERE blockref IS NOT NULL
       GROUP BY blockref HAVING COUNT(DISTINCT compressed_id) <= 1) as empty_blocks
WHERE empty_blocks.blockref = nt.blockref;

--incoherent blocks
UPDATE navitia_timetable nt
SET blockref = null
FROM ( SELECT DISTINCT x.blockref FROM (
         SELECT *,row_number() OVER (PARTITION BY blockref ORDER BY start_time) as block_idx
             FROM (SELECT id,blockref,min(departure_time) as start_time,max(arrival_time) as end_time
               FROM navitia_timetable
               WHERE blockref is not null
               GROUP BY blockref,id) as s1) as x JOIN (
         SELECT *,row_number() OVER (PARTITION BY blockref ORDER BY start_time) as block_idx
             FROM (SELECT id,blockref,min(departure_time) as start_time,max(arrival_time) as end_time
               FROM navitia_timetable
               WHERE blockref is not null
               GROUP BY blockref,id) as s1) as y ON (x.blockref = y.blockref AND x.block_idx < y.block_idx)
WHERE x.end_time > y.start_time) as incoherent_blocks
WHERE incoherent_blocks.blockref = nt.blockref;

--Remove GVB blocks 
UPDATE navitia_timetable nt
SET blockref = null
FROM (SELECT DISTINCT id FROM journey WHERE privatecode like 'GVB:%') as j
WHERE nt.id = j.id;

drop table if exists navitia_blocktransfers;
create 
--temporary
table navitia_blocktransfers as (
SELECT from_id,from_compressed_id,to_id,to_compressed_id,bt.blockref
FROM ( SELECT 
       x.blockref,
       x.id as from_id,
       x.compressed_id as from_compressed_id,
       y.id as to_id,
       y.compressed_id as to_compressed_id,
       x.end_time as arrival_time,
       y.start_time as departure_time
       FROM (
         SELECT *,row_number() OVER (PARTITION BY blockref ORDER BY start_time) as block_idx
             FROM (SELECT id,blockref,compressed_id,min(departure_time) as start_time,max(arrival_time) as end_time
               FROM navitia_timetable
               WHERE blockref is not null
               GROUP BY blockref,compressed_id,id) as s1) as x JOIN (
         SELECT *,row_number() OVER (PARTITION BY blockref ORDER BY start_time) as block_idx
             FROM (SELECT id,blockref,compressed_id,min(departure_time) as start_time,max(arrival_time) as end_time
               FROM navitia_timetable
               WHERE blockref is not null
               GROUP BY blockref,compressed_id,id) as s1) as y ON (x.blockref = y.blockref AND x.block_idx = y.block_idx-1)) as bt
     JOIN journey as fj ON (from_id = fj.id)
     JOIN journeypattern as fjp ON (fj.journeypatternref = fjp.id)
     JOIN route as fjr ON (fjp.routeref = fjr.id)
     JOIN line as fjl ON (fjr.lineref = fjl.id)
     JOIN journey as tj ON (to_id = tj.id)
     JOIN journeypattern as tjp ON (tj.journeypatternref = tjp.id)
     JOIN route as tjr ON (tjp.routeref = tjr.id)
     JOIN line as tjl ON (tjr.lineref = tjl.id)
WHERE
bt.blockref like 'IFF:%' OR 
((departure_time - arrival_time) between 0 and 10*60));

--company
drop table if exists navitia.company;
create table navitia.company as (
SELECT
row_number() OVER (ORDER BY id)-1 as id,
NULL::text as comment,
name,
concat_ws(':','company',operator_id) as uri,
NULL::text as address_name,
NULL::text as address_number,
NULL::text as address_type_name,
phone as phone_number,
NULL::text as mail,
url as website,
NULL::text as fax,
id as rid_id
FROM operator
WHERE id in (SELECT DISTINCT operatorref FROM line)
ORDER BY operator_id ASC,id ASC);

COPY (SELECT
id,
comment,
name,
uri,
address_name,
address_number,
address_type_name,
phone_number,
mail,
website,
fax
FROM navitia.company) to '/tmp/navitia.company';

--network
drop table if exists navitia.network;
create table navitia.network as (
SELECT
row_number() OVER (ORDER BY id)-1 as id,
NULL::text as comment,
name,
concat_ws(':','network',operator_id) as uri,
operator_id as external_code,
2147483647 as sort,
url as website,
id as rid_id
FROM operator
WHERE id in (SELECT DISTINCT operatorref FROM line)
ORDER BY operator_id ASC,id ASC);

COPY (SELECT
id,
comment,
name,
uri,
external_code,
sort,
website
FROM navitia.network) to '/tmp/navitia.network';

--commercialmode
drop table if exists navitia.commercial_mode;
create table navitia.commercial_mode as (
SELECT
row_number() OVER (ORDER BY prio,id)-1 as id,
uri,
name,
rid_id
FROM (
SELECT 
id,
concat_ws(':','commercial_mode',CASE WHEN (operator_id LIKE 'IFF:%') THEN privatecode ELSE id::text END) as uri,
name,
id::text as rid_id,
2 as prio
FROM productcategory
WHERE id in (SELECT DISTINCT productcategoryref from servicejourney) AND name is not null
UNION
SELECT 
row_number() OVER (ORDER BY transportmode) as id,
concat_ws(':','commercial_mode',transportmode) as uri,
name,
transportmode as rid_id,
1 as prio
FROM transportmode) as x
);

COPY (SELECT
id,
uri,
name
FROM navitia.commercial_mode) to '/tmp/navitia.commercial_mode';

DROP TABLE IF EXISTS navitia.physical_mode;
CREATE TABLE navitia.physical_mode as(
SELECT * FROM(
SELECT
gtfs_route_type as id,
concat_ws(':','physical_mode',transportmode) as uri,
name
FROM transportmode
UNION
SELECT
gtfs_route_type+(SELECT COUNT(*) FROM transportmode) as id,
concat_ws(':','physical_mode','TRAIN'||transportmode) as uri,
'Treinvervangende '||name
FROM transportmode
WHERE transportmode != 'TRAIN') as x
ORDER by id desc
);

COPY (SELECT * FROM navitia.physical_mode) to '/tmp/navitia.physical_mode';

--In RID/KV1 commercial_mode / productcategory is set per Journey, in Kraken on linelevel 
drop table if exists navitia_lines;
create temporary table navitia_lines as (
SELECT 
unnest(journey_ids) as journey_id,
commercial_mode_id,
lineref as orig_lineref,
concat_ws(':',lineref,nullif(rank() OVER (PARTITION BY lineref ORDER BY commercial_mode_id)-1,0)) as lineref
FROM (
SELECT
array_agg(j.id) as journey_ids,
coalesce(cc.id,fallback_cc.id) as commercial_mode_id,
lineref
FROM (SELECT DISTINCT ON (id) * FROM navitia_timetable) as j 
                            LEFT JOIN navitia.commercial_mode as cc ON (productcategoryref::text = cc.rid_id)
                            JOIN line as l ON (l.id = lineref)
                            JOIN navitia.commercial_mode as fallback_cc ON (transportmode = fallback_cc.rid_id)
GROUP BY lineref,commercial_mode_id
) as x
);

drop table if exists navitia_patterns;
CREATE temporary TABLE navitia_patterns AS (
SELECT unnest(array_agg(id)) as journey_id,count(*) OVER (ORDER BY lineref,trip_headsign,stop_ids)-1 as journeypattern_id,trip_headsign,lineref
FROM 
(SELECT
id,
array_agg(stop_id ORDER BY pointorder) as stop_ids,
(array_agg(trip_headsign))[1] as trip_headsign,
nl.lineref
FROM navitia_timetable as nt JOIN navitia_lines as nl ON (nt.id = journey_id)
GROUP BY nl.lineref,id) as x
GROUP BY lineref,trip_headsign,stop_ids
);

DROP TABLE IF EXISTS navitia.line;
create table navitia.line as (
SELECT
row_number() OVER (ORDER BY l.id)-1 as id,
nn.id as network_id,
commercial_mode_id,
NULL::TEXT as comment,
concat_ws(':','line',lineref) as uri,
l.operator_id as external_code,
coalesce(l.name,l.publiccode) as name,
l.publiccode as code,
color_shield as color,
2147483647 as sort,
lineref as rid_id
FROM (SELECT DISTINCT ON (lineref) lineref,orig_lineref,commercial_mode_id FROM navitia_lines) as nl 
      JOIN line as l ON (l.id = orig_lineref)
      JOIN navitia.network as nn ON (nn.rid_id = l.operatorref));

COPY (
SELECT
id,
network_id,
commercial_mode_id,
comment,
uri,
external_code,
name,
code,
color,
sort
FROM navitia.line) to '/tmp/navitia.line';

DROP TABLE IF EXISTS navitia.route;
create table navitia.route as (
SELECT DISTINCT ON (journeypattern_id)
journeypattern_id as id,
navitial.id as line_id,
NULL::TEXT as comment,
trip_headsign as name,
concat_ws(':','route',journeypattern_id) as uri,
navitial.id::TEXT as external_code
FROM navitia_patterns JOIN navitia_lines as nl USING (journey_id)
                      JOIN line as l ON (l.id = orig_lineref) 
                      JOIN navitia.line as navitial ON (nl.lineref = rid_id) 
                      JOIN transportmode as t USING (transportmode));

COPY (SELECT * FROM navitia.route) to '/tmp/navitia.route';

DROP TABLE IF EXISTS navitia.journey_pattern;
create table navitia.journey_pattern as (
SELECT DISTINCT ON (journeypattern_id)
journeypattern_id as id,
journeypattern_id as route_id,
CASE WHEN (o.operator_id LIKE 'IFF:%' AND transportmode != 'TRAIN') THEN gtfs_route_type+(SELECT COUNT(*) FROM transportmode)
     ELSE gtfs_route_type END as physical_mode_id,
NULL::TEXT as comment,
concat_ws(':','journey_pattern',journeypattern_id) as uri,
trip_headsign as name,
false as is_frequence
FROM navitia_patterns JOIN navitia_lines as nl USING (journey_id)
                      JOIN line as l ON (l.id = orig_lineref) 
                      JOIN navitia.line as navitial ON (nl.lineref = rid_id)
                      JOIN operator as o ON (o.id = operatorref) 
                      JOIN transportmode as t USING (transportmode)
);

COPY (SELECT * FROM navitia.journey_pattern) to '/tmp/navitia.journey_pattern';

DROP TABLE IF EXISTS navitia.properties;
CREATE TABLE IF NOT EXISTS navitia.properties (
    id BIGINT PRIMARY KEY,
-- Accès UFR
    wheelchair_boarding BOOLEAN NOT NULL,
-- Abris couvert
    sheltered BOOLEAN NOT NULL,
-- Ascenseur
    elevator BOOLEAN NOT NULL,
-- Escalier mécanique
    escalator BOOLEAN NOT NULL,
-- Embarquement vélo
    bike_accepted BOOLEAN NOT NULL,
-- Parc vélo
    bike_depot BOOLEAN NOT NULL,
-- Annonce visuelle
    visual_announcement BOOLEAN NOT NULL,
-- Annonce sonore
    audible_announcement BOOLEAN NOT NULL,
-- Accompagnement à l'arrêt
    appropriate_escort BOOLEAN NOT NULL,
-- Information claire à l'arrêt
    appropriate_signage BOOLEAN NOT NULL
);

INSERT INTO navitia.properties VALUES (0,false,false,false,false,false,false,false,false,false,false);
INSERT INTO navitia.properties VALUES (1,true,false,false,false,false,false,false,false,false,false);

COPY (SELECT * FROM navitia.properties) to '/tmp/navitia.properties';

DROP TABLE IF EXISTS navitia.stop_area;
CREATE TABLE navitia.stop_area AS (
SELECT
row_number() OVER (ORDER BY uri)-1 as id,
properties_id,
uri,
external_code,
name,
coord,
NULL::text as comment,
visible,
rid_stoparea_id,
rid_stoppoint_id
FROM 
((SELECT DISTINCT ON (sa.id)
sa.id,
0 as properties_id,
concat_ws(':','stop_area',CASE WHEN (sa.operator_id like 'IFF:%' AND sa.operator_id not in (select privatecode from stoparea group by privatecode 
having count(*) > 1)) 
                          THEN sa.publiccode ELSE sa.id::text END) as uri,
coalesce(sa.publiccode,sa.operator_id) as external_code,
sa.name,
st_setsrid(st_makepoint(sa.longitude,sa.latitude),4326) as coord,
NULL::text as comment,
true as visible,
sa.id as rid_stoparea_id,
NULL as rid_stoppoint_id
FROM 
(SELECT DISTINCT stop_id FROM navitia_timetable) as  t JOIN stoppoint as sp ON (stop_id = sp.id)
                                                       JOIN stoparea as sa ON (stoparearef = sa.id)
)UNION(
SELECT DISTINCT ON (sp.id)
sp.id,
case when (restrictedmobilitysuitable) THEN 1 ELSE 0 END as properties_id,
concat_ws(':','stop_area','P'||sp.id) as uri,
coalesce(sp.publiccode,sp.operator_id) as external_code,
sp.name,
st_setsrid(st_makepoint(sp.longitude,sp.latitude),4326) as coord,
NULL::text as comment,
true as visible,
NULL as rid_stoparea_id,
sp.id as rid_stoppoint_id
FROM 
(SELECT DISTINCT stop_id FROM navitia_timetable) as  t JOIN stoppoint as sp ON (stop_id = sp.id)
WHERE stoparearef is null)) as x);
create index on navitia.stop_area(rid_stoparea_id);
create index on navitia.stop_area(rid_stoppoint_id);

COPY (SELECT
id,
properties_id,
uri,
external_code,
name,
coord,
comment,
visible
FROM navitia.stop_area) to '/tmp/navitia.stop_area';

DROP TABLE IF EXISTS navitia.stop_point;
CREATE TABLE navitia.stop_point AS (
SELECT
row_number() OVER (ORDER BY sp.id)-1 as id,
case when (restrictedmobilitysuitable) THEN 1 ELSE 0 END as properties_id,
concat_ws(':','stop_point',sp.id) as uri,
coalesce(publiccode,'') as external_code,
st_setsrid(st_makepoint(sp.longitude,sp.latitude),4326) as coord,
NULL::integer as fare_zone,
sp.name,
NULL::text as comment,
coalesce(ns.id,ns_p.id) as stop_area_id,
platformcode as platform_code,
sp.id as rid_id
FROM 
(SELECT DISTINCT stop_id FROM navitia_timetable) as  t JOIN scheduledstoppoint as sp ON (stop_id = sp.id)
                                                       LEFT JOIN navitia.stop_area AS ns ON (stoparearef = ns.rid_stoparea_id)
                                                       LEFT JOIN navitia.stop_area AS ns_p ON (sp.id = ns_p.rid_stoppoint_id)
);

COPY (SELECT
id,
properties_id,
uri,
external_code,
coord,
fare_zone,
name,
comment,
stop_area_id,
platform_code
FROM navitia.stop_point) to '/tmp/navitia.stop_point';

DROP TABLE IF EXISTS navitia.journey_pattern_point;
create table navitia.journey_pattern_point as (
SELECT DISTINCT ON (journeypattern_id,nt.pointorder)
row_number() OVER (ORDER BY journeypattern_id,pointorder)-1 as id,
journeypattern_id as journey_pattern_id,
coalesce(sd.name,d.name) as name,
concat_ws(':',journeypattern_id,pointorder) as uri,
stopidx as order,
NULL::text as comment,
ns.id as stop_point_id
FROM (SELECT DISTINCT ON (journeypattern_id) * FROM navitia_patterns) as n JOIN navitia_timetable as nt ON (nt.id = journey_id)
                      JOIN servicejourney as j ON (nt.id = j.id)
                      JOIN journeypattern as jp ON (jp.id = j.journeypatternref)
                      LEFT JOIN destinationdisplay as d ON (d.id = jp.destinationdisplayref)
                      JOIN pointinjourneypattern as pjp USING (journeypatternref,pointorder)
                      LEFT JOIN destinationdisplay as sd ON (sd.id = pjp.destinationdisplayref)
                      JOIN navitia.stop_point as ns ON (ns.rid_id = stop_id)
);
create index on journey_pattern_point(journey_pattern_id,stop_point_id);

COPY (SELECT * FROM navitia.journey_pattern_point) to '/tmp/navitia.journey_pattern_point';

CREATE TABLE IF NOT EXISTS navitia.vehicle_properties (
    id BIGSERIAL PRIMARY KEY,
-- Accès UFR
    wheelchair_accessible BOOLEAN NOT NULL,
-- Embarquement vélo
    bike_accepted BOOLEAN NOT NULL,
-- Air conditionné
    air_conditioned BOOLEAN NOT NULL,
-- Annonce visuelle
    visual_announcement BOOLEAN NOT NULL,
-- Annonce sonore
    audible_announcement BOOLEAN NOT NULL,
-- Accompagnement
    appropriate_escort BOOLEAN NOT NULL,
-- Information claire
    appropriate_signage BOOLEAN NOT NULL,
-- Ligne Scolaire
    school_vehicle BOOLEAN NOT NULL
);

INSERT INTO navitia.vehicle_properties VALUES (0,false,false,false,false,false,false,false,false); 
INSERT INTO navitia.vehicle_properties VALUES (1,true,false,false,false,false,false,false,false); 
INSERT INTO navitia.vehicle_properties VALUES (2,false,true,false,false,false,false,false,false); 
INSERT INTO navitia.vehicle_properties VALUES (3,true,true,false,false,false,false,false,false); 

COPY (SELECT * FROM navitia.vehicle_properties) to '/tmp/navitia.vehicle_properties';

drop table if exists navitia.vehicle_journey;
create table navitia.vehicle_journey as (
SELECT
compressed_id as id,
service_id as adapted_validity_pattern_id,
service_id as validity_pattern_id,
nc.id as company_id,
journeypattern_id as journey_pattern_id,
concat_ws(':','vehicle_journey',journey.privatecode,nullif(row_number() OVER (PARTITION BY journey.privatecode ORDER BY journey.id)-1,0)) as uri,
journey.privatecode as external_code,
NULL::TEXT as comment,
NULL::TEXT as odt_message,
d.name as name,
0::bigint as odt_type_id,
CASE WHEN ((coalesce(hasliftorramp,false) or coalesce(lowfloor,false)) and not coalesce(bicycleallowed,false)) THEN 1
     WHEN (not (coalesce(hasliftorramp,false) or coalesce(lowfloor,false)) and coalesce(bicycleallowed,false)) THEN 2
     WHEN ((coalesce(hasliftorramp,false) or coalesce(lowfloor,false)) and coalesce(bicycleallowed,false)) THEN 3
     ELSE 0 END as vehicle_properties_id,
NULL::bigint as theoric_vehicle_journey_id,
block_p.from_compressed_id as previous_vehicle_journey_id,
block_n.to_compressed_id as next_vehicle_journey_id,
journey.id as rid_id
FROM (SELECT DISTINCT ON (id) * FROM navitia_timetable) as j JOIN journey USING (id)
                                                    JOIN navitia_patterns as np ON (journey_id = journey.id)
                                                    JOIN servicecalendar USING (compressed_id)
                                                    JOIN journeypattern as jp ON (jp.id = journeypatternref)
                                                    JOIN destinationdisplay as d ON (d.id = destinationdisplayref)
                                                    JOIN route as r ON (r.id = routeref)
                                                    JOIN line as l ON (l.id = r.lineref)
                                                    LEFT JOIN navitia_blocktransfers as block_p ON (block_p.to_compressed_id = compressed_id)
                                                    LEFT JOIN navitia_blocktransfers as block_n ON (block_n.from_compressed_id = compressed_id)
                                                    JOIN navitia.company as nc ON (nc.rid_id = operatorref));

UPDATE navitia.vehicle_journey SET previous_vehicle_journey_id = null WHERE previous_vehicle_journey_id not in (SELECT ID FROM navitia.vehicle_journey);
UPDATE navitia.vehicle_journey SET next_vehicle_journey_id = null WHERE next_vehicle_journey_id not in (SELECT ID FROM navitia.vehicle_journey);

SELECT count(*),'INVALID BLOCKTRANSFERS' FROM navitia.vehicle_journey j1 JOIN navitia.vehicle_journey j2 ON (j2.id = j1.next_vehicle_journey_id)
WHERE j2.previous_vehicle_journey_id != j1.id;

SELECT COUNT(*),'INVALID BLOCKTRANSFERS' FROM navitia.vehicle_journey j1 JOIN navitia.vehicle_journey j2 ON (j1.id = j2.previous_vehicle_journey_id)
WHERE j1.next_vehicle_journey_id != j2.id;

COPY (
SELECT
id,
adapted_validity_pattern_id,
validity_pattern_id,
company_id,
journey_pattern_id,
uri,
coalesce(external_code,''),
comment,
odt_message,
name,
odt_type_id,
vehicle_properties_id,
theoric_vehicle_journey_id,
previous_vehicle_journey_id,
next_vehicle_journey_id
FROM navitia.vehicle_journey) to '/tmp/navitia.vehicle_journey';

drop table navitia.stop_time;
create table navitia.stop_time as (
SELECT 
row_number() OVER (ORDER BY nv.id,nt.pointorder)-1 as id,
nv.id as vehicle_journey_id,
njpt.id as journey_pattern_point_id,
arrival_time,
departure_time,
NULL::text as local_traffic_zone,
2147483647 as start_time,
2147483647 as end_time,
2147483647 as headway_sec,
coalesce(ondemand,false) as odt,
forboarding as pick_up_allowed,
foralighting as drop_off_allowed,
false as is_frequency,
NULL::text as comment,
not coalesce(iswaitpoint,true) as date_time_estimated,
NULL::bigint as properties_id
FROM navitia.vehicle_journey  as nv JOIN navitia.journey_pattern_point as njpt USING (journey_pattern_id)
                                    JOIN navitia_timetable as nt ON (rid_id = nt.id AND nt.stopidx = njpt.order)
);

COPY (SELECT * FROM navitia.stop_time) to '/tmp/navitia.stop_time';

DROP TABLE IF EXISTS navitia.connection_kind;
CREATE TABLE IF NOT EXISTS navitia.connection_kind (
    id BIGINT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL
);
INSERT INTO navitia.connection_kind VALUES ('0','extension');
INSERT INTO navitia.connection_kind VALUES ('1','guarantee');
INSERT INTO navitia.connection_kind VALUES ('2','undefined');
INSERT INTO navitia.connection_kind VALUES ('6','stay_in');

COPY (SELECT * FROM navitia.connection_kind) to '/tmp/navitia.connection_kind';

DROP TABLE IF EXISTS navitia.connection_type;
CREATE TABLE IF NOT EXISTS navitia.connection_type (
    id BIGINT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL
);
INSERT INTO navitia.connection_type VALUES ('0','StopPoint');
INSERT INTO navitia.connection_type VALUES ('1','StopArea');
INSERT INTO navitia.connection_type VALUES ('2','Walking');
INSERT INTO navitia.connection_type VALUES ('3','VJ');
INSERT INTO navitia.connection_type VALUES ('4','Guaranteed');
INSERT INTO navitia.connection_type VALUES ('5','Default');

COPY (SELECT * FROM navitia.connection_type) to '/tmp/navitia.connection_type';

drop table navitia.connection;
CREATE TABLE navitia.connection AS (
SELECT DISTINCT ON (departure_stop_point_id,destination_stop_point_id)
departure_stop_point_id,
destination_stop_point_id,
connection_type_id,
properties_id,
greatest(duration,0) as duration,
max_duration,
display_duration
FROM (
(SELECT 
o.id as departure_stop_point_id,
d.id as destination_stop_point_id,
0 as connection_type_id,
0::bigint as properties_id,
transfer_time-30 as duration,
transfer_time as max_duration,
transfer_time as display_duration
FROM transfers JOIN stoppoint as from_stop ON (from_stop_id = from_stop.operator_id)
               JOIN stoppoint as to_stop ON (to_stop_id = to_stop.operator_id)
               JOIN navitia.stop_point as o ON (from_stop.id = o.rid_id)
               JOIN navitia.stop_point as d ON (to_stop.id = d.rid_id)) UNION
(SELECT 
d.id as departure_stop_point_id,
o.id as destination_stop_point_id,
0 as connection_type_id,
0::bigint as properties_id,
transfer_time-30 as duration,
transfer_time as max_duration,
transfer_time as display_duration
FROM transfers JOIN stoppoint as from_stop ON (from_stop_id = from_stop.operator_id)
               JOIN stoppoint as to_stop ON (to_stop_id = to_stop.operator_id)
               JOIN navitia.stop_point as o ON (from_stop.id = o.rid_id)
               JOIN navitia.stop_point as d ON (to_stop.id = d.rid_id))) as x
ORDER BY departure_stop_point_id,destination_stop_point_id,duration
);

INSERT INTO navitia.connection (
SELECT DISTINCT ON (departure_stop_point_id,destination_stop_point_id)
x.*
FROM (
(SELECT 
o.id as departure_stop_point_id,
d.id as destination_stop_point_id,
0 as connection_type_id,
0::bigint as properties_id,
(distance/0.75)::integer+90 as duration,
(distance/0.5)::integer as max_duration,
(distance/0.75)::integer as display_duration
FROM generated_transfers JOIN navitia.stop_point as o ON (from_stop_id = o.rid_id)
                         JOIN navitia.stop_point as d ON (to_stop_id = d.rid_id)
WHERE distance < 500)
UNION
(
SELECT d.id as departure_stop_point_id,
o.id as destination_stop_point_id,
0 as connection_type_id,
0::bigint as properties_id,
(distance/0.75)::integer+90 as duration,
(distance/0.5)::integer as max_duration,
(distance/0.75)::integer as display_duration
FROM generated_transfers JOIN navitia.stop_point as o ON (from_stop_id = o.rid_id)
                         JOIN navitia.stop_point as d ON (to_stop_id = d.rid_id)
WHERE distance < 500)) as x LEFT JOIN navitia.connection as nc USING (departure_stop_point_id,destination_stop_point_id)
WHERE nc.departure_stop_point_id is null
);

create index on navitia.connection(departure_stop_point_id,destination_stop_point_id);
INSERT INTO navitia.connection (
SELECT DISTINCT ON (departure_stop_point_id,destination_stop_point_id)
x.*
FROM (
(SELECT 
o.id as departure_stop_point_id,
d.id as destination_stop_point_id,
0 as connection_type_id,
0::bigint as properties_id,
(distance/0.75)::integer+90 as duration,
(distance/0.5)::integer as max_duration,
(distance/0.75)::integer as display_duration
FROM 
(SELECT distinct sp1.stoparearef as from_stoparea,sp2.stoparearef as to_stoparea
         FROM stoppoint as sp1
         JOIN stoppoint as sp2 ON (st_expand(sp1.the_geom_rd,200) && sp2.the_geom_rd)
         WHERE sp1.isscheduled = true AND sp2.isscheduled = true
           AND sp1.stoparearef is not null AND sp2.stoparearef is not null
UNION
SELECT distinct sp1.stoparearef as from_stoparea,sp2.stoparearef as to_stoparea
         FROM stoppoint as sp1
         JOIN stoppoint as sp2 ON (st_expand(sp1.the_geom_rd,600) && sp2.the_geom_rd)
         WHERE sp1.isscheduled = true AND sp2.isscheduled = true AND 
(sp1.name = 'Utrecht Centraal' or sp1.name like 'Utrecht, CS%' OR sp1.name like 'Amsterdam, Centraal%' 
 OR sp1.name like 'Amsterdam, CS%' OR sp1.name = 'Amsterdam Centraal')
AND (sp2.name = 'Utrecht Centraal' OR sp2.name like 'Utrecht, CS%' OR sp2.name like 'Amsterdam, Centraal%'
     OR sp2.name like 'Amsterdam, CS%' OR sp2.name = 'Amsterdam Centraal')
           AND sp1.stoparearef is not null AND sp2.stoparearef is not null
) as nearby_stopareas
JOIN (SELECT sp1.id as from_stop_id,sp1.stoparearef as from_stoparea,sp2.id as to_stop_id,sp2.stoparearef as to_stoparea
 FROM stoppoint as sp1
 JOIN stoppoint as sp2 ON (st_expand(sp1.the_geom_rd,1000) && sp2.the_geom_rd)) as nearby_stoppoints USING (from_stoparea,to_stoparea)
 JOIN generated_transfers USING (from_stop_id,to_stop_id)
 JOIN navitia.stop_point as o ON (from_stop_id = o.rid_id)
 JOIN navitia.stop_point as d ON (to_stop_id = d.rid_id)
)UNION(
SELECT 
d.id as departure_stop_point_id,
o.id as destination_stop_point_id,
0 as connection_type_id,
0::bigint as properties_id,
(distance/0.75)::integer+90 as duration,
(distance/0.5)::integer as max_duration,
(distance/0.75)::integer as display_duration
FROM 
(SELECT distinct sp1.stoparearef as from_stoparea,sp2.stoparearef as to_stoparea
         FROM stoppoint as sp1
         JOIN stoppoint as sp2 ON (st_expand(sp1.the_geom_rd,200) && sp2.the_geom_rd)
         WHERE sp1.isscheduled = true AND sp2.isscheduled = true
           AND sp1.stoparearef is not null AND sp2.stoparearef is not null
UNION
SELECT distinct sp1.stoparearef as from_stoparea,sp2.stoparearef as to_stoparea
         FROM stoppoint as sp1
         JOIN stoppoint as sp2 ON (st_expand(sp1.the_geom_rd,600) && sp2.the_geom_rd)
         WHERE sp1.isscheduled = true AND sp2.isscheduled = true AND 
(sp1.name = 'Utrecht Centraal' or sp1.name like 'Utrecht, CS%' OR sp1.name like 'Amsterdam, Centraal%' 
 OR sp1.name like 'Amsterdam, CS%' OR sp1.name = 'Amsterdam Centraal')
AND (sp2.name = 'Utrecht Centraal' OR sp2.name like 'Utrecht, CS%' OR sp2.name like 'Amsterdam, Centraal%' 
     OR sp2.name like 'Amsterdam, CS%' OR sp2.name = 'Amsterdam Centraal')
           AND sp1.stoparearef is not null AND sp2.stoparearef is not null
) as nearby_stopareas
JOIN (SELECT sp1.id as from_stop_id,sp1.stoparearef as from_stoparea,sp2.id as to_stop_id,sp2.stoparearef as to_stoparea
 FROM stoppoint as sp1
 JOIN stoppoint as sp2 ON (st_expand(sp1.the_geom_rd,1000) && sp2.the_geom_rd)) as nearby_stoppoints USING (from_stoparea,to_stoparea)
 JOIN generated_transfers USING (from_stop_id,to_stop_id)
 JOIN navitia.stop_point as o ON (from_stop_id = o.rid_id)
 JOIN navitia.stop_point as d ON (to_stop_id = d.rid_id)
)) as x LEFT JOIN navitia.connection as nc USING (departure_stop_point_id,destination_stop_point_id)
WHERE nc.departure_stop_point_id is null
);

COPY (SELECT
departure_stop_point_id,
destination_stop_point_id,
connection_type_id,
properties_id,
duration,
max_duration,
display_duration FROM navitia.connection) to '/tmp/navitia.connection';

--contributor
DROP TABLE IF EXISTS navitia.contributor;
create table navitia.contributor as (
SELECT 0 as id,
'default_contributor'::TEXT as uri,
'default_contributor'::TEXT as name
);

COPY (SELECT * FROM navitia.contributor) to '/tmp/navitia.contributor';
