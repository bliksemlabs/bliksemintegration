create temporary table gtfs_feed_info as (
SELECT 
'OVapi'::text as feed_publisher_name,
'http://www.ovapi.nl'::text as feed_publisher_url,
'nl'::text as feed_lang,
replace(cast(date 'today' as text),'-','') as feed_start_date,
replace(cast(max(validdate) as text),'-','') as feed_end_date,
nextval('gtfs_version') as feed_version
FROM availabilityconditionday
);

create temporary table servicecalendar as (
SELECT validfrom,bitcalendar,row_number() OVER () as service_id,unnest(array_agg(availabilityconditionref)) as availabilityconditionref  FROM (
   SELECT availabilityconditionref, bitcalendar(array_agg(validdate ORDER BY validdate)) as bitcalendar,min(validdate) as validfrom FROM 
    availabilityconditionday as ad WHERE isavailable = true AND 
validdate between date 'yesterday' AND (select 
max(todate) from activeavailabilitycondition where operator_id like 'IFF%') GROUP by availabilityconditionref) as x
GROUP BY validfrom,bitcalendar
ORDER BY service_id
);

create temporary table gtfs_calendar_dates as (
SELECT DISTINCT ON (service_id,date,exception_type)
service_id,
replace(unnest(bitcalendar(validfrom,bitcalendar))::text,'-','') as date,
1 as exception_type
FROM servicecalendar
);

create temporary table gtfs_shapes as (
SELECT 
routeref::text as shape_id,
pointorder as shape_pt_sequence,
latitude as shape_pt_lat,
longitude as shape_pt_lon,
distancefromstart as shape_dist_traveled
FROM (SELECT *,count(*) OVER (partition BY routeref) as routepointcount FROM pointinroute) as pointinroute LEFT JOIN 
     (SELECT routeref,max(journeypatternpoints) as patternpointcount
      FROM journeypattern,
            (SELECT journeypatternref,count(distinct pointorder) as journeypatternpoints 
            FROM pointinjourneypattern group by journeypatternref) as jc
      WHERE jc.journeypatternref = journeypattern.id GROUP BY routeref) as journeypatterncount USING (routeref)
WHERE routepointcount > patternpointcount*1.5
ORDER by routeref,pointorder
);

create temporary table gtfs_trips as (
SELECT 
lineref as route_id,
service_id,
j.id as trip_id,
d.name as trip_headsign,
j.name as trip_short_name,
pc.name as trip_long_name,
(directiontype % 2 = 0)::int4 as direction_id,
CASE WHEN (blockcoherent = true AND blockref is not null AND blockcount > 1 
     AND (blockref like 'IFF:%' or j.privatecode like 'ARR:160__:%')) THEN blocknumber ELSE NULL END as block_id,
--CASE WHEN (blockcoherent = true AND blockref is not null AND blockcount > 1) THEN blocknumber ELSE NULL END as block_id,
shape_id::text,
CASE WHEN (hasliftorramp is null and lowfloor is null) THEN 0::int4
     WHEN (hasliftorramp or lowfloor) THEN 1::int4
     ELSE 2::int4 END as wheelchair_accessible,
CASE WHEN (bicycleallowed) THEN 2 ELSE NULL END as trip_bikes_allowed
FROM (select *,count(id) over (PARTITION BY availabilityconditionref,blockref) as  blockcount,
(blockref not in (SELECT distinct j1.blockref FROM 
(SELECT journey.privatecode,blockref,departuretime,departuretime+max(totaldrivetime) as arrivaltime,count(journey.id) OVER (PARTITION BY blockref 
ORDER BY departuretime) as blockseq
 FROM journey JOIN pointintimedemandgroup USING (timedemandgroupref) WHERE blockref IS NOT null GROUP By journey.id,blockref) as j1
JOIN
( SELECT journey.privatecode,blockref,departuretime,departuretime+max(totaldrivetime) as arrivaltime,count(journey.id) OVER (PARTITION BY blockref 
ORDER BY departuretime) as blockseq
  FROM journey JOIN pointintimedemandgroup USING (timedemandgroupref) WHERE blockref IS NOT null GROUP By journey.id,blockref) as j2
 ON (j1.blockref = j2.blockref and j1.blockseq = j2.blockseq -1)
WHERE j1.arrivaltime > j2.departuretime)) as blockcoherent,
               count(blockref) over (ORDER BY blockref) as blocknumber FROM servicejourney) as j
                         JOIN servicecalendar USING (availabilityconditionref) 
                         LEFT JOIN journeypattern as p on (j.journeypatternref = p.id)
                         LEFT JOIN destinationdisplay as d ON (p.destinationdisplayref = d.id)
                         LEFT JOIN productcategory as pc on (j.productcategoryref = pc.id)
                         LEFT JOIN route ON (routeref = route.id)
                         LEFT JOIN (select distinct shape_id from gtfs_shapes) as shapes ON (routeref::text = shape_id)
);

SELECT count(*) FROM gtfs_trips WHERE service_id is null;
SELECT COUNT(distinct shape_id) from gtfs_trips;
SELECT COUNT(distinct shape_id) from gtfs_shapes;
DELETE FROM gtfs_shapes WHERE shape_id NOT IN (SELECT DISTINCT shape_id from gtfs_trips);

create temporary table gtfs_routes as (
SELECT 
l.id as route_id,
o.operator_id as agency_id,
publiccode as route_short_name,
l.name as route_long_name,
NULL::text as route_desc,
gtfs_route_type as route_type,
color_shield as route_color,
color_text as route_text_color
FROM 
line as l LEFT JOIN operator as o ON (l.operatorref = o.id) LEFT JOIN transportmode using (transportmode)
WHERE l.id in (SELECT DISTINCT route_id FROM gtfs_trips)
);

create temporary table gtfs_agency as (
SELECT
operator_id as agency_id,
name as agency_name,
url as agency_url,
timezone as agency_timezone,
phone as agency_phone
FROM operator
WHERE operator_id in (select distinct agency_id from gtfs_routes)
);

create temporary table gtfs_stop_times as (
SELECT 
j.id as trip_id,
p_pt.pointorder as stop_sequence,
p_pt.pointref as stop_id,
CASE WHEN (p.destinationdisplayref != p_pt.destinationdisplayref AND p_pt.destinationdisplayref is not null) THEN d.name ELSE null END as 
stop_headsign,
to32time(departuretime+totaldrivetime) as arrival_time,
to32time(departuretime+totaldrivetime+stopwaittime) as departure_time,
CASE WHEN (forboarding = false) THEN 1
     WHEN (ondemand = true)     THEN 2
     WHEN (requeststop = true)  THEN 3
     ELSE                            0 END as pickup_type,
CASE WHEN (foralighting = false) THEN 1
     WHEN (ondemand = true)      THEN 2
     WHEN (requeststop = true)   THEN 3
     ELSE                            0 END as drop_off_type,
iswaitpoint::int4 as timepoint,
CASE WHEN (shape_id is not null) THEN distancefromstartroute ELSE NULL END as shape_dist_traveled,
fareunitspassed as fare_units_traveled
FROM servicejourney as j LEFT JOIN journeypattern as p on (j.journeypatternref = p.id)
                         LEFT JOIN pointinjourneypattern as p_pt on (p_pt.journeypatternref = p.id)
                         LEFT JOIN pointintimedemandgroup as t_pt on (j.timedemandgroupref = t_pt.timedemandgroupref AND p_pt.pointorder = 
t_pt.pointorder)
                         LEFT JOIN destinationdisplay as d ON (p_pt.destinationdisplayref = d.id)
                         JOIN gtfs_trips ON (j.id = trip_id),
     scheduledstoppoint as s_pt WHERE p_pt.pointref = s_pt.id and totaldrivetime is not null and j.id is not null
);

SELECT COUNT(*) FROM gtfs_stop_times WHERE trip_id not in (select distinct trip_id from gtfs_trips);

create temporary table gtfs_stops as (
SELECT
s.id::text as stop_id,
s.publiccode as stop_code,
s.name as stop_name,
s.latitude as stop_lat,
s.longitude as stop_lon,
0 as location_type,
CASE WHEN (s.operator_id not like 'RET:%' AND s.name != 'Petten, Campanula') THEN 'stoparea:'||stoparearef ELSE null END as parent_station,
CASE WHEN (stoparearef is null) THEN s.timezone ELSE NULL END as stop_timezone,
restrictedmobilitysuitable::int4 as wheelchair_boarding,
s.platformcode as platform_code,
rail_fare.station as zone_id
FROM scheduledstoppoint as s LEFT JOIN stoparea ON (stoparea.id = s.stoparearef)
                             LEFT JOIN (select distinct station from rail_fare) as rail_fare ON (rail_fare.station = stoparea.operator_id)
WHERE s.id in (SELECT DISTINCT stop_id from gtfs_stop_times) OR s.operator_id like 'IFF%'
);
INSERT INTO gtfs_stops (
SELECT
'stoparea:'||id as stop_id,
publiccode as stop_code,
name as stop_name,
latitude as stop_lat,
longitude as stop_lon,
1 as location_type,
NULL as parent_station,
timezone as stop_timezone,
0 as wheelchair_boarding,
NULL as platformcode,
rail_fare.station as zone_id
FROM stoparea LEFT JOIN (select distinct station from rail_fare) as rail_fare ON (rail_fare.station = stoparea.operator_id)
WHERE 'stoparea:'||id in (SELECT DISTINCT parent_station FROM gtfs_stops)
);

CREATE TEMPORARY TABLE gtfs_transfers as (
SELECT
pointref as from_stop_id,
onwardpointref as to_stop_id,
NULL::text as from_route_id,
NULL::text as to_route_id,
journeyref as from_trip_id,
onwardjourneyref as to_trip_id,
CASE WHEN (transfer_type = 0) THEN 3
     WHEN (transfer_type = 1) THEN 0
     WHEN (transfer_type = 2) THEN 1 END as transfer_type
FROM 
journeytransfers
WHERE
pointref::text in (SELECT DISTINCT stop_id from gtfs_stops) AND
onwardpointref::text in (SELECT DISTINCT stop_id from gtfs_stops) AND
journeyref IN (SELECT DISTINCT trip_id from gtfs_trips) AND
onwardjourneyref IN (SELECT DISTINCT trip_id from gtfs_trips)
);

CREATE TEMPORARY TABLE gtfs_fare_rules as (
SELECT 
'IFF:'||least(fare_units,250) as fare_id,
NULL::text as route_id,
station as origin_id,
onwardstation as destination_id,
NULL::text as contains_id
FROM rail_fare
WHERE
station in (select distinct operator_id from stoparea) AND
onwardstation in (select distinct operator_id from stoparea)
);

CREATE TEMPORARY TABLE gtfs_fare_attributes as (
SELECT DISTINCT ON (secondfull::float,fare_id)
'IFF:'||f.fare_units as fare_id,
'IFF:NS'::text as agency_id,
secondfull as price,
'EUR'::text as currency_type,
1::int4 as payment_method,
NULL::int4 as transfers,
NULL::int4 as transfer_duration
FROM rail_fare_prices as f
WHERE
fare_units in (SELECT DISTINCT fare_units FROM rail_fare)
ORDER BY secondfull::float,fare_id
);

COPY (SELECT * FROM gtfs_feed_info) to '/tmp/feed_info.txt' CSV HEADER;
COPY (SELECT * FROM gtfs_agency) to '/tmp/agency.txt' CSV HEADER;
COPY (SELECT * FROM gtfs_shapes WHERE shape_id::text IN (SELECT DISTINCT shape_id::text FROM gtfs_trips))  to '/tmp/shapes.txt' CSV HEADER;
COPY (SELECT * FROM gtfs_routes) to '/tmp/routes.txt' CSV HEADER;
COPY (SELECT * FROM gtfs_calendar_dates) to '/tmp/calendar_dates.txt' CSV HEADER;
COPY (SELECT * FROM gtfs_stops) to '/tmp/stops.txt' CSV HEADER;
COPY (SELECT * FROM gtfs_trips) to '/tmp/trips.txt' CSV HEADER;
COPY (SELECT * FROM gtfs_stop_times) to '/tmp/stop_times.txt' CSV HEADER;
COPY (SELECT * FROM gtfs_transfers) to '/tmp/transfers.txt' CSV HEADER;
COPY (SELECT * FROM gtfs_fare_rules) to '/tmp/fare_rules.txt' CSV HEADER;
COPY (SELECT * FROM gtfs_fare_attributes) to '/tmp/fare_attributes.txt' CSV HEADER;
