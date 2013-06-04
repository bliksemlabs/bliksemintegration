copy(
SELECT 
'OVapi' as feed_publisher_name,
'http://www.ovapi.nl' as feed_publisher_url,
'nl' as feed_lang,
replace(cast(date 'yesterday' as text),'-','') as feed_start_date,
replace(cast(max(validdate) as text),'-','') as feed_end_date,
nextval('gtfs_version') as feed_version
FROM availabilityconditionday
) to '/tmp/feed_info.txt' CSV HEADER;

copy(
SELECT
operator_id as agency_id,
name as agency_name,
url as agency_url,
timezone as agency_timezone,
language as agency_lang,
phone as agency_phone
FROM operator
) to '/tmp/agency.txt' CSV HEADER;

copy(
SELECT
id::text as stop_id,
publiccode as stop_code,
name as stop_name,
latitude as stop_lat,
longitude as stop_lon,
0 as location_type,
'stoparea:'||stoparearef as parent_station,
CASE WHEN (stoparearef is null) THEN timezone ELSE NULL END as stop_timezone,
0 as wheelchair_boarding
FROM scheduledstoppoint
UNION
SELECT
'stoparea:'||id as stop_id,
publiccode as stop_code,
name as stop_name,
latitude as stop_lat,
longitude as stop_lon,
1 as location_type,
NULL as parent_station,
timezone as stop_timezone,
0 as wheelchair_boarding
FROM stoparea
) to '/tmp/stops.txt' CSV HEADER;

drop table transportmode;
create table transportmode (
    transportmode varchar(255) primary key,
    route_type integer
);
insert into transportmode VALUES ('TRAM',0);
insert into transportmode VALUES ('METRO',1);
insert into transportmode VALUES ('TRAIN',2);
insert into transportmode VALUES ('BUS',3);
insert into transportmode VALUES ('BOAT',4);

copy(
SELECT 
l.id as route_id,
o.operator_id as agency_id,
publiccode as route_short_name,
l.name as route_long_name,
NULL as route_desc,
route_type
FROM 
line as l LEFT JOIN operator as o ON (l.operatorref = o.id) LEFT JOIN transportmode using (transportmode)
) to '/tmp/routes.txt' CSV HEADER;

copy(
SELECT 
routeref as shape_id,
latitude as shape_pt_lat,
longitude as shape_pt_lon,
pointorder as shape_pt_sequence,
distancefromstart as shape_dist_traveled
FROM pointinroute
ORDER by routeref,pointorder
) to '/tmp/shapes.txt' CSV HEADER;

copy(
SELECT 
ad.availabilityconditionref as service_id,
replace(validdate::text,'-','') as date,
1 as exception_type
FROM availabilityconditionday as ad,activeavailabilitycondition as a
WHERE
ad.availabilityconditionref = a.id AND isavailable = true
) to '/tmp/calendar_dates.txt' CSV HEADER;

copy(
SELECT 
lineref as route_id,
availabilityconditionref as service_id,
j.id as trip_id,
d.name as trip_headsign,
j.name as trip_short_name,
pc.name as trip_long_name,
(directiontype % 2 = 0)::int4 as direction_id,
blockref as block_id,
r.id as shape_id,
(hasliftorramp or lowfloor) as wheelchair_accessible,
CASE WHEN (bicycleallowed) THEN 2 ELSE NULL END as trip_bikes_allowed
FROM servicejourney as j LEFT JOIN journeypattern as p on (j.journeypatternref = p.id)
                         LEFT JOIN route as r on (p.routeref = r.id)
                         LEFT JOIN destinationdisplay as d ON (p.destinationdisplayref = d.id)
                         LEFT JOIN productcategory as pc on (j.productcategoryref = pc.id)
) to '/tmp/trips.txt' CSV HEADER;

copy(
SELECT 
j.id as trip_id,
p_pt.pointorder as stop_sequence,
p_pt.pointref as stop_id,
CASE WHEN (p.destinationdisplayref != p_pt.destinationdisplayref) THEN d.name ELSE null END as stop_headsign,
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
distancefromstartroute as shape_dist_traveled
FROM servicejourney as j LEFT JOIN journeypattern as p on (j.journeypatternref = p.id)
                         LEFT JOIN pointinjourneypattern as p_pt on (p_pt.journeypatternref = p.id)
                         LEFT JOIN pointintimedemandgroup as t_pt on (j.timedemandgroupref = t_pt.timedemandgroupref AND p_pt.pointorder = 
t_pt.pointorder)
                         LEFT JOIN destinationdisplay as d ON (p_pt.destinationdisplayref = d.id), scheduledstoppoint as s_pt
WHERE p_pt.pointref = s_pt.id and totaldrivetime is not null
) to '/tmp/stop_times.txt' CSV HEADER;
