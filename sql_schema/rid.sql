create extension postgis;
CREATE SEQUENCE gtfs_version START 1;

create table DataSource(
    id serial primary key NOT NULL,
    operator_id varchar(255) NOT NULL,
    name varchar(255) NOT NULL,
    description varchar(255),
    email varchar (255),
    url varchar (255)
);

create table Operator(
    id serial primary key NOT NULL,
    operator_id varchar(255) NOT NULL,
    privatecode varchar(255) NOT NULL,
    name varchar(255) NOT NULL,
    phone varchar(255),
    url varchar(255) NOT NULL,
    timezone varchar(255) NOT NULL,
    language varchar(2) NOT NULL
);

create table Version(
    id serial primary key,
    operator_id varchar(255),
    privatecode varchar(255) NOT NULL,
    datasourceRef integer NOT NULL references datasource(id),
    startdate date NOT NULL,
    enddate date NOT NULL,
    description varchar(255),
    versionmajor integer,
    versionminor integer
);

create table RejectedVersion(
    id serial primary key,
    operator_id varchar(255),
    privatecode varchar(255) NOT NULL,
    datasourceRef integer NOT NULL references datasource(id),
    startdate date NOT NULL,
    enddate date NOT NULL,
    description varchar(255),
    error varchar(255)
);


create table AvailabilityCondition(
    id bigserial primary key NOT NULL,
    privatecode varchar(255) NOT NULL,
    operator_id varchar(255),
    unitcode varchar(255),
    versionRef integer references version(id) NOT NULL,
    name varchar(255),
    fromdate date,
    todate date
);

create table AvailabilityConditionDay(
    id serial8,
    availabilityconditionRef integer references AvailabilityCondition(id) NOT NULL,
    validdate date NOT NULL,
    isavailable boolean,
    primary key (availabilityconditionRef,validdate)
);

create table productCategory(
    id bigserial primary key NOT NULL,
    privatecode varchar(255),
    operator_id varchar(255),
    shortname varchar(255),
    name varchar(255)
);

create table notice(
    id bigserial primary key NOT NULL,
    privatecode varchar(255),
    operator_id varchar(255),
    publiccode varchar(255),
    shortcode varchar(255),
    name varchar(255) NOT NULL
);

create table noticegroup(
    id integer NOT NULL,
    noticeRef integer references notice(id),
    primary key(id,noticeref)
);


create table noticeassignment(
    id bigserial primary key NOT NULL,
    noticegroupRef integer,
    privatecode varchar(255),
    operator_id varchar(255),
    name varchar(255) NOT NULL,
    validfrom date,
    validthru date
);

create table destinationDisplay(
    id bigserial primary key NOT NULL,
    privatecode varchar(255) NOT NULL,
    operator_id varchar(255),
    name varchar(255) NOT NULL,
    shortname varchar(255) NOT NULL,
    vianame varchar(255)
);

create table timedemandgroup(
    id bigserial primary key NOT NULL,
    operator_id varchar(255),
    privatecode varchar(255)
);

create table pointintimedemandgroup(
    id bigserial primary key NOT NULL,
    operator_id varchar(255),
    privatecode varchar(255),
    timedemandgroupref integer references timedemandgroup(id) NOT NULL,
    pointorder integer NOT NULL,
    totaldrivetime integer NOT NULL,
    stopwaittime integer NOT NULL
);

create table Line(
    id bigserial primary key NOT NULL,
    operatorref integer references operator(id),
    privatecode varchar(255) NOT NULL,
    operator_id varchar(255) NOT NULL,
    publiccode varchar(255) NOT NULL,
    TransportMode varchar(255) NOT NULL,
    name varchar(255),
    monitored boolean
);

create table Route(
    id bigserial primary key NOT NULL,
    operator_id varchar(255) NOT NULL,
    lineref integer references line(id)
);

create table PointInRoute(
    routeref integer references route(id) NOT NULL,
    privatecode varchar(255),
    pointorder integer NOT NULL,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    distancefromstart integer,
    primary key (routeref,pointorder)
);

create table AdministrativeZone(
    id bigserial primary key NOT NULL,
    privatecode varchar(255),
    operator_id varchar(255),
    name varchar(255),
    description varchar (255)
);

create table JourneyPattern(
    id bigserial primary key NOT NULL,
    privatecode varchar(255),
    operator_id varchar(255),
    routeref integer references route(id),
    directiontype integer,
    destinationdisplayref integer references destinationDisplay(id)
);

create table StopArea(
    id bigserial primary key NOT NULL,
    privatecode varchar(255),
    operator_id varchar(255),
    name varchar(255),
    town varchar(255),
    latitude double precision,
    longitude double precision,
    timezone varchar(255),
    publiccode varchar(255)
);

create table StopPoint(
    id bigserial primary key NOT NULL,
    privatecode varchar(255),
    operator_id varchar(255),
    publiccode varchar(255),
    isScheduled boolean,
    stopareaRef integer references stoparea(id),
    name varchar(255),
    town varchar(255),
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    rd_x integer,
    rd_y integer, 
    timezone varchar(255),
    platformcode varchar(25),
    the_geom geometry(Point,4326),
    the_geom_rd geometry (Point,28992),
    visualimpairmentsuitable boolean,
    restrictedmobilitysuitable boolean
);

CREATE VIEW scheduledstoppoint AS (SELECT 
id,privatecode,operator_id,publiccode,stoparearef,name,town,latitude,longitude,rd_x,rd_y,timezone,platformcode,visualimpairmentsuitable,
restrictedmobilitysuitable
FROM stoppoint where isscheduled = true);

create table PointInJourneyPattern(
    journeypatternref integer references journeypattern(id) NOT NULL,
    pointorder integer NOT NULL,
    privatecode varchar(255),
    operator_id varchar(255),
    pointref integer references stoppoint(id) NOT NULL,
    destinationdisplayref integer references DestinationDisplay(id),
    noticeassignmentRef integer references noticeassignment(id),
    administrativezoneRef integer references administrativezone(id),
    onwardpointref integer references stoppoint(id),
    iswaitpoint boolean,
    waittime integer,
    requeststop boolean,
    foralighting boolean NOT NULL,
    forboarding boolean NOT NULL,
    distancefromstartroute integer,
    fareUnitsPassed integer,
    primary key (journeypatternref,pointorder)
);

create table Journey(
    id bigserial primary key NOT NULL,
    privatecode varchar(255) NOT NULL,
    operator_id varchar(255) NOT NULL,
    availabilityconditionRef integer references AvailabilityCondition(id) NOT NULL,
    journeypatternref integer references JourneyPattern(id) NOT NULL,
    timedemandgroupref integer references timedemandgroup(id) NOT NULL,
    productCategoryRef integer references productCategory (id) NOT NULL,
    noticeassignmentRef integer references noticeassignment(id),
    departuretime integer,
    blockref varchar(255),
    name varchar(255),
    lowfloor boolean,
    hasLiftOrRamp boolean,
    haswifi boolean,
    bicycleAllowed boolean,
    onDemand boolean
);

create table journeytransfers(
    operator_id varchar(255),
    journeyref bigint,
    pointref bigint references stoppoint(id),
    onwardjourneyref bigint,
    onwardpointref bigint references stoppoint(id),
    transfer_type int4,
    PRIMARY KEY (journeyref,pointref,onwardjourneyref,onwardpointref),
    FOREIGN KEY (journeyref) REFERENCES journey(id) ON DELETE CASCADE,
    FOREIGN KEY (onwardjourneyref) REFERENCES journey(id) ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION 
to32time(time24 text, shift24 integer) RETURNS text AS $$
SELECT lpad(floor((total / 3600))::text, 2, '0')||':'||lpad(((total % 3600) / 60)::text, 2, '0')||':'||lpad((total % 60)::text, 2, '0') AS time
FROM
(SELECT
  (cast(split_part($1, ':', 1) as int4) * 3600)      -- hours
+ (cast(split_part($1, ':', 2) as int4) * 60)        -- minutes
+ CASE WHEN $1 similar to '%:%:%' THEN (cast(split_part($1, ':', 3) as int4)) ELSE 0 END -- seconds when applicable
+ (shift24 * 86400) as total --Add 24 hours (in seconds) when shift occured
) as xtotal
$$ LANGUAGE SQL;

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

CREATE VIEW ActiveAvailabilityCondition AS (
SELECT id,privatecode,operator_id,unitcode,versionref,startdate,todate FROM (
SELECT ac.id,privatecode,operator_id,unitcode,versionref,name,min(validdate) as startdate,max(validdate) as todate
FROM AvailabilityCondition as ac LEFT JOIN AvailabilityConditionDay as ad ON ( ac.id = ad.availabilityconditionRef)
WHERE isavailable = true
GROUP BY ac.id) as x
);

CREATE VIEW CurrentAvailabilityCondition AS (
SELECT id,privatecode,operator_id,unitcode,versionref,startdate,todate FROM (
SELECT ac.id,privatecode,operator_id,unitcode,versionref,name,min(validdate) as startdate,max(validdate) as todate
FROM AvailabilityCondition as ac LEFT JOIN AvailabilityConditionDay as ad ON ( ac.id = ad.availabilityconditionRef)
WHERE isavailable = true AND validdate >= date 'yesterday'
GROUP BY ac.id) as x
);

CREATE VIEW ServiceJourney AS (
SELECT * FROM journey
);

CREATE VIEW ActiveServiceJourney AS (
SELECT * FROM journey WHERE AvailabilityConditionRef in (select id from ActiveAvailabilityCondition)
);

CREATE FUNCTION bitcalendar(date[]) RETURNS varbit AS $$
SELECT string_agg((b is not null)::int4::char, '' ORDER BY a)::varbit FROM
(SELECT generate_series($1[array_lower($1,1)], $1[array_upper($1,1)], '1 day')::date AS a) AS x
LEFT JOIN
(SELECT unnest($1) AS b) AS y
ON a = b;
$$ LANGUAGE SQL;

CREATE FUNCTION bitcalendar(date, varbit) RETURNS date[] AS $$
SELECT array_agg(DISTINCT selecteddate ORDER BY selecteddate)
FROM
(SELECT *, row_number() OVER () AS a
FROM (SELECT generate_series($1, $1 + LENGTH($2), '1 day')::date AS selecteddate) AS x) AS x1
LEFT JOIN
(SELECT *, row_number() OVER () AS b
FROM (SELECT regexp_split_to_table($2::varchar,'') AS usedate) AS y) AS y1
ON a = b
WHERE usedate = '1';
$$ LANGUAGE SQL; 

CREATE table transportmode (
    transportmode varchar(255) primary key, 
    bison_transporttype varchar(255), 
    gtfs_route_type int4,
    name varchar(255)
);
INSERT INTO transportmode VALUES ('TRAM','TRAM',0,'Tram');
INSERT INTO transportmode VALUES ('METRO','METRO',1,'Metro');
INSERT INTO transportmode VALUES ('TRAIN','TRAIN',2,'Trein');
INSERT INTO transportmode VALUES ('BUS','BUS',3,'Bus');
INSERT INTO transportmode VALUES ('BOAT','BOAT',4,'Veer');

create table rail_fare (
    station varchar(255),
    onwardstation varchar(255),
    fare_units integer,
    primary key(station,onwardstation)
);

create table rail_fare_prices (
    fare_units integer primary key,
    secondfull varchar(6),
    second20 varchar(6),
    second40 varchar(6),
    firstfull varchar(6),
    first20 varchar(6),
    first40 varchar(6)
);

--indices
create index on journey(availabilityconditionref);
create index on journey(journeypatternref);
create index on journey(privatecode);
create index on journey(timedemandgroupref);
create index on stoppoint(operator_id);
create index stoppoint_geom_gist on stoppoint USING gist(the_geom);
create index stoppoint_geom_rd_gist on stoppoint USING gist(the_geom_rd);
create index on pointinjourneypattern(pointref);
create index on availabilityconditionday(validdate);
create index on availabilityconditionday(validdate,isavailable);
