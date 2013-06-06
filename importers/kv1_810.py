import psycopg2
import psycopg2.extras
from kv1_805 import *
from schema.schema_810 import schema

def getTimeDemandGroups(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    timedemandgroups = {}
    cur.execute("""
SELECT * FROM (
SELECT
concat_ws(':',dataownercode,lineplanningnumber,journeypatterncode,timedemandgroupcode) as operator_id,
concat_ws(':',dataownercode,lineplanningnumber,journeypatterncode,timedemandgroupcode) as privatecode,
cast(timinglinkorder  as integer) as pointorder,
drivetime,
stopwaittime
FROM timdemrnt
UNION (
SELECT DISTINCT ON (version,dataownercode,lineplanningnumber,journeypatterncode,timedemandgroupcode)
concat_ws(':',dataownercode,lineplanningnumber,journeypatterncode,timedemandgroupcode) as operator_id,
concat_ws(':',dataownercode,lineplanningnumber,journeypatterncode,timedemandgroupcode) as privatecode,
cast(timinglinkorder+1 as integer) as pointorder,
NULL as drivetime,
NULL as stopwaittime
FROM timdemrnt
ORDER BY version,dataownercode,lineplanningnumber,journeypatterncode,timedemandgroupcode,pointorder DESC)) as x
ORDER BY operator_id,pointorder
""")
    totaldrivetime = 0
    stopwaittime = 0
    for row in cur.fetchall():
        if row['operator_id'] not in timedemandgroups:
            timedemandgroups[row['operator_id']] = {'operator_id' : row['operator_id'], 'privatecode' : row['privatecode'], 'POINTS' : [{'pointorder' : row['pointorder'],'totaldrivetime' : 0, 'stopwaittime' : 0}]}
            totaldrivetime = row['drivetime']
            stopwaittime = row['stopwaittime']
        else:
            points = timedemandgroups[row['operator_id']]['POINTS']
            point_dict = {'pointorder' : row['pointorder'],'totaldrivetime' : totaldrivetime, 'stopwaittime' : stopwaittime}
            points.append(point_dict)
            if row['drivetime'] is None: 
                totaldrivetime = None
                stopwaittime = None
            else:
                totaldrivetime += row['drivetime'] + stopwaittime
                stopwaittime = row['stopwaittime']
    return timedemandgroups

def getAvailabilityConditionsFromCalendars(conn,validfrom):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    availabilityConditions = {}
    cur.execute("""
SELECT DISTINCT ON (version, dataownercode, organizationalunitcode,timetableversioncode,periodgroupcode,specificdaycode,daytype)
concat_ws(':',version, dataownercode, organizationalunitcode,timetableversioncode,periodgroupcode,specificdaycode,daytype) as operator_id,
concat_ws(':',version, dataownercode, organizationalunitcode,timetableversioncode,periodgroupcode,specificdaycode,daytype) as privatecode,
dataownercode||':'||organizationalunitcode as unitcode,
'1' as versionref,
description as name,
cast(greatest(coalesce(t.validfrom,pg.validfrom),%s::date) as text) as fromdate,
cast(coalesce(t.validthru,pg.validthru) as text) as todate
FROM pegrval as pg JOIN tive as t USING (version, dataownercode, organizationalunitcode, periodgroupcode)
                   JOIN pujo as pu USING (version, dataownercode, organizationalunitcode, timetableversioncode, periodgroupcode, specificdaycode)
WHERE coalesce(t.validthru,pg.validthru) >= %s::date;
""",[validfrom]*2)
    for row in cur.fetchall():
        availabilityConditions[row['operator_id']] = row
    cur.execute("""
SELECT
operator_id as availabilityconditionRef,
array_agg(date ORDER BY date) as validdates,
true as isavailable
FROM
(
SELECT
operator_id,
cast(date as text) as date
FROM(
	SELECT
	operator_id,
	CASE WHEN (validfrom != validthru) THEN cast (generate_series(validfrom,validthru,'1 day') as date) ELSE validfrom END as date,
        daytype
	FROM (
		SELECT DISTINCT ON (operator_id,validfrom,validthru)
		concat_ws(':',version, dataownercode, organizationalunitcode,timetableversioncode,periodgroupcode,specificdaycode,daytype) AS operator_id,
		pj.daytype,
		pg.validfrom as validfrom,
		pg.validthru as validthru
                FROM pegrval as pg JOIN tive as tv USING (version, dataownercode, organizationalunitcode, periodgroupcode)
                                   JOIN pujo as pj USING (version, dataownercode, organizationalunitcode, timetableversioncode, periodgroupcode, specificdaycode)
                WHERE coalesce(tv.validthru,pg.validthru) >= %s::date
                ORDER BY operator_id,validfrom,validthru
                ) as calendar
	) as calendar_dates
WHERE
position( CAST(CASE WHEN extract(dow from date) = 0 THEN 7 ELSE extract(dow from date) END as text) in daytype) != 0
AND NOT EXISTS (
  SELECT 1 FROM (select cast(validdate as date) as excopdate,left(cast(daytypeason as text),1) as daytypeason from excopday) as excopday
  WHERE date = excopdate and position( CAST(CASE WHEN extract(dow from date) = 0 THEN 7 ELSE extract(dow from date) END as text) in daytypeason) = 0
  ) AND
date >= %s
UNION
SELECT
concat_ws(':',version, dataownercode, organizationalunitcode,timetableversioncode,periodgroupcode,specificdaycode,daytype) AS operator_id,
cast(date as text) as date
FROM(
	SELECT
	*,
        (select distinct daytype 
         FROM pujo
         WHERE pujo.version = dates.version AND pujo.dataownercode = dates.dataownercode AND pujo.timetableversioncode = dates.timetableversioncode 
                AND pujo.organizationalunitcode = dates.organizationalunitcode AND pujo.periodgroupcode = dates.periodgroupcode 
                AND pujo.specificdaycode = dates.specificdaycode AND position( daytypeason in daytype) != 0)
	FROM(
		SELECT
		*,
		CASE WHEN (validfrom != validthru) THEN cast (generate_series(validfrom,validthru,'1 day') as date) ELSE validfrom END as date
		FROM (
			SELECT DISTINCT ON (version,dataownercode,timetableversioncode,organizationalunitcode,periodgroupcode,specificdaycode,validfrom,validthru)
                        version,
                        dataownercode,
                        timetableversioncode,
                        organizationalunitcode,
                        periodgroupcode,
                        specificdaycode,
			coalesce(tv.validfrom,pg.validthru) as validfrom,
			coalesce(tv.validthru,pg.validthru) as validthru
			FROM pegrval as pg JOIN tive as tv USING (version, dataownercode, organizationalunitcode, periodgroupcode)
                                           JOIN pujo as pj USING (version, dataownercode, organizationalunitcode, timetableversioncode, periodgroupcode, specificdaycode)
			WHERE coalesce(tv.validthru,pg.validthru) >= %s::date
			ORDER BY version,dataownercode,timetableversioncode,organizationalunitcode,periodgroupcode,specificdaycode,validfrom,validthru
                        ) as calendar
		) as dates,
                 (select cast(validdate as date) as excopdate,left(cast(daytypeason as text),1) as daytypeason from excopday) as excopday
	WHERE
                excopdate = date) as x
WHERE
daytype is not null AND
date >= %s) as x
GROUP BY operator_id
;""",[validfrom]*4)
    for row in cur.fetchall():
        availabilityConditions[row['availabilityconditionref']]['DAYS'] = row
    cur.close()
    return availabilityConditions

def getJourneysFromPujo(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT
concat_ws(':',dataownercode,lineplanningnumber,journeynumber) as privatecode,
concat_ws(':',version, dataownercode, organizationalunitcode,timetableversioncode,periodgroupcode,specificdaycode,daytype, lineplanningnumber, journeynumber) as operator_id,
concat_ws(':',version, dataownercode, organizationalunitcode,timetableversioncode,periodgroupcode,specificdaycode,daytype) as availabilityconditionRef,
concat_ws(':',dataownercode,lineplanningnumber,journeypatterncode) as journeypatternref,
concat_ws(':',dataownercode,lineplanningnumber,journeypatterncode,timedemandgroupcode) as timedemandgroupref,
cast(coalesce(productformulatype,0) as text) as productCategoryRef,
NULL as noticeassignmentRef,
toseconds(departuretime,0) as departuretime,
NULL as blockref,
cast(journeynumber as integer) as name,
CASE WHEN (wheelchairaccessible = 'UNKNOWN') THEN NULL 
     ELSE (wheelchairaccessible = 'ACCESSIBLE') END as lowfloor,
CASE WHEN (wheelchairaccessible = 'UNKNOWN') THEN NULL 
     ELSE (wheelchairaccessible = 'ACCESSIBLE') END as hasLiftOrRamp,
NULL as haswifi,
CASE WHEN (dataownercode = 'CXX' and lineplanningnumber in ('N419','Z050','Z060','Z020')) THEN true
     ELSE null END
     as bicycleallowed,
productformulatype in (2,8,35,36) as onDemand
FROM pujo LEFT JOIN (SELECT DISTINCT ON (version,dataownercode,lineplanningnumber,journeypatterncode)
                                             version,dataownercode,lineplanningnumber,journeypatterncode,productformulatype FROM jopatili
                                             ORDER BY version,dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder) as pattern
                        USING(version,dataownercode,lineplanningnumber,journeypatterncode)
              LEFT JOIN line using (version,dataownercode,lineplanningnumber)
WHERE dataownerisoperator = true
ORDER BY version, dataownercode, organizationalunitcode,timetableversioncode,periodgroupcode,specificdaycode,daytype, lineplanningnumber, journeynumber
""")
    journeys = cur.fetchall()
    cur.close()
    return journeys


def getJourneys(timedemandGroupRefForJourney,conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT DISTINCT ON (version, dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber)
concat_ws(':',dataownercode,lineplanningnumber,journeynumber) as privatecode,
concat_ws(':',version, dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber) as operator_id,
concat_ws(':', dataownercode, organizationalunitcode, schedulecode, scheduletypecode) as availabilityconditionRef,
concat_ws(':',dataownercode,lineplanningnumber,journeypatterncode) as journeypatternref,
NULL as timedemandgroupref,
cast(coalesce(productformulatype,0) as text) as productCategoryRef,
NULL as noticeassignmentRef,
NULL as departuretime,
NULL as blockref,
cast(journeynumber as integer) as name,
CASE WHEN (wheelchairaccessible = 'UNKNOWN') THEN NULL 
     ELSE (wheelchairaccessible = 'ACCESSIBLE') END as lowfloor,
CASE WHEN (wheelchairaccessible = 'UNKNOWN') THEN NULL 
     ELSE (wheelchairaccessible = 'ACCESSIBLE') END as hasLiftOrRamp,
NULL as haswifi,
CASE WHEN (dataownercode = 'ARR' and lineplanningnumber like '15___') THEN true
     WHEN (dataownercode = 'GVB' and lineplanningnumber like '90_') THEN true
     WHEN (dataownercode = 'GVB' and lineplanningnumber in ('50','51','52','53','54')) THEN true
     WHEN (dataownercode = 'ARR' and lineplanningnumber in ('17090','17194','17196')) THEN true
     ELSE NULL END as bicycleallowed,
productformulatype in (2,8,35,36) as onDemand
FROM pujopass LEFT JOIN (SELECT DISTINCT ON (version,dataownercode,lineplanningnumber,journeypatterncode)
                                             version,dataownercode,lineplanningnumber,journeypatterncode,productformulatype FROM jopatili
                                             ORDER BY version,dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder) as pattern
                        USING(version,dataownercode,lineplanningnumber,journeypatterncode)
              LEFT JOIN line using (version,dataownercode,lineplanningnumber)
ORDER BY version, dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber,wheelchairaccessible ASC,stoporder
""")
    journeys = []
    for row in cur.fetchall():
        row.update(timedemandGroupRefForJourney[row['operator_id']])
        journeys.append(row)
    cur.close()
    return journeys

def getJourneyPatterns(routeRefForPattern,conn,routes):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    journeypatterns = {}
    cur.execute("""
SELECT
jopa.dataownercode||':'||lineplanningnumber||':'||journeypatterncode as operator_id,
NULL as routeref,
direction as directiontype,
destinationdisplayref
FROM jopa left join ( SELECT DISTINCT ON (version, dataownercode, lineplanningnumber, journeypatterncode)
			version, dataownercode, lineplanningnumber, journeypatterncode,dataownercode||':'||destcode as destinationdisplayref
			FROM jopatili
			ORDER BY version, dataownercode, lineplanningnumber, journeypatterncode,timinglinkorder ) as jopatili 
                    USING (version, dataownercode, lineplanningnumber, journeypatterncode)""")
    for row in cur.fetchall():
        journeypatterns[row['operator_id']] = row
        journeypatterns[row['operator_id']]['POINTS'] = []
        row['routeref'] = routeRefForPattern[row['operator_id']]
    cur.execute("""
SELECT
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
0 as fareunitspassed
FROM jopatili as j  LEFT JOIN dest USING (version,destcode) LEFT JOIN usrstop as u ON (u.version = j.version AND u.userstopcode = 
j.userstopcodebegin)
UNION (
SELECT DISTINCT ON (j.version,j.dataownercode,lineplanningnumber,journeypatterncode)
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
0 as fareunitspassed
FROM jopatili as j LEFT JOIN usrstop as u ON (u.version = j.version AND u.userstopcode = j.userstopcodeend)
ORDER BY j.version,j.dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder DESC)
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

def getAdministrativeZones(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT
dataownercode||':'||confinrelcode as operator_id,
description as name
FROM 
confinrel LEFT JOIN conarea using (dataownercode,concessionareacode)
""")
    administrativezones = {}
    for row in cur.fetchall():
        administrativezones[row['operator_id']] = row
    cur.close()
    return administrativezones

def getLines(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    lines = {}
    cur.execute("""
SELECT
CASE WHEN (dataownercode = 'ARR' and lineplanningnumber like '15___') THEN 'WATERBUS'
     WHEN (dataownercode = 'HTM' and transporttype = 'BUS' and cast(lineplanningnumber as integer) <= 42) THEN 'HTMBUZZ'
     WHEN (dataownercode = 'CXX' and substring(line.lineplanningnumber,1,1) = 'U')           THEN 'GVU'
     WHEN (dataownercode = 'CXX' and substring(line.lineplanningnumber,1,1) IN ('A','X'))    THEN 'BRENG'
     WHEN (dataownercode = 'CXX' and substring(line.lineplanningnumber,1,1) = 'L')           THEN 'HERMES'
     ELSE dataownercode END as operatorref, 
dataownercode||':'||lineplanningnumber as operator_id,
lineplanningnumber as privatecode,
linepublicnumber as publiccode,
transporttype as TransportMode,
CASE WHEN (linepublicnumber <> linename) THEN linename ELSE null END as name
FROM
line
""")
    for row in cur.fetchall():
        lines[row['operator_id']] = row
    cur.close()
    return lines

def load(path,filename):
    zip = zipfile.ZipFile(path+'/'+filename,'r')
    if 'Csv.zip' in zip.namelist():
        zipfile.ZipFile.extract(zip,'Csv.zip','/tmp')
        zip = zipfile.ZipFile('/tmp/Csv.zip','r')
    conn = psycopg2.connect(kv1_database_connect)
    cur =  conn.cursor()
    cur.execute(schema)
    meta = importzip(conn,zip)
    return (meta,conn)
