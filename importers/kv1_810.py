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
SELECT DISTINCT ON (dataownercode,lineplanningnumber,journeypatterncode,timedemandgroupcode)
concat_ws(':',dataownercode,lineplanningnumber,journeypatterncode,timedemandgroupcode) as operator_id,
concat_ws(':',dataownercode,lineplanningnumber,journeypatterncode,timedemandgroupcode) as privatecode,
cast(timinglinkorder+1 as integer) as pointorder,
NULL as drivetime,
NULL as stopwaittime
FROM timdemrnt
ORDER BY dataownercode,lineplanningnumber,journeypatterncode,timedemandgroupcode,pointorder DESC)) as x
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
    for key,value in timedemandgroups.items():
        m = md5.new()
        m.update(str(value['POINTS']))
        value['operator_id'] = m.hexdigest()
    return timedemandgroups

def getAvailabilityConditionsFromCalendars(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    availabilityConditions = {}
    cur.execute("""
SELECT
concat_ws(':',dataownercode, organizationalunitcode,timetableversioncode,periodgroupcode,specificdaycode,daytype) as operator_id,
concat_ws(':',dataownercode, organizationalunitcode,timetableversioncode,periodgroupcode,specificdaycode,daytype) as privatecode,
dataownercode||':'||organizationalunitcode as unitcode,
'1' as versionref,
null as name,
cast(min(coalesce(t.validfrom,pg.validfrom)) as text) as fromdate,
cast(max(coalesce(t.validthru,pg.validthru)) as text) as todate
FROM pegrval as pg JOIN tive as t USING (dataownercode, organizationalunitcode, periodgroupcode)
                   JOIN pujo as pu USING (dataownercode, organizationalunitcode, timetableversioncode, periodgroupcode, specificdaycode)
GROUP BY dataownercode, organizationalunitcode,timetableversioncode,periodgroupcode,specificdaycode,daytype
""")
    for row in cur.fetchall():
        availabilityConditions[row['operator_id']] = row
    cur.execute("""
SELECT 
concat_ws(':',dataownercode, organizationalunitcode,timetableversioncode,periodgroupcode,specificdaycode,pujo.daytype) as availabilityconditionref,
array_agg(DISTINCT validdate ORDER BY validdate) as validdates,
true as isavailable
FROM (SELECT DISTINCT dataownercode, organizationalunitcode,timetableversioncode,periodgroupcode,specificdaycode,daytype FROM pujo) as pujo 
JOIN (
SELECT 
c.dataownercode,c.organizationalunitcode,c.periodgroupcode,timetableversioncode,c.specificdaycode,c.validdate,coalesce(daytypeason,extract(isodow 
from c.validdate)) as daytype
FROM 
(SELECT 
dataownercode,organizationalunitcode,periodgroupcode,timetableversioncode,specificdaycode,
generate_series(coalesce(pv.validfrom,tv.validfrom),coalesce(pv.validthru,tv.validthru),interval '1 day')::date as validdate
FROM tive AS tv JOIN pegrval AS pv USING (dataownercode,organizationalunitcode,periodgroupcode)) as c
LEFT JOIN excopday as ex ON (ex.dataownercode = c.dataownercode AND ex.specificdaycode = c.specificdaycode AND 
c.validdate::date = ex.validdate::date)
ORDER BY c.validdate desc) as calendar USING (dataownercode,organizationalunitcode,periodgroupcode,timetableversioncode,specificdaycode)
WHERE position(calendar.daytype::integer::text in pujo.daytype) != 0
GROUP BY availabilityconditionref
;""")
    for row in cur.fetchall():
        availabilityConditions[row['availabilityconditionref']]['DAYS'] = row
    cur.close()
    return availabilityConditions

def getJourneysFromPujo(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT
concat_ws(':',dataownercode,lineplanningnumber,journeynumber) as privatecode,
concat_ws(':',dataownercode, organizationalunitcode,timetableversioncode,periodgroupcode,specificdaycode,daytype, lineplanningnumber, journeynumber) as operator_id,
concat_ws(':',dataownercode, organizationalunitcode,timetableversioncode,periodgroupcode,specificdaycode,daytype) as availabilityconditionRef,
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
productformulatype in (2,8,35,36) as onDemand,
((dataownerisoperator = false and lineplanningnumber not in ('X058')) OR transporttype = 'TRAIN') as isvirtual
FROM pujo LEFT JOIN (SELECT DISTINCT ON (dataownercode,lineplanningnumber,journeypatterncode)
                                             dataownercode,lineplanningnumber,journeypatterncode,productformulatype FROM jopatili
                                             ORDER BY dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder) as pattern
                        USING(dataownercode,lineplanningnumber,journeypatterncode)
              LEFT JOIN line using (dataownercode,lineplanningnumber)
ORDER BY dataownercode, organizationalunitcode,timetableversioncode,periodgroupcode,specificdaycode,daytype, lineplanningnumber, journeynumber
""")
    journeys = {}
    for journey in cur.fetchall():
        journeys[journey['operator_id']] = journey
    cur.close()
    return journeys


def getJourneys(timedemandGroupRefForJourney,conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT DISTINCT ON (dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber)
concat_ws(':',dataownercode,lineplanningnumber,journeynumber) as privatecode,
concat_ws(':',dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber) as operator_id,
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
productformulatype in (2,8,35,36) as onDemand,
(dataownercode != 'ARR' and dataownerisoperator = false) as isvirtual
FROM pujopass LEFT JOIN (SELECT DISTINCT ON (dataownercode,lineplanningnumber,journeypatterncode)
                                             dataownercode,lineplanningnumber,journeypatterncode,productformulatype FROM jopatili
                                             ORDER BY dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder) as pattern
                        USING(dataownercode,lineplanningnumber,journeypatterncode)
              LEFT JOIN line using (dataownercode,lineplanningnumber)
ORDER BY dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber,wheelchairaccessible ASC,stoporder
""")
    journeys = {}
    for row in cur.fetchall():
        row.update(timedemandGroupRefForJourney[row['operator_id']])
        journeys[row['operator_id']] = row
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
FROM jopa left join ( SELECT DISTINCT ON ( dataownercode, lineplanningnumber, journeypatterncode)
			 dataownercode, lineplanningnumber, journeypatterncode,dataownercode||':'||destcode as destinationdisplayref
			FROM jopatili
			ORDER BY dataownercode, lineplanningnumber, journeypatterncode,timinglinkorder ) as jopatili 
                    USING (dataownercode, lineplanningnumber, journeypatterncode)""")
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
     when (destnamefull ilike '%alleen uitstappen%') THEN false 
     ELSE getin END as forboarding,
0 as distancefromstartroute,
coalesce(sum(distance) OVER (PARTITION BY j.dataownercode,lineplanningnumber,journeypatterncode
                                        ORDER BY j.dataownercode, lineplanningnumber, journeypatterncode, timinglinkorder
                                        ROWS between UNBOUNDED PRECEDING and 1 PRECEDING),0) as fareunitspassed
FROM jopatili as j LEFT JOIN link as l using (dataownercode,userstopcodebegin,userstopcodeend)
                   LEFT JOIN dest USING (destcode) LEFT JOIN usrstop as u ON (u.userstopcode = j.userstopcodebegin)
UNION (
SELECT DISTINCT ON (j.dataownercode,lineplanningnumber,journeypatterncode)
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
sum(distance) OVER (PARTITION BY j.dataownercode,lineplanningnumber,journeypatterncode) as fareunitspassed
FROM jopatili as j LEFT JOIN link as l using (dataownercode,userstopcodebegin,userstopcodeend)
                   LEFT JOIN usrstop as u ON (u.userstopcode = j.userstopcodeend)
ORDER BY j.dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder DESC)
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

def getLineWithGeneratedNames(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    lines = getLines(conn)
    cur.execute("""
SELECT DISTINCT ON (operator_id) 
operator_id,
CASE WHEN (dataownercode = 'ARR' and lineplanningnumber like '15___') THEN 'WATERBUS'
     WHEN (dataownercode = 'HTM' and transportmode = 'BUS' and cast(lineplanningnumber as integer) <= 42) THEN 'HTMBUZZ'
     WHEN (dataownercode = 'CXX' and lineplanningnumber in ('X058'))                         THEN 'NIAG'
     WHEN (dataownercode = 'CXX' and substring(lineplanningnumber,1,1) = 'U')           THEN 'GVU'
     WHEN (dataownercode = 'CXX' and substring(lineplanningnumber,1,1) IN ('A','X'))    THEN 'BRENG'
     WHEN (dataownercode = 'CXX' and substring(lineplanningnumber,1,1) = 'L')           THEN 'HERMES'
     WHEN (dataownercode = 'SYNTUS' and description like 'TW%')           THEN 'TWENTS'
     ELSE dataownercode END as operatorref, 
privatecode,publiccode,TransportMode,
CASE WHEN (publiccode != linename) THEN linename ELSE name END as name
FROM 
((SELECT
u.dataownercode,l.lineplanningnumber,l.description,
u.dataownercode||':'||l.lineplanningnumber as operator_id,
l.lineplanningnumber as privatecode,
linepublicnumber as publiccode,
transporttype as TransportMode,
replace( CASE WHEN (terug.destnamemain is null) THEN concat_ws(' - ',u.name,dest_heen.destnamefull)
     ELSE concat_ws(' - ',dest_heen.destnamefull,terug.destnamemain) END,linepublicnumber||' ','') as name,
1 as priority,linename
 FROM 
 ((SELECT DISTINCT ON (dataownercode,lineplanningnumber) * FROM (
  SELECT dataownercode,lineplanningnumber,journeypatterncode,count((dataownercode, organizationalunitcode, schedulecode, 
scheduletypecode, lineplanningnumber, journeynumber)) as journeycount
  FROM
    (SELECT DISTINCT ON (dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber)
      * FROM pujopass JOIN jopa USING (dataownercode,lineplanningnumber,journeypatterncode)
        WHERE direction = 1
        ORDER BY dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber ASC) as schedpujoh
    JOIN operday USING (dataownercode,organizationalunitcode, schedulecode, scheduletypecode)
    GROUP BY dataownercode,lineplanningnumber,journeypatterncode) AS journeypatterns_heen
  ORDER BY dataownercode,lineplanningnumber,journeycount DESC) as heen_distribution
  JOIN (SELECT DISTINCT ON (dataownercode,lineplanningnumber,journeypatterncode) *
        FROM JOPATILI
        ORDER BY dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder ASC) as jopatiliheen
  JOIN dest as dest_heen USING (dataownercode,destcode)
  USING (dataownercode,lineplanningnumber,journeypatterncode))
LEFT JOIN 
 ((SELECT DISTINCT ON (dataownercode,lineplanningnumber) * FROM (
  SELECT dataownercode,lineplanningnumber,journeypatterncode,count((dataownercode, organizationalunitcode, schedulecode, 
scheduletypecode, lineplanningnumber, journeynumber)) as journeycount
  FROM
    (SELECT DISTINCT ON (dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber)
      * FROM pujopass JOIN jopa USING (dataownercode,lineplanningnumber,journeypatterncode)
        WHERE direction = 2
        ORDER BY dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber ASC) as schedpujot
    JOIN operday USING (dataownercode,organizationalunitcode, schedulecode, scheduletypecode)
    GROUP BY dataownercode,lineplanningnumber,journeypatterncode) AS journeypatterns_terug
  ORDER BY dataownercode,lineplanningnumber,journeycount DESC) as terug_distribution
  JOIN (SELECT DISTINCT ON (dataownercode,lineplanningnumber,journeypatterncode) *
        FROM JOPATILI
        ORDER BY dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder ASC) as jopatiliterug
  JOIN dest as dest_terug USING (dataownercode,destcode)
  USING (dataownercode,lineplanningnumber,journeypatterncode)) as terug USING (dataownercode,lineplanningnumber)
  JOIN (SELECT dataownercode,userstopcode,name from usrstop) as u ON (u.dataownercode = jopatiliheen.dataownercode AND jopatiliheen.userstopcodebegin = u.userstopcode)
  JOIN line as l ON (l.dataownercode = u.dataownercode AND l.lineplanningnumber = jopatiliheen.lineplanningnumber))
UNION 
(SELECT
u.dataownercode,l.lineplanningnumber,l.description,
u.dataownercode||':'||l.lineplanningnumber as operator_id,
l.lineplanningnumber as privatecode,
linepublicnumber as publiccode,
transporttype as TransportMode,
replace( CASE WHEN (terug.destnamemain is null) THEN concat_ws(' - ',u.name,dest_heen.destnamefull)
     ELSE concat_ws(' - ',dest_heen.destnamefull,terug.destnamemain) END,linepublicnumber||' ','') as name,
2 as priority,linename
 FROM 
 ((SELECT DISTINCT ON (dataownercode,lineplanningnumber) * FROM (
  SELECT dataownercode,lineplanningnumber,journeypatterncode,count((dataownercode, organizationalunitcode, schedulecode, 
scheduletypecode, lineplanningnumber, journeynumber)) as journeycount
  FROM
    (SELECT DISTINCT ON (dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber)
      * FROM pujopass JOIN jopa USING (dataownercode,lineplanningnumber,journeypatterncode)
        WHERE direction = 2
        ORDER BY dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber ASC) as schedpujoh
    JOIN operday USING (dataownercode,organizationalunitcode, schedulecode, scheduletypecode)
    GROUP BY dataownercode,lineplanningnumber,journeypatterncode) AS journeypatterns_heen
  ORDER BY dataownercode,lineplanningnumber,journeycount DESC) as heen_distribution
  JOIN (SELECT DISTINCT ON (dataownercode,lineplanningnumber,journeypatterncode) *
        FROM JOPATILI
        ORDER BY dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder ASC) as jopatiliheen
  JOIN dest as dest_heen USING (dataownercode,destcode)
  USING (dataownercode,lineplanningnumber,journeypatterncode))
LEFT JOIN 
 ((SELECT DISTINCT ON (dataownercode,lineplanningnumber) * FROM (
  SELECT dataownercode,lineplanningnumber,journeypatterncode,count((dataownercode, organizationalunitcode, schedulecode, 
scheduletypecode, lineplanningnumber, journeynumber)) as journeycount
  FROM
    (SELECT DISTINCT ON (dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber)
      * FROM pujopass JOIN jopa USING (dataownercode,lineplanningnumber,journeypatterncode)
        WHERE direction = 1
        ORDER BY dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber ASC) as schedpujot
    JOIN operday USING (dataownercode,organizationalunitcode, schedulecode, scheduletypecode)
    GROUP BY dataownercode,lineplanningnumber,journeypatterncode) AS journeypatterns_terug
  ORDER BY dataownercode,lineplanningnumber,journeycount DESC) as terug_distribution
  JOIN (SELECT DISTINCT ON (dataownercode,lineplanningnumber,journeypatterncode) *
        FROM JOPATILI
        ORDER BY dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder ASC) as jopatiliterug
  JOIN dest as dest_terug USING (dataownercode,destcode)
  USING (dataownercode,lineplanningnumber,journeypatterncode)) as terug USING (dataownercode,lineplanningnumber)
  JOIN (SELECT dataownercode,userstopcode,name from usrstop) as u ON (u.dataownercode = jopatiliheen.dataownercode AND jopatiliheen.userstopcodebegin = u.userstopcode)
  JOIN line as l ON (l.dataownercode = u.dataownercode AND l.lineplanningnumber = jopatiliheen.lineplanningnumber))) as x
ORDER BY operator_id,priority
""")
    for row in cur.fetchall():
        lines[row['operator_id']] = row
    cur.close()
    return lines

def getLines(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    lines = {}
    cur.execute("""
SELECT
CASE WHEN (dataownercode = 'ARR' and lineplanningnumber like '15___') THEN 'WATERBUS'
     WHEN (dataownercode = 'ARR' and lineplanningnumber in ('18019')) THEN 'WATERBUS'
     WHEN (dataownercode = 'ARR' and lineplanningnumber in ('18017','18018')) THEN 'AQUALINER'
     WHEN (dataownercode = 'HTM' and transporttype = 'BUS' and cast(lineplanningnumber as integer) <= 42) THEN 'HTMBUZZ'
     WHEN (dataownercode = 'CXX' and lineplanningnumber in ('X058'))                         THEN 'NIAG'
     WHEN (dataownercode = 'CXX' and substring(line.lineplanningnumber,1,1) = 'U')           THEN 'GVU'
     WHEN (dataownercode = 'CXX' and substring(line.lineplanningnumber,1,1) IN ('A','X'))    THEN 'BRENG'
     WHEN (dataownercode = 'CXX' and substring(line.lineplanningnumber,1,1) = 'L')           THEN 'HERMES'
     WHEN (dataownercode = 'CXX' and line.lineplanningnumber in (SELECT DISTINCT lineplanningnumber
                                                                 FROM jopatili JOIN confinrel USING (dataownercode,confinrelcode)
                                                                 JOIN conarea USING (dataownercode,concessionareacode)
                                                                 WHERE concessionareacode in ('FLE-YSS','FL-YS-2014','OV-YS-2014','OSL-YSS')))
                                                                                             THEN 'OVREGIOY'
     WHEN (dataownercode = 'QBUZZ' and substring(line.lineplanningnumber,1,1) = 'u')         THEN 'UOV'
     WHEN (dataownercode = 'SYNTUS' and description like 'TW%')           THEN 'TWENTS'
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

def load(path,filename,point_from_pool=False):
    zip = zipfile.ZipFile(path+'/'+filename,'r')
    path_parts = zip.namelist()[0].split('/')
    validfrom = None
    if len(path_parts) > 0 and len(path_parts[0].split('-')) == 3:
        validfrom = path_parts[0]
    if 'Csv.zip' in zip.namelist():
        zipfile.ZipFile.extract(zip,'Csv.zip','/tmp')
        zip = zipfile.ZipFile('/tmp/Csv.zip','r')
    conn = psycopg2.connect(kv1_database_connect)
    cur =  conn.cursor()
    cur.execute(schema)
    meta = importzip(conn,zip)
    if validfrom is not None:
        meta['validfrom'] = validfrom
    if point_from_pool:
        fix_pool(conn)
    checkUsrstopPoint(conn)
    return (meta,conn)
