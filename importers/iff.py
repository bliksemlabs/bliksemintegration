import psycopg2
import psycopg2.extras
import md5
from copy import deepcopy

cache = {}

def getFakePool(conn,stopbegin,stopend):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT
%s as privatecode,
1 as pointorder,
latitude,
longitude,
0 as distancefromstart
FROM quays
WHERE id = %s
UNION 
SELECT
%s as privatecode,
2 as pointorder,
latitude,
longitude,
200 as distancefromstart
FROM quays
WHERE id = %s
""",[stopbegin,stopend]*2)
    return cur.fetchall()
    
def getPoolIFF(conn,lineplanningnumber,stopcodebegin,stopcodeend):
    print (lineplanningnumber,stopcodebegin,stopcodeend,'iff')
    if len(lineplanningnumber.split(':')) > 3:
        getFakePool(conn,stopcodebegin,stopcodeend)
    key = ':'.join([stopcodebegin,stopcodeend])
    if key in cache:
        print 'hit'
        return deepcopy(cache[key])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT 
NULL as privatecode,
p1.idx as pointorder,
cast(CAST(ST_Y(p1.the_geom) AS NUMERIC(8,5)) as text) AS latitude,
cast(CAST(ST_X(p1.the_geom) AS NUMERIC(7,5)) as text) AS longitude,
coalesce(SUM  (st_distance(st_transform(p1.the_geom,28992),st_transform(p2.the_geom,28992))::integer)
         OVER (partition by p1.stopbegin,p1.stopend order by p1.idx ROWS between UNBOUNDED PRECEDING and 1 PRECEDING),0) as distancefromstart
FROM poolpoints as p1 LEFT JOIN poolpoints as p2 ON (p1.stopbegin = p2.stopbegin AND p1.stopend = p2.stopend AND p1.idx +1 = p2.idx)
WHERE p1.stopbegin = %s and p1.stopend = %s
ORDER BY pointorder
""",[stopcodebegin,stopcodeend])
    try:
        cache[key] = cur.fetchall()
        if len(cache[key]) < 2:
            cache[key] = getFakePool(conn,stopcodebegin,stopcodeend)
        else:
            cache[key][0]['privatecode'] = stopcodebegin
            cache[key][-1]['privatecode'] = stopcodebegin
        return deepcopy(cache[key])
    finally:
        cur.close()

def calculateTimeDemandGroups(conn):
    cur = conn.cursor('timdemgrps',cursor_factory=psycopg2.extras.RealDictCursor)
    timdemgroup_ids = {}
    timdemgroups = {}
    journeyinfo = {}
    cur.execute("""
SELECT concat_ws(':','IFF',serviceid,line_id,footnote,coalesce(variant,servicenumber)) as JOURNEY_id, 
array_agg(cast(stoporder as integer) order by stoporder) as stoporders,array_agg(toseconds(coalesce(arrivaltime,departuretime),0) order by stoporder) as 
arrivaltimes,array_agg(toseconds(coalesce(departuretime,arrivaltime),0) order by stoporder) as departuretimes
FROM passtimes
GROUP BY JOURNEY_id
""")
    for row in cur:
        points = [(row['stoporders'][0],0,0)]
        dep_time = row['departuretimes'][0]
        for i in range(len(row['stoporders'][:-1])):     
            cur_arr_time = row['arrivaltimes'][i+1]
            cur_dep_time = row['departuretimes'][i+1]
            points.append((row['stoporders'][i+1],cur_arr_time-dep_time,cur_dep_time-cur_arr_time))
        m = md5.new()
        m.update(str(points))
        timdemgrp = {'POINTS' : []}
        for point in points:
            point_dict = {'pointorder' : point[0],'totaldrivetime' : point[1], 'stopwaittime' : point[2]}
            timdemgrp['POINTS'].append(point_dict)
        journeyinfo[row['journey_id']] = {'departuretime' : dep_time, 'timedemandgroupref' : m.hexdigest()}
        timdemgrp['operator_id'] = m.hexdigest()
        timdemgroups[m.hexdigest()] = timdemgrp
    cur.close()
    return (journeyinfo,timdemgroups)

def getStopPoints(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    userstops = {}
    cur.execute("""
SELECT
'IFF:'||stopid as operator_id,
stopid as privatecode,
'IFF:'||station as stoparearef,
name,
NULL as town,
(trainchanges != 2) as isscheduled,
coalesce(latitude::text,ST_Y(the_geom)::NUMERIC(9,6)::text) AS latitude,
coalesce(longitude::text,ST_X(the_geom)::NUMERIC(8,6)::text) AS longitude,
x as rd_x,
y as rd_y,
platform as platformcode
FROM 
(SELECT DISTINCT ON (station,platform) station,platform,station||':'||coalesce(platform,'0') as stopid FROM passtimes) as stations
          LEFT JOIN (select country,shortname as station,trainchanges,name,x,y,st_transform(st_setsrid(st_makepoint(x,y),28992),4326) as the_geom 
from station) as station USING (station)
          LEFT JOIN quays ON (stopid = quays.id)                          
""")
    for row in cur.fetchall():
        userstops[row['operator_id']] = row
    cur.close()
    return userstops

def getStopAreas(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    stopareas = {}
    cur.execute("""
SELECT
'IFF:'||station as operator_id,
station as privatecode,
station as publiccode,
name,
NULL as town,
coalesce(latitude::text,ST_Y(the_geom)::NUMERIC(9,6)::text) AS latitude,
coalesce(longitude::text,ST_X(the_geom)::NUMERIC(8,6)::text) AS longitude,
CASE WHEN (country = 'GB') THEN 'Europe/London' ELSE 'Europe/Amsterdam' END as timezone
FROM 
(SELECT DISTINCT ON (station) station FROM passtimes) as stations
          LEFT JOIN (select country,shortname as station,trainchanges,name,x,y,st_transform(st_setsrid(st_makepoint(x,y),28992),4326) as the_geom from station) as station USING (station)
          LEFT JOIN places ON (places.id = station)
""")
    for row in cur.fetchall():
        stopareas[row['operator_id']] = row
    cur.close()
    return stopareas

def getAvailabilityConditions(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    availabilityconditions = {}
    cur.execute("""
SELECT 
'IFF:'||versionnumber||':'||footnote as operator_id,
'IFF:'||versionnumber||':'||footnote as privatecode,
'IFF' as unitcode,
'1' as versionref,
NULL as name,
min(servicedate)::text as fromdate,
max(servicedate)::text as todate
FROM footnote,delivery
GROUP BY versionnumber,footnote
ORDER BY versionnumber,footnote
;
""")
    for row in cur.fetchall():
        availabilityconditions[row['operator_id']] = row
    cur.execute("""
SELECT 
'IFF:'||versionnumber||':'||footnote as availabilityconditionRef,
array_agg(servicedate::text) as validdates,
true as isavailable
FROM footnote,delivery
GROUP BY versionnumber,footnote
ORDER BY versionnumber,footnote
;
""")
    for row in cur.fetchall():
        availabilityconditions[row['availabilityconditionref']]['DAYS'] = row
    cur.close()
    return availabilityconditions

def getProductCategories(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    productcategories = {}
    cur.execute("""
SELECT 
'IFF:'||code as operator_id,
code as privatecode,
description as name
FROM trnsmode
WHERE code in (select distinct transmode from timetable_transport);
""")
    for row in cur.fetchall():
        productcategories[row['operator_id']] = row
    cur.close()
    return productcategories

def getDestinationDisplays(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    destinationdisplays = {}
    cur.execute("""
SELECT DISTINCT ON (shortname)
shortname as privatecode,
'IFF:'||shortname as operator_id,
name,
name as shortname
FROM (
SELECT DISTINCT ON (serviceid,patterncode,line_id)
serviceid,servicenumber,station as shortname
FROM passtimes
ORDER BY serviceid ASC,patterncode ASC,line_id ASC,stoporder DESC) as x LEFT JOIN station USING (shortname);
""")
    for row in cur.fetchall():
        destinationdisplays[row['operator_id']] = row
    cur.close()
    return destinationdisplays

def clusterPatternsIntoRoute(conn,getPool):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT 
line_id,array_agg(patterncode ORDER BY char_length(pattern) DESC,patterncode) as 
patterncodes,array_agg(pattern ORDER BY char_length(pattern) DESC,patterncode) as patterns
FROM
(SELECT line_id,'IFF:'||line_id||':'||patterncode as patterncode,string_agg(station||':'||coalesce(platform,'0'),'>') as pattern 
FROM (SELECT DISTINCT ON (line_id,patterncode,stoporder) * From passtimes order by line_id,patterncode,stoporder) as passtimes
GROUP BY line_id,patterncode) as y
GROUP BY line_id""")
    rows = cur.fetchall()
    patterncodeInRoute = {}
    for row in rows:
        if row['line_id'] not in patterncodeInRoute:
            patterncodeInRoute[row['line_id']] = [ (row['patterns'][0],[row['patterncodes'][0]]) ]
        for i in range(len(row['patterncodes'][1:])):
            pattern = row['patterns'][i+1]
            patterncode = row['patterncodes'][i+1]
            route_found = False
            for route in patterncodeInRoute[row['line_id']]:
                if pattern in route[0]:
                    route[1].append(patterncode)
                    route_found = True
                    break
            if not route_found:
                patterncodeInRoute[row['line_id']].append((pattern,[patterncode]))
    routes_result = {}
    routeRefForPattern = {}
    linecounter = 0
    for line,routes in patterncodeInRoute.items():
        linecounter += 1
        print '%s / %s' % (linecounter,len(patterncodeInRoute))
        for routes in routes:
            result = {'POINTS' : []}
            stops = routes[0].split('>')
            for i in range(len(stops)-1):
                stopbegin = stops[i]
                stopend = stops[i+1]
                if len(result['POINTS']) == 0:
                    order = 0
                    distance = 0
                else:
                    order = result['POINTS'][-1]['pointorder']
                    distance = result['POINTS'][-1]['distancefromstart']
                pool = getPool(conn,line,stopbegin,stopend)
                if len(pool) == 0:
                    raise Exception('KV1: Pool empty')
                if pool[0]['privatecode'] != stopbegin and len(result['POINTS']) == 0:
                    pointbegin = deepcopy(pool[0])
                    pointbegin['privatecode'] = stopbegin
                    order += 1
                    result['POINTS'].append(pointbegin)
                if pool[-1]['privatecode'] != stopend:
                    pointend = deepcopy(pool[-1])
                    pointend['privatecode'] = stopend
                    pointend['pointorder'] += 1
                    pool.append(pointend)
                for point in pool:
                    if len(result['POINTS']) > 0 and point['privatecode'] is not None and result['POINTS'][-1]['privatecode'] == point['privatecode']:
                        continue
                    point['pointorder'] += order
                    point['distancefromstart'] += distance 
                    result['POINTS'].append(point)
            m = md5.new()
            if len(result['POINTS']) < 2:
                raise Exception('Routepoints empty %s\n\n%s\n\n%s' % (line,stops,routes))
            result['lineref'] = line
            m.update(str(result))
            result['operator_id'] = m.hexdigest()
            routes_result[m.hexdigest()] = result
            for patterncode in routes[1]:
                routeRefForPattern[patterncode] = m.hexdigest()
    cur.close()
    return (routeRefForPattern,routes_result)


def getJourneyPatterns(routeRefForPattern,conn,routes):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    journeypatterns = {}
    cur.execute("""
SELECT
'IFF:'||line_id||':'||patterncode as operator_id,
NULL as routeref,
CASE WHEN (COALESCE(variant,servicenumber) != 0) THEN ((not COALESCE(variant,servicenumber)%2 = 1)::int4 + 1)
                                                  ELSE (stops[array_upper(stops,1)] < stops[array_lower(stops,1)])::int4 + 1 END as directiontype,
'IFF:'||stops[array_upper(stops,1)] as destinationdisplayref
FROM (
SELECT DISTINCT ON (line_id,patterncode) line_id,patterncode,servicenumber,variant,stops
FROM
(SELECT line_id,patterncode,array_agg(station ORDER BY stoporder) as stops FROM
   (SELECT DISTINCT ON (line_id,patterncode,stoporder) * From passtimes order by line_id,patterncode,stoporder) as passtimes
 GROUP BY line_id,patterncode) as x JOIN passtimes as p USING (line_id,patterncode) 
ORDER BY line_id,patterncode,stoporder ASC) as y
""")
    for row in cur.fetchall():
        journeypatterns[row['operator_id']] = row
        journeypatterns[row['operator_id']]['POINTS'] = []
        row['routeref'] = routeRefForPattern[row['operator_id']]
    cur.execute("""
SELECT DISTINCT ON (p1.line_id,p1.patterncode,p1.stoporder)
'IFF:'||p1.line_id||':'||p1.patterncode as journeypatternref,
p1.stoporder::integer as pointorder,
NULL as privatecode,
NULL as operator_id,
'IFF:'||p1.station||':'||coalesce(p1.platform,'0') as pointref,
'IFF:'||p2.station||':'||coalesce(p2.platform,'0') as onwardpointref,
NULL as destinationdisplayref,
NULL as noticeassignmentRef,
NULL as administrativezoneref,
p1.forboarding as iswaitpoint,
0 as waittime,
NULL as requeststop,
coalesce(p1.foralighting,true) as foralighting,
coalesce(p1.forboarding,true) as forboarding,
0 as distancefromstartroute,
0 as fareunitspassed
FROM passtimes as p1 LEFT JOIN passtimes as p2 ON (p1.serviceid = p2.serviceid AND 
                                                  (p1.servicenumber = 0 OR p1.servicenumber = p2.servicenumber OR p1.variant = p2.variant)AND
                                                  p1.stoporder +1 = p2.stoporder)
ORDER BY p1.line_id,p1.patterncode,p1.stoporder ASC
""")
    distance = 0
    patternref = None
    for row in cur.fetchall():
        if row['journeypatternref'] != patternref:
            distance = 0
            patternref = row['journeypatternref']
        for point in routes[journeypatterns[row['journeypatternref']]['routeref']]['POINTS']:
            if point['distancefromstart'] >= distance and point['privatecode'] is not None and 'IFF:'+point['privatecode'] == row['pointref']:
                distance = point['distancefromstart']
                row['distancefromstartroute'] = distance
                break
        if distance == 0 and int(row['pointorder']) > 3:
            raise Exception('distancefromstartroute going wrong'+str(journeypatterns[row['journeypatternref']]['POINTS']))
        row['distancefromstartroute'] = distance
        journeypatterns[row['journeypatternref']]['POINTS'].append(row)
    cur.close()
    return journeypatterns

def getVersion(conn,filename):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT '1' as datasourceref,firstday as startdate, lastday as enddate, versionnumber as privatecode,description,%s as operator_id
FROM delivery LIMIT 1;""",[filename])
    return cur.fetchone()

def getOperator(conn):
    operators = {'THALYS'  : {'url' : 'http://www.thalys.nl',      'language' : 'nl', 'phone' : '0900-9296'},
                 'DB'      : {'url' : 'http://www.db.de'    ,      'language' : 'de', 'phone' : '+491806996633'},
                 'KEO'     : {'url' : 'http://www.keolis.de',      'language' : 'de', 'phone' : '+4918029273727'},
                 'SYN'     : {'url' : 'http://www.syntus.nl',      'language' : 'nl', 'phone' : '0314350111'},
                 'GVB'     : {'url' : 'http://www.gvb.nl',         'language' : 'nl', 'phone' : '0900-8011'},
                 'NMBS'    : {'url' : 'http://www.nmbs.be',        'language' : 'nl', 'phone' : '+3225282828'},
                 'GVU'     : {'url' : 'http://www.gvu.nl',         'language' : 'nl', 'phone' : '0900-8998959'},
                 'EUROSTAR': {'url' : 'http://www.eurostar.co.uk', 'language' : 'nl', 'phone' : '0900-9296'},
                 'BRENG'   : {'url' : 'http://www.breng.nl',       'language' : 'nl', 'phone' : '026-2142140'},
                 'VEO'     : {'url' : 'http://www.veolia.nl',      'language' : 'nl', 'phone' : '088-0761111'},
                 'VALLEI'  : {'url' : 'http://www.valleilijn.nl',  'language' : 'nl', 'phone' : '0900-2666399'},
                 'HISPEED' : {'url' : 'http://www.nshispeed.nl',   'language' : 'nl', 'phone' : '0900-9296'},
                 'SNCF'    : {'url' : 'http://www.sncf.fr',        'language' : 'fr', 'phone' : '+33890640650'},
                 'CONNEXXI': {'url' : 'http://www.connexxion.nl',  'language' : 'nl', 'phone' : '0900-2666399'},
                 'RNET'    : {'url' : 'http://www.connexxion.nl',  'language' : 'nl', 'phone' : '0900-2666399'},
                 'NS'      : {'url' : 'http://www.ns.nl',          'language' : 'nl', 'phone' : '0900-2021163'},
                 'ARRIVA'  : {'url' : 'http://www.arriva.nl',      'language' : 'nl', 'phone' : '0900-2022022'},
                 'NOORD'   : {'url' : 'http://www.arriva.nl',      'language' : 'nl', 'phone' : '0900-2022022'}
                }
    result = {}
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(""" 
select upper(code) as privatecode,
'IFF:'||upper(code) as operator_id,
name,
'Europe/Amsterdam' as timezone
FROM company where company in (select distinct companynumber from timetable_service);""")
    rows = cur.fetchall()
    cur.close()
    for row in rows:
        result[row['operator_id']] = operators[row['privatecode']]
        result[row['operator_id']].update(row)
    return result

def getJourneys(timedemandGroupRefForJourney,conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT DISTINCT ON (serviceid,line_id,patterncode,servicenumber)
concat_ws(':','IFF',coalesce(variant,servicenumber)) as privatecode,
concat_ws(':','IFF',serviceid,line_id,v.footnote,coalesce(variant,servicenumber)) as operator_id,
concat_ws(':', 'IFF',versionnumber,v.footnote) as availabilityconditionRef,
concat_ws(':','IFF',line_id,patterncode) as journeypatternref,
NULL as timedemandgroupref,
'IFF:'||transmode as productCategoryRef,
NULL as noticeassignmentRef,
NULL as departuretime,
'IFF:'||serviceid as blockref,
coalesce(variant,servicenumber)::integer as name,
NULL as lowfloor,
NULL as hasLiftOrRamp,
NULL as haswifi,
CASE WHEN transmode in ('NSS','NSB','B','BNS','X','U','Y') THEN false
     WHEN (ARRAY['GEFI']::varchar[] <@ attrs) THEN false
     WHEN (ARRAY['FIET']::varchar[] <@ attrs) THEN true
     WHEN transmode in('IC','SPR','S','ST','INT','ES','THA','TGV','ICD') THEN true 
     ELSE NULL END as bicycleAllowed,
CASE WHEN (ARRAY['RESV']::varchar[] <@ attrs) THEN true ELSE NULL END as onDemand
FROM PASSTIMES LEFT JOIN timetable_validity as v USING (serviceid),delivery
ORDER BY serviceid,line_id,patterncode,servicenumber,stoporder
""")
    journeys = {}
    for row in cur.fetchall():
        row.update(timedemandGroupRefForJourney[row['operator_id']])
        journeys[row['operator_id']] = row
    cur.close()
    return journeys

def getTripTransfers(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
SELECT DISTINCT ON (journeyref,pointref,onwardjourneyref,onwardpointref)
md5(concat_ws(':','IFF',p.serviceid,p.line_id,p.footnote,coalesce(p.variant,p.servicenumber),onward.serviceid,onward.line_id,onward.footnote,coalesce(onward.variant,onward.servicenumber),possiblechange)) as operator_id,
concat_ws(':','IFF',p.serviceid,p.line_id,p.footnote,coalesce(p.variant,p.servicenumber)) as journeyref,
'IFF:'||p.station||':'||coalesce(p.platform,'0') as pointref,
concat_ws(':','IFF',onward.serviceid,onward.line_id,onward.footnote,coalesce(onward.variant,onward.servicenumber)) as onwardjourneyref,
'IFF:'||onward.station||':'||coalesce(onward.platform,'0') as onwardpointref,
possiblechange as transfer_type
FROM 
changes as c,passtimes as p, passtimes as onward
WHERE
c.fromservice = p.serviceid AND
c.station = p.station AND
c.toservice = onward.serviceid AND
c.station = onward.station AND
coalesce(p.foralighting,true) = true AND
coalesce(onward.forboarding,true) = true
ORDER BY journeyref,pointref,onwardjourneyref,onwardpointref,transfer_type""")
    transfers = {}
    for row in cur.fetchall():
        transfers[row['operator_id']] = row
    cur.close()
    return transfers
     
def getLines(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
(SELECT DISTINCT ON (line_id)
p.line_id as operator_id,
p.line_id as privatecode,
'IFF:'||upper(c.code) as operatorref,
description as publiccode,
CASE WHEN (p.servicename is not null) THEN p.servicename||' '||least(begin_station.name,dest_station.name)||' <-> '||greatest(dest_station.name,begin_station.name)
     ELSE least(begin_station.name,dest_station.name)||' <-> '||greatest(begin_station.name,dest_station.name)||' '||transmode||route(servicenumber,variant) END AS name,
'TRAIN' as transportmode,
false as monitored
FROM
passtimes as p LEFT JOIN timetable_service as s USING (serviceid,servicenumber,variant) ,trnsmode as m,company as c,
(select distinct on (line_id,serviceid) serviceid,idx,station,stoporder from passtimes order by line_id,serviceid,stoporder ASC) as begin,
(select distinct on (line_id,serviceid) serviceid,idx,station,stoporder from passtimes order by line_id,serviceid,stoporder DESC) as dest,
station as begin_station,
station as dest_station
WHERE
m.code = p.transmode and
p.companynumber = c.company AND
p.serviceid = begin.serviceid AND
p.serviceid = dest.serviceid AND
begin.station = begin_station.shortname AND
dest.station = dest_station.shortname AND
transmode not in ('NSS','NSB','B','NSM','NST','BNS','X','U','Y')
ORDER BY line_id ASC,(servicenumber % 2 = 0),dest.stoporder DESC)
UNION
(SELECT DISTINCT ON (p.line_id)
p.line_id as operator_id,
p.line_id as privatecode,
'IFF:'||upper(c.code) as operatorref,
description as publiccode,
least(begin_station.name,dest_station.name)||' <-> '||greatest(dest_station.name,begin_station.name) as name,
CASE WHEN (transmode in ('NSS','NSB','B','BNS','X','U','Y')) THEN 'BUS'
     WHEN (transmode = 'NSM') THEN 'METRO'
     WHEN (transmode = 'NST') THEN 'TRAM'
     ELSE 'BUS' END as transportmode,
false as monitored
FROM
trnsmode as m,passtimes as p,company as c,
(select distinct on (line_id) line_id,station from passtimes
   WHERE serviceid in (SELECT DISTINCT ON (line_id) serviceid from passtimes order by line_id,stoporder DESC)
    order by line_id DESC,stoporder ASC) as begin,
(select distinct on (line_id) line_id,station from passtimes WHERE serviceid in (SELECT DISTINCT ON (line_id) serviceid from passtimes order by 
line_id,stoporder DESC)
   order by line_id DESC,stoporder DESC) as dest,
station as begin_station,
station as dest_station
WHERE
m.code = p.transmode and
p.companynumber = c.company AND
p.line_id = begin.line_id AND
p.line_id = dest.line_id AND
begin.station = begin_station.shortname AND
dest.station = dest_station.shortname AND
transmode in ('NSS','NSB','B','NSM','NST','BNS','X','U','Y')
ORDER BY 
p.line_id,least(begin.station,dest.station),greatest(begin.station,dest.station))""")
    lines = {}
    for row in cur.fetchall():
        lines[row['operator_id']] = row
    cur.close()
    return lines

