import psycopg2

class Pattern:
    def __init__(self, pattern_id,stop_ids,pickup_types,drop_off_types,productcategory,ondemand,timepoints):
        self.pattern_id = pattern_id
        self.stop_ids = stop_ids
        self.productcategory = productcategory
        self.ondemand = ondemand
        self.pickup_types = pickup_types
        self.drop_off_types = drop_off_types
        self.timepoints = timepoints
    
    @property
    def signature(self):
        return (tuple(self.stops),tuple(self.pickup_types),tuple(self.drop_off_types),tuple(self.timepoints), productcategory,ondemand)

class TripBundle:
    def __init__(self, riddb, pattern,min_time=0,max_time=999999):
        self.riddb = riddb
        self.pattern = pattern
        self.trip_ids = []
        self.min_time = min_time
        self.max_time = max_time

    def find_time_range(self):
        return (self.min_time,self.max_time)

    def gettimepatterns(self):
        timepatterns = []
        cur = self.riddb.conn.cursor()
        cur.execute("""SELECT DISTINCT journeypatternref||':'||timedemandgroupref FROM journey WHERE id = any(%s)""",[[long(x) for x in self.trip_ids]])
        for row in cur.fetchall():
            timedemandgroup_id = row[0]
            timedemandgroup = self.riddb.timedemandgroups[timedemandgroup_id]
            assert len(timedemandgroup) == len(self.pattern.stop_ids)
            timepatterns.append( (timedemandgroup_id,timedemandgroup))  
        return timepatterns

    def getattributes(self):
        attributes = []
        query = "SELECT (lowfloor or hasliftorramp),haswifi,bicycleallowed FROM journey WHERE id = %s LIMIT 1"
        cur = self.riddb.conn.cursor()
        for trip_id in self.sorted_trip_ids():
            cur.execute(query,[trip_id])
            wheelchair_accessible,haswifi,bicycleallowed = cur.fetchone()
            attr = {}
            attr['wheelchair_accessible'] = wheelchair_accessible
            attr['has_wifi'] = haswifi
            attr['bicycle_allowed'] = bicycleallowed
            attributes.append(attr)
        return attributes

    def sorted_trip_ids(self) :
        """ function from a route (TripBundle) to a list of all trip_ids for that route,
        sorted by first departure time of each trip """
        query = """
    select id::text, departuretime from journey
    where id in (%s)
    order by departuretime""" % (",".join( ["'%s'"%x for x in self.trip_ids] ))
        cur = self.riddb.conn.cursor()
        cur.execute(query)
        # get all trip ids in this pattern ordered by first departure time
        sorted_trips = [trip_id for (trip_id, departuretime) in cur.fetchall()]
        cur.close()
        return sorted_trips
        
    def add_trip(self, trip_id):
        self.trip_ids.append( trip_id )
        
    def stop_time_bundle( self, stop_id, service_id ):
        c = self.gtfsdb.conn.cursor()
        
        query = """
SELECT stop_times.* FROM stop_times, trips 
  WHERE stop_times.trip_id = trips.trip_id 
        AND trips.trip_id IN (%s) 
        AND trips.service_id = ? 
        AND stop_times.stop_id = ?
        AND arrival_time NOT NULL
        AND departure_time NOT NULL
  ORDER BY departure_time"""%(",".join(["'%s'"%x for x in self.trip_ids]))
      
        c.execute(query, (service_id,str(stop_id)))
        
        return list(c)
    
    def stop_time_bundles( self, service_id ):
        
        c = self.gtfsdb.conn.cursor()
        
        query = """
        SELECT stop_times.trip_id, 
               stop_times.arrival_time, 
               stop_times.departure_time, 
               stop_times.stop_id, 
               stop_times.stop_sequence, 
               stop_times.shape_dist_traveled 
        FROM stop_times, trips
        WHERE stop_times.trip_id = trips.trip_id
        AND trips.trip_id IN (%s)
        AND trips.service_id = ?
        AND arrival_time NOT NULL
        AND departure_time NOT NULL
        ORDER BY stop_sequence"""%(",".join(["'%s'"%x for x in self.trip_ids]))
            
        #bundle queries by trip_id
        
        trip_id_sorter = {}
        for trip_id, arrival_time, departure_time, stop_id, stop_sequence, shape_dist_traveled in c.execute(query, (service_id,)):
            if trip_id not in trip_id_sorter:
                trip_id_sorter[trip_id] = []
                
            trip_id_sorter[trip_id].append( (trip_id, arrival_time, departure_time, stop_id, stop_sequence, shape_dist_traveled) )
        
        return zip(*(trip_id_sorter.values()))

class RIDDatabase:
    def __init__(self, dbname):
        self.dbname = dbname
        self.conn = psycopg2.connect("dbname='%s'"%(dbname))
        cur = self.conn.cursor()
        cur.execute("""
create temporary table servicecalendar as (
SELECT validfrom,bitcalendar,row_number() OVER () as service_id,unnest(array_agg(availabilityconditionref)) as availabilityconditionref  FROM (
   SELECT availabilityconditionref, bitcalendar(array_agg(validdate ORDER BY validdate)) as bitcalendar,min(validdate) as validfrom FROM 
    availabilityconditionday as ad WHERE ad.isavailable = true and validdate >= date 'yesterday' GROUP by availabilityconditionref) as x
GROUP BY validfrom,bitcalendar
ORDER BY service_id
);""") #Compile servicecelandar
        cur.close()
        self.timedemandgroups = {}
        for timedemandgroup_id,timedemandgroup in self.gettimepatterns():
            self.timedemandgroups[timedemandgroup_id] = timedemandgroup
      
    def date_range(self):
        cur = self.conn.cursor()
        cur.execute("select min(validdate),max(validdate) from availabilityconditionday where isavailable = true;")
        start_date, end_date = cur.fetchone()
        cur.close()
        return start_date, end_date

    def gettransfers(self,from_stop_id,maxdistance=None):
        cur = self.conn.cursor()
        if maxdistance is None:
            maxdistance = 999999
        cur.execute("""
SELECT DISTINCT ON (from_stop_id,to_stop_id)
from_stop_id,to_stop_id,9,distance
FROM
((SELECT from_stop_id::text,to_stop_id::text,9,distance 
FROM transfers
WHERE from_stop_id = %s AND distance < %s
ORDER BY from_stop_id,to_stop_id
)UNION(
SELECT to_stop_id::text,from_stop_id::text,9,distance
FROM transfers
WHERE to_stop_id = %s AND distance < %s
ORDER BY from_stop_id,to_stop_id)) as x
ORDER BY from_stop_id,to_stop_id
""",[from_stop_id,maxdistance]*2)
        res = cur.fetchall()
        cur.close()
        return res

    def find_max_service (self) :
        cur = self.conn.cursor()
        cur.execute("""
SELECT
validdate,count(distinct journey.id)
FROM journey LEFT JOIN availabilityconditionday USING (availabilityconditionref)
WHERE isavailable = true
GROUP BY validdate
ORDER BY count DESC
LIMIT 1""")
        max_date = cur.fetchone()[0]
        cur.close()
        return max_date
   
    def tripids_in_serviceperiods(self):
        cur = self.conn.cursor()
        cur.execute("""
SELECT
journey.id::text as tid,service_id as sid
FROM journey LEFT JOIN servicecalendar USING (availabilityconditionref)
""");
        return cur.fetchall()

    def service_periods(self, sample_date):
        cur = self.conn.cursor()
        cur.execute("""
SELECT DISTINCT ON (service_id) service_id
FROM availabilityconditionday JOIN servicecalendar USING (availabilityconditionref)
WHERE validdate = %s and isavailable = true""",[sample_date]);
        return [x[0] for x in cur.fetchall()]

    def stops(self):
        cur = self.conn.cursor()        
        cur.execute("""
SELECT id::text as stop_id,name as stop_name,latitude as stop_lat,longitude as stop_lon,platformcode FROM scheduledstoppoint ORDER BY id""" )
        ret = cur.fetchall()
        cur.close()
        return ret

    def stopattributes(self):
        cur = self.conn.cursor()
        query = """
SELECT DISTINCT ON (stop_id)
scheduledstoppoint.id::text as stop_id,
scheduledstoppoint.name as stop_name,
latitude as stop_lat,
longitude as stop_lon,
platformcode as platform_code,
restrictedmobilitysuitable as wheelchair_boarding,
visualimpairmentsuitable as visual_accessible,
string_agg(gtfs_route_type::text,';') OVER (PARTITION BY scheduledstoppoint.id)
FROM scheduledstoppoint LEFT JOIN
(SELECT DISTINCT pointref,gtfs_route_type FROM pointinjourneypattern as pjp
                        JOIN journeypattern             as jp  ON (journeypatternref = jp.id)
                        JOIN route                      as r   ON (routeref = r.id)
                        JOIN line                       as l   ON (lineref = l.id)
                        JOIN transportmode              as tm  USING (transportmode)) as x ON (scheduledstoppoint.id = pointref)
"""
        cur.execute( query )
        ret = []
        for row in cur.fetchall():
            row = list(row)
            attr = {}
            if row[7] is not None:
                attr['route_types'] = [int(x) for x in row[7].split(';')]
            attr['wheelchair_boarding'] = row[5]
            attr['visual_accessible'] = row[6]
            attr['platform_code'] = row[4]
            row[4] = attr
            ret.append(row[:5])
        cur.close()
        return ret

    def tripinfo(self,trip_id):
        cur = self.conn.cursor()
        cur.execute( """
SELECT 
l.id::text,d.name,o.operator_id,l.publiccode,l.name,gtfs_route_type as mode,pc.name as trip_long_name
FROM journey as j JOIN journeypattern as jp on (jp.id = journeypatternref)
                  JOIN route as r on (r.id = routeref)
                  JOIN line as l on (l.id = lineref)
                  JOIN transportmode as m USING (transportmode)
                  JOIN operator as o on (o.id = operatorref)
                  JOIN destinationdisplay as d on (d.id = destinationdisplayref)
                  LEFT JOIN productcategory as pc ON (pc.id = productcategoryref)
WHERE j.id = %s
""",[trip_id])
        ret = cur.fetchone()
        cur.close()
        return ret

    def fetch_timedemandgroups(self,trip_ids) :
        """ generator that takes a list of trip_ids 
        and returns all timedemandgroups in order for those trip_ids """
        for trip_id in trip_ids :
            query = """SELECT journeypatternref||':'||timedemandgroupref,departuretime FROM journey WHERE journey.id = %s""" 
            c = self.conn.cursor()
            c.execute(query,[trip_id])
            for (timedemandgroupref,departuretime) in c.fetchall():
                yield (timedemandgroupref,departuretime)

    def fetch_stop_times(self,trip_ids) :
        """ generator that takes a list of trip_ids 
        and returns all stop times in order for those trip_ids """
        for trip_id in trip_ids :
            last_time = 0
            query = """
            select departuretime+totaldrivetime as arrival_time, departuretime+totaldrivetime+stopwaittime as departure_time,
                   pointorder as stop_sequence
            from journey JOIN pointintimedemandgroup USING (timedemandgroupref)
            where journey.id = %s
            order by pointorder""" 
            c = self.conn.cursor()
            c.execute(query,[trip_id])
            for (arrival_time, departure_time, stop_sequence) in c.fetchall():
                if departure_time < last_time :
                    print "non-increasing departure times on trip %s, forcing positive" % trip_id
                    departure_time = last_time
                last_time = departure_time
                yield(arrival_time, departure_time)

    def count_stops(self):
        cur = self.conn.cursor()
        cur.execute( "SELECT count(*) FROM scheduledstoppoint" )
        return cur.fetchone()[0]

    def gettimepatterns(self):
        cur = self.conn.cursor()
        cur.execute("""
SELECT journeypatternref||':'||timedemandgroupref,array_agg(totaldrivetime||':'||stopwaittime::text ORDER BY pointorder) as timegroup
FROM 
(SELECT DISTINCT ON (journeypatternref,timedemandgroupref,pointorder) * 
 FROM (select distinct timedemandgroupref,journeypatternref from journey) as journey 
                       JOIN pointintimedemandgroup USING (timedemandgroupref)
                       JOIN pointinjourneypattern USING (journeypatternref,pointorder)
                       JOIN stoppoint ON (pointref = stoppoint.id)
 WHERE isscheduled = true) as x
GROUP BY journeypatternref,timedemandgroupref
ORDER BY journeypatternref,timedemandgroupref""")
        res = cur.fetchall()
        for row in res:
            for i in range(len(row[1])):
                v = row[1][i].split(':')
                row[1][i] = (int(v[0]),int(v[1]))
            if row[1][0][0] != 0 or row[1][0][1] != 0:
                row[1][0] = (0,0)
        return res

    def service_ids(self):
        query = "SELECT DISTINCT service_id FROM servicecalendar"
        cur = self.conn.cursor()
        cur.execute(query)
        return [x[0] for x in cur.fetchall()]
    def agency(self,agency_id):
        query = "SELECT operator_id,name,url,phone,timezone FROM operator WHERE operator_id = %s"
        cur = self.conn.cursor()
        cur.execute(query,[agency_id])
        res = cur.fetchall()
        if len(res) != 1:
            return None
        return res[0]

    def compile_trip_bundles(self, reporter=None):
        
        c = self.conn.cursor()

        patterns = {}
        bundles = {}

        c.execute( """SELECT servicejourney.journeypatternref,pc.name,ondemand,array_agg(servicejourney.id::text) as trips,array_agg(distinct servicejourney.timedemandgroupref),
min(min_time),max(max_time)
                      FROM servicejourney LEFT JOIN productcategory as pc ON (productcategoryref = pc.id),
(SELECT DISTINCT ON (timedemandgroupref,journeypatternref) timedemandgroupref,journeypatternref,
count(distinct pointintimedemandgroup.pointorder) as timedemandgrouppoints,
count(distinct pointinjourneypattern.pointorder) as journeypatternpoints,
min(departuretime) as min_time,
max(departuretime+totaldrivetime+stopwaittime) as max_time
FROM 
servicejourney JOIN pointintimedemandgroup USING (timedemandgroupref)
               JOIN pointinjourneypattern USING (journeypatternref,pointorder)
GROUP BY journeypatternref,timedemandgroupref) as lengths
WHERE servicejourney.timedemandgroupref = lengths.timedemandgroupref AND servicejourney.journeypatternref = lengths.journeypatternref
                      GROUP BY servicejourney.journeypatternref,timedemandgrouppoints,journeypatternpoints,pc.name,ondemand""" )
        i = 0
        routes = c.fetchall()
        for journeypatternref,productcategory,ondemand,trips,timedemandgroups,min_time,max_time in routes:
            i+=1
            if reporter and i%(len(routes)//50+1)==0: reporter.write( "%d/%d trips grouped by %d patterns\n"%(i,len(routes),len(bundles)))
            d = self.conn.cursor()
            d.execute("""
SELECT journeypatternref,array_agg(pointref::text ORDER BY pointorder) as stop_ids,
                         array_agg(pickup_type ORDER BY pointorder) as pickup_types,
                         array_agg(drop_off_type ORDER BY pointorder) as drop_off_types,
                         array_agg(iswaitpoint::int4 ORDER BY pointorder) as timepoints,
       unnest(array_agg(distinct headsign)) as headsign
FROM
(SELECT
       journeypatternref,
       jp.pointref,
       jp.pointorder,
       iswaitpoint,
       CASE WHEN (forboarding = false) THEN 1
            WHEN (requeststop = true) THEN 2
            ELSE 0 END as pickup_type,
       CASE WHEN (foralighting = false) THEN 1
            WHEN (requeststop = true)  THEN 2
            ELSE 0 END as drop_off_type,
       d.name as headsign
       FROM pointinjourneypattern as jp JOIN journeypattern AS j ON (j.id = journeypatternref)
                                        JOIN destinationdisplay AS d ON (d.id = j.destinationdisplayref)
,pointintimedemandgroup as tp,scheduledstoppoint as sp
       WHERE jp.pointref = sp.id AND jp.pointorder = tp.pointorder AND timedemandgroupref = %s AND journeypatternref = %s) as x
GROUP BY journeypatternref
""",[timedemandgroups[0],journeypatternref])
            try:
                journeypatternref,stop_ids,pickup_types,drop_off_types,timepoints,headsign = d.fetchone()
            except:
                print [timedemandgroups[0],journeypatternref]
                continue
            pattern_signature = (tuple(stop_ids),tuple(pickup_types),tuple(drop_off_types),tuple(timepoints),headsign)
            if productcategory is not None and len(productcategory) < 1:
                productcategory = None
            if pattern_signature not in patterns:
                pattern = Pattern( len(patterns), stop_ids,pickup_types,drop_off_types,productcategory,ondemand,timepoints)
                patterns[pattern_signature] = pattern
            else:
                pattern = patterns[pattern_signature]
                
            if pattern not in bundles:
                bundles[pattern] = TripBundle( self, pattern ,min_time=min_time,max_time=max_time)
            for trip_id in trips:
                bundles[pattern].add_trip( trip_id )
                if min_time < bundles[pattern].min_time:
                    bundles[pattern].min_time = min_time
                if max_time > bundles[pattern].max_time:  
                    bundles[pattern].max_time = max_time

        c.close()
        
        return bundles.values()

