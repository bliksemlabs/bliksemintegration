import psycopg2

class Pattern:
    def __init__(self, pattern_id,stop_ids,pickup_types,drop_off_types,productcategory,ondemand):
        self.pattern_id = pattern_id
        self.stop_ids = stop_ids
        self.productcategory = productcategory
        self.ondemand = ondemand
        self.pickup_types = pickup_types
        self.drop_off_types = drop_off_types
    
    @property
    def signature(self):
        return (tuple(self.stops),tuple(self.pickup_types),tuple(drop_off_types), productcategory,ondemand)

class TripBundle:
    def __init__(self, riddb, pattern):
        self.riddb = riddb
        self.pattern = pattern
        self.trip_ids = []

    def sorted_trip_ids(self) :
        """ function from a route (TripBundle) to a list of all trip_ids for that route,
        sorted by first departure time of each trip """
        query = """
    select id, departuretime from journey
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

class RIDdatabase:
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

    def date_range(self):
        cur = self.conn.cursor()
        cur.execute("select min(validdate),max(validdate) from availabilityconditionday where isavailable = true;")
        start_date, end_date = cur.fetchone()
        cur.close()
        return start_date, end_date

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
journey.id as tid,service_id as sid
FROM journey LEFT JOIN servicecalendar USING (availabilityconditionref)
""");
        return cur.fetchall()

    def service_periods(self, sample_date):
        cur = self.conn.cursor()
        cur.execute("""
SELECT DISTINCT ON (service_id) service_id
FROM availabilityconditionday LEFT JOIN servicecalendar USING (availabilityconditionref)
WHERE validdate = %s and isavailable = true""",[sample_date]);
        return [x[0] for x in cur.fetchall()]

    def stops(self):
        cur = self.conn.cursor()
        
        cur.execute( "SELECT id as stop_id, name as stop_name, latitude as stop_lat, longitude stop_lon FROM scheduledstoppoint ORDER BY id" )
        ret = cur.fetchall()
        cur.close()
        return ret

    def tripinfo(self,trip_id):
        cur = self.conn.cursor()
        cur.execute( """
SELECT 
l.id,d.name,o.name,l.publiccode,l.name,gtfs_route_type as mode
FROM journey as j LEFT JOIN journeypattern as jp on (jp.id = journeypatternref)
                  LEFT JOIN route as r on (r.id = routeref)
                  LEFT JOIN line as l on (l.id = lineref)
                  LEFT JOIN transportmode as m USING (transportmode)
                  LEFT JOIN operator as o on (o.id = operatorref)
                  LEFT JOIN destinationdisplay as d on (d.id = destinationdisplayref)
WHERE j.id = %s
""",[trip_id])
        ret = cur.fetchone()
        cur.close()
        return ret

    def fetch_stop_times(self,trip_ids) :
        """ generator that takes a list of trip_ids 
        and returns all stop times in order for those trip_ids """
        for trip_id in trip_ids :
            last_time = 0
            query = """
            select departuretime+totaldrivetime as arrival_time, departuretime+totaldrivetime+stopwaittime as departure_time,
                   pointorder as stop_sequence
            from journey LEFT JOIN pointintimedemandgroup USING (timedemandgroupref)
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
SELECT timedemandgroupref,array_agg(totaldrivetime||':'||stopwaittime::text ORDER BY pointorder) as timegroup
FROM pointintimedemandgroup GROUP BY timedemandgroupref""")
        res = cur.fetchall()
        for row in res:
            for point in row[1]:
                point.split(':')
        return res

    def service_ids(self):
        query = "SELECT DISTINCT service_id FROM servicecalendar"
        cur = self.conn.cursor()
        cur.execute(query)
        return [x[0] for x in cur.fetchall()]

    def compile_trip_bundles(self, reporter=None):
        
        c = self.conn.cursor()

        patterns = {}
        bundles = {}

        c.execute( """SELECT servicejourney.journeypatternref,pc.name,ondemand,array_agg(servicejourney.id) as trips,array_agg(distinct servicejourney.timedemandgroupref)
                      FROM servicejourney LEFT JOIN productcategory as pc ON (productcategoryref = pc.id),
(SELECT DISTINCT ON (timedemandgroupref,journeypatternref) timedemandgroupref,journeypatternref,
count(distinct pointintimedemandgroup.pointorder) as timedemandgrouppoints,
count(distinct pointinjourneypattern.pointorder) as journeypatternpoints
FROM 
servicejourney LEFT JOIN pointintimedemandgroup USING (timedemandgroupref)
               LEFT JOIN pointinjourneypattern USING (journeypatternref,pointorder)
GROUP BY journeypatternref,timedemandgroupref) as lengths
WHERE servicejourney.timedemandgroupref = lengths.timedemandgroupref AND servicejourney.journeypatternref = lengths.journeypatternref
                      GROUP BY servicejourney.journeypatternref,timedemandgrouppoints,journeypatternpoints,pc.name,ondemand""" )
        i = 0
        routes = c.fetchall()
        for journeypatternref,productcategory,ondemand,trips,timedemandgroups in routes:
            i+=1
            if reporter and i%(len(routes)//50+1)==0: reporter.write( "%d/%d trips grouped by %d patterns\n"%(i,len(routes),len(bundles)))
            d = self.conn.cursor()
            d.execute("""
SELECT journeypatternref,array_agg(pointref ORDER BY pointorder) as stop_ids,
                         array_agg(pickup_type ORDER BY pointorder) as pickup_types,
                         array_agg(drop_off_type ORDER BY pointorder) as drop_off_types
FROM
(SELECT
       journeypatternref,
       jp.pointref,
       jp.pointorder,
       CASE WHEN (forboarding = false) THEN 1
            WHEN (requeststop = true) THEN 2
            ELSE 0 END as pickup_type,
       CASE WHEN (foralighting = false) THEN 1
            WHEN (requeststop = true)  THEN 2
            ELSE 0 END as drop_off_type
       FROM pointinjourneypattern as jp,pointintimedemandgroup as tp,scheduledstoppoint as sp
       WHERE jp.pointref = sp.id AND jp.pointorder = tp.pointorder AND timedemandgroupref = %s AND journeypatternref = %s) as x
GROUP BY journeypatternref
""",[timedemandgroups[0],journeypatternref])
            journeypatternref,stop_ids,pickup_types,drop_off_types = d.fetchone()
            pattern_signature = (tuple(stop_ids),tuple(pickup_types),tuple(drop_off_types))
            if productcategory is not None and len(productcategory) < 1:
                productcategory = None
            if pattern_signature not in patterns:
                pattern = Pattern( len(patterns), stop_ids,pickup_types,drop_off_types,productcategory,ondemand)
                patterns[pattern_signature] = pattern
            else:
                pattern = patterns[pattern_signature]
                
            if pattern not in bundles:
                bundles[pattern] = TripBundle( self, pattern )
            for trip_id in trips:
                bundles[pattern].add_trip( trip_id )

        c.close()
        
        return bundles.values()

