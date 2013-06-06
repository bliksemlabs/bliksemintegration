import helper
import logging
import psycopg2
from settings.const import database_connect

conn = psycopg2.connect(database_connect)
cur = conn.cursor()
cur.execute("""
SELECT j.id,jp.operator_id,j.operator_id FROM
(select journeypatternref,count(distinct pointorder) as points from pointinjourneypattern group by journeypatternref) as pattern,
(select timedemandgroupref,count(distinct pointorder)  as timepoints from pointintimedemandgroup group by timedemandgroupref) as timepattern,
journey as j LEFT JOIN journeypattern as jp ON (j.journeypatternref = jp.id)
WHERE
j.journeypatternref = pattern.journeypatternref AND
j.timedemandgroupref = timepattern.timedemandgroupref AND 
points != timepoints;
""")
rows = cur.fetchall()
cur.close()
timegroupsValid = len(rows) == 0
assert timegroupsValid

cur.execute("""
SELECT links.operator_id,rechts.operator_id FROM 
(SELECT j.id,j.operator_id,j.privatecode,validdate FROM journey as j LEFT JOIN availabilityconditionday USING (availabilityconditionref) where 
isavailable = true) as links,
(SELECT j.id,j.operator_id,j.privatecode,validdate FROM journey as j LEFT JOIN availabilityconditionday USING (availabilityconditionref) where 
isavailable = true) as rechts
WHERE links.id != rechts.id AND links.validdate = rechts.validdate AND links.privatecode = rechts.privatecode
""")
rows = cur.fetchall()
cur.close()
duplicateTripidentifiers = len(rows) == 0
assert uniqueTripidentifiers
