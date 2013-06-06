import time
import iso8601
import psycopg2
import psycopg2.extras
import zlib
import sys
from datetime import datetime
from gtfs_realtime_pb2 import FeedMessage, FeedHeader, Alert, TripDescriptor, VehiclePosition
from utils import getCauseFromReason, getEffectFromEffect

feedmessage = FeedMessage()
feedmessage.header.gtfs_realtime_version = "1.0"
feedmessage.header.incrementality = FeedHeader.FULL_DATASET
feedmessage.header.timestamp = int(time.time())

conn = psycopg2.connect("dbname='rid'")
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

cur.execute("""
SELECT
*,
CAST(EXTRACT( epoch FROM messagestarttime ) AS int) AS "messagestarttime_epoch",
CAST(EXTRACT( epoch FROM messageendtime ) AS int) AS "messageendtime_epoch"
FROM kv15_stopmessage 
LEFT JOIN (SELECT dataownercode,messagecodedate,messagecodenumber,array_agg(DISTINCT id ORDER BY id) as stops
FROM kv15_stopmessage_userstopcode,stoppoint 
WHERE dataownercode||':'||userstopcode = stoppoint.operator_id
GROUP BY dataownercode,messagecodedate,messagecodenumber
) as userstopcodes USING (dataownercode,messagecodedate,messagecodenumber)
LEFT JOIN (SELECT dataownercode,messagecodedate,messagecodenumber,array_agg(DISTINCT id ORDER BY id) as routes
FROM kv15_stopmessage_lineplanningnumber,line
WHERE dataownercode||':'||lineplanningnumber = line.operator_id
GROUP BY dataownercode,messagecodedate,messagecodenumber
) as lineplanningnumbers USING (dataownercode,messagecodedate,messagecodenumber)
         WHERE array_length(stops, 1) > 0 AND
               isdeleted = FALSE AND
               messagepriority <> 'COMMERCIAL' AND
               current_timestamp < messageendtime;
""")

message = cur.fetchone()
while message is not None:
    feedentity = feedmessage.entity.add()
    feedentity.id = 'A%08X' % (zlib.crc32('|'.join([str(message[x]) for x in ['dataownercode', 'messagecodedate', 'messagecodenumber']])))

    active_period = feedentity.alert.active_period.add()

    active_period.start = message['messagestarttime_epoch']

    if message['messagedurationtype'] == 'ENDTIME':
        active_period.end = message['messageendtime_epoch']

    if message['routes'] is None:
        for stop_id in message['stops']:
            entityselector = feedentity.alert.informed_entity.add()
            entityselector.stop_id = str(stop_id)

    else:
        for stop_id in message['stops']:
            for route_id in message['routes']:
                entityselector = feedentity.alert.informed_entity.add()
                entityselector.route_id = str(route_id)
                entityselector.stop_id = str(stop_id)

    feedentity.alert.cause = getCauseFromReason(message)
    feedentity.alert.effect = getEffectFromEffect(message)

    if message['messagecontent'] is not None and len(message['messagecontent']) > 0:
        translation = feedentity.alert.header_text.translation.add()
        translation.text = message['messagecontent']
        translation.language = 'nl'

    description_text = []
    for x in ['reasoncontent', 'effectcontent', 'advicecontent']:
        try:
            if message[x] is not None and len(message[x]) > 0:
                description_text.append(message[x])
        except:
            pass

    if len(description_text) > 0:
        translation = feedentity.alert.description_text.translation.add()
        translation.text = '\n\t\n'.join(description_text)
        translation.language = 'nl'

    message = cur.fetchone()

f = open(sys.argv[1], "wb")
f.write(feedmessage.SerializeToString())
f.close()
