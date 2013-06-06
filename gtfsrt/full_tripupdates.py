import time
import iso8601
import sys
import zlib
import zmq
from cStringIO import StringIO
from gtfs_realtime_pb2 import FeedMessage, FeedHeader, TripUpdate
from gzip import GzipFile
from json6 import kv6tojson
from utils import getTripJourneyPattern, getFirstStopFromJourneyPattern, getStopOrderFromJourneyPattern

KV6_ZMQ = "tcp://127.0.0.1:7806"
GTFS_RT_REQ_ZMQ = "tcp://127.0.0.1:6007"

context = zmq.Context()

storage = None

def makemessage():
    feedmessage = FeedMessage()
    feedmessage.header.gtfs_realtime_version = "1.0"
    feedmessage.header.incrementality = FeedHeader.FULL_DATASET
    feedmessage.header.timestamp = int(time.time())

    for needle, posinfo in storage.items():
        if posinfo['trip_id'] is not None and  posinfo is not None and 'punctuality' in posinfo['last']:
            feedentity = feedmessage.entity.add()
            feedentity.id = needle
            feedentity.trip_update.timestamp = int(time.mktime(iso8601.parse_date(posinfo['last']['timestamp']).timetuple()))
            feedentity.trip_update.trip.start_date = posinfo['start_date']
            feedentity.trip_update.trip.trip_id = posinfo['trip_id']
            
            update = feedentity.trip_update.stop_time_update.add()
            update.schedule_relationship = TripUpdate.StopTimeUpdate.SCHEDULED

            if posinfo['last']['messagetype'] == 'DELAY':
                update.stop_sequence, update.stop_id = getFirstStopFromJourneyPattern(posinfo['journeypatternref'])
                update.departure.delay = posinfo['last']['punctuality']
            else:
                update.stop_sequence, update.stop_id = getStopOrderFromJourneyPattern(posinfo['journeypatternref'], posinfo['last']['dataownercode'], posinfo['last']['userstopcode'], posinfo['last']['passagesequencenumber'])
                update.arrival.delay = posinfo['last']['punctuality']
                feedentity.trip_update.vehicle.id = str(posinfo['last']['vehiclenumber'])

    f = open("/tmp/tripUpdates.pb", "wb")
    f.write(feedmessage.SerializeToString())
    f.close()


while True:
    sys.stderr.write("Cleaning the cache...\n")
    storage = dict()

    sys.stderr.write("Connecting to %s...\n" % (KV6_ZMQ))
    receiver = context.socket(zmq.SUB)
    receiver.connect(KV6_ZMQ)
    receiver.setsockopt(zmq.SUBSCRIBE, '')

    request = context.socket(zmq.REP)
    request.bind(GTFS_RT_REQ_ZMQ)

    poll = zmq.Poller()
    poll.register(receiver, zmq.POLLIN)
    poll.register(request, zmq.POLLIN)

    while True:
        sockets = dict(poll.poll(60000))
        if receiver in sockets and sockets[receiver] == zmq.POLLIN:
            multipart = receiver.recv_multipart()
            if multipart[0].endswith('/KV6posinfo'):
            kv6 = kv6tojson(GzipFile('','r',0,StringIO(''.join(multipart[1:]))).read())

            for posinfo in kv6:
                if posinfo['lineplanningnumber'] is None:
                    continue

                needle = '%08X' % (abs(zlib.crc32('|'.join([str(posinfo[x]) for x in ['dataownercode', 'lineplanningnumber', 'operatingday', 'journeynumber', 'reinforcementnumber']]))))

                if posinfo['messagetype'] == 'INIT':
                    if needle in storage:
                        del(storage[needle])
                    continue

                elif posinfo['messagetype'] == 'END':
                    if needle in storage:
                        storage[needle] = None
                    continue

                if needle not in storage:
                    if posinfo['messagetype'] == 'OFFROUTE':
                        continue

                    storage[needle] = {}
                    storage[needle]['start_date'] = posinfo['operatingday'].replace('-', '')
                    storage[needle]['trip_id'], storage[needle]['journeypatternref'] = getTripJourneyPattern(posinfo['dataownercode'], posinfo['lineplanningnumber'], posinfo['journeynumber'], posinfo['operatingday'])

                if storage[needle] is not None:
                    storage[needle]['last'] = posinfo

        elif request in sockets and sockets[request] == zmq.POLLIN:
            cmd = request.recv()
            if cmd == 'RESET':
                request.send('Goodbye :)')
                request.close()
                receiver.close()
                break

            makemessage()
            request.send(str(len(storage)))

        else:
            request.close()
            receiver.close()

            break
