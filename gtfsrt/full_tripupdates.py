import time
import iso8601
import sys
import zlib
import zmq
from cStringIO import StringIO
from gtfs_realtime_pb2 import FeedMessage, FeedHeader, TripUpdate
from gzip import GzipFile
from json6 import kv6tojson
from json17 import kv17tojson
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

    for needle, tripinfo in storage.items():
        if tripinfo['trip_id'] is not None and  tripinfo is not None and 'punctuality' in tripinfo['posinfo']:
            hasPosinfo = False
            feedentity = feedmessage.entity.add()
            feedentity.id = needle
            feedentity.trip_update.trip.start_date = tripinfo['start_date']
            feedentity.trip_update.trip.trip_id = tripinfo['trip_id']

            first_stop_sequence = None
            first_stop_id = None

            if tripinfo['posinfo']['messagetype'] == 'DELAY':
                first_stop_sequence, first_stop_id = getFirstStopFromJourneyPattern(tripinfo['journeypatternref'])

            if 'cvlinfo' in tripinfo:
                if 'mutatejourney' in tripinfo['cvlinfo']:
                    if tripinfo['cvlinfo']['mutatejourney']['messagetype'] == 'CANCEL':
                        feedentity.trip_update.trip.schedule_relationship = TripDescriptor.CANCELED
                        feedentity.trip_update.timestamp = int(time.mktime(iso8601.parse_date(tripinfo['cvlinfo']['mutatejourney']['timestamp']).timetuple()))
                        # We don't want to "continue" here with posinfo information, which could be old
                        continue

                    elif tripinfo['cvlinfo']['messagetype'] == 'ADD':
                        feedentity.trip_update.trip.schedule_relationship = TripDescriptor.ADDED
                        feedentity.trip_update.timestamp = int(time.mktime(iso8601.parse_date(tripinfo['cvlinfo']['mutatejourney']['timestamp']).timetuple()))

                    elif tripinfo['cvlinfo']['messagetype'] == 'RECOVER':
                        feedentity.trip_update.trip.schedule_relationship = TripDescriptor.SCHEDULED
                        feedentity.trip_update.timestamp = int(time.mktime(iso8601.parse_date(tripinfo['cvlinfo']['mutatejourney']['timestamp']).timetuple()))

                if 'mutatejourneystop' in tripinfo['cvlinfo']:
                    for operation in tripinfo['cvlinfo']['mutatejourneystop']['operations']:
                        if operation['messagetype'] in ['CHANGEDESTINATION', 'MUTATIONMESSAGE']:
                            continue

                        update = feedentity.trip_update.stop_time_update.add()
                        update.stop_sequence, update.stop_id = getStopOrderFromJourneyPattern(tripinfo['journeypatternref'], tripinfo['cvlinfo']['dataownercode'], tripinfo['cvlinfo']['userstopcode'], tripinfo['cvlinfo']['passagesequencenumber'])
                        if operation['messagetype'] == 'SHORTEN':
                            update.schedule_relationship = TripUpdate.StopTimeUpdate.SKIPPED
                        else:
                            update.schedule_relationship = TripUpdate.StopTimeUpdate.SCHEDULED

                            if operation['messagetype'] == 'LAG':
                                update.departure.delay = operation['lagtime']

                            elif operation['messagetype'] == 'CHANGEPASSTIMES':
                                # TODO: VALIDATE!! Also for SUMMER/WINTER time!
                                update.arrival.time = int(time.mktime(iso8601.parse_date(tripinfo['cvlinfo']['operatingday'] + 'T' + operation['targetarrivaltime'] + '+02').timetuple()))
                                update.departure.time = int(time.mktime(iso8601.parse_date(tripinfo['cvlinfo']['operatingday'] + 'T' + operation['targetdeparturetime'] + '+02').timetuple()))

                        # Special case: this is the next stop or we have a DELAY
                        if 'posinfo' in tripinfo:
                            if tripinfo['posinfo']['messagetype'] == 'DELAY':
                                if first_stop_sequence == stop_sequence and first_stop_id == stop_id:
                                    hasPosinfo = True
                                    update.departure.delay = tripinfo['posinfo']['punctuality']

                            elif tripinfo['cvlinfo']['dataownercode'] == tripinfo['posinfo']['dataownercode'] and tripinfo['cvlinfo']['userstopcode'] == tripinfo['posinfo']['userstopcode'] and tripinfo['cvlinfo']['passagesequencenumber'] == tripinfo['posinfo']['passagesequencenumber']:
                                hasPosinfo = True
                                feedentity.trip_update.vehicle.id = str(tripinfo['posinfo']['vehiclenumber'])
                                if operation['messagetype'] == 'CHANGEPASSTIMES':
                                    update.arrival.time += tripinfo['posinfo']['punctuality']
                                    update.departure.time += tripinfo['posinfo']['punctuality']
                                else:
                                    update.arrival.delay = tripinfo['posinfo']['punctuality']

            if 'posinfo' in tripinfo and not hasPosinfo:
                update = feedentity.trip_update.stop_time_update.add()
                update.schedule_relationship = TripUpdate.StopTimeUpdate.SCHEDULED
                feedentity.trip_update.timestamp = int(time.mktime(iso8601.parse_date(tripinfo['posinfo']['timestamp']).timetuple()))

                if tripinfo['posinfo']['messagetype'] == 'DELAY':
                    update.stop_sequence, update.stop_id = getFirstStopFromJourneyPattern(tripinfo['journeypatternref'])
                    update.departure.delay = tripinfo['posinfo']['punctuality']
                else:
                    update.stop_sequence, update.stop_id = getStopOrderFromJourneyPattern(tripinfo['journeypatternref'], tripinfo['posinfo']['dataownercode'], tripinfo['posinfo']['userstopcode'], tripinfo['posinfo']['passagesequencenumber'])
                    update.arrival.delay = tripinfo['posinfo']['punctuality']
                    feedentity.trip_update.vehicle.id = str(tripinfo['posinfo']['vehiclenumber'])

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
                        storage[needle]['posinfo'] = posinfo

            if multipart[0].endswith('/KV6cvlinfo'):
                kv17 = kv17tojson(GzipFile('','r',0,StringIO(''.join(multipart[1:]))).read())

                for cvlinfo in kv17:
                    needle = '%08X' % (abs(zlib.crc32('|'.join([str(cvlinfo[x]) for x in ['dataownercode', 'lineplanningnumber', 'operatingday', 'journeynumber', 'reinforcementnumber']]))))

                    if needle not in storage:
                        storage[needle] = {}
                        storage[needle]['start_date'] = posinfo['operatingday'].replace('-', '')
                        storage[needle]['trip_id'], storage[needle]['journeypatternref'] = getTripJourneyPattern(cvlinfo['dataownercode'], cvlinfo['lineplanningnumber'], cvlinfo['journeynumber'], cvlinfo['operatingday'])                        
                    
                    if storage[needle] is not None:
                        storage[needle]['cvlinfo'] = cvlinfo

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
