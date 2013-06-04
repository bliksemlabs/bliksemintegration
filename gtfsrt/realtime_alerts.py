"""
For a new client:
 1) send FULL_DATASET (to erase remaining data)
 2) send DIFFERENTIAL for each new live update

"""

from gtfs_realtime_pb2 import FeedMessage
import iso8601
import sys
import time
import zmq
from utils import removeAlert, getAlertKV15, getAlertKV17

KV15_ZMQ = "tcp://127.0.0.1:6015"
KV17_ZMQ = "tcp://127.0.0.1:6017"

context = zmq.Context()

while True:
    sys.stderr.write("Connecting to %s...\n" % (KV15_ZMQ))
    kv15 = context.socket(zmq.SUB)
    kv15.connect(KV15_ZMQ)
    kv15.setsockopt(zmq.SUBSCRIBE, '')
    
    sys.stderr.write("Connecting to %s...\n" % (KV17_ZMQ))
    kv17 = context.socket(zmq.SUB)
    kv17.connect(KV17_ZMQ)
    kv17.setsockopt(zmq.SUBSCRIBE, '')


    poll = zmq.Poller()
    poll.register(kv15, zmq.POLLIN)
    poll.register(kv17, zmq.POLLIN)

    while True:
        sockets = dict(poll.poll(60000))
        if kv15 in sockets and sockets[kv15] == zmq.POLLIN:
            packet = kv15.recv_json()

            feedmessage = FeedMessage()
            feedmessage.header.gtfs_realtime_version = "1.0"
            feedmessage.header.incrementality = gtfs_realtime_pb2.FeedHeader.DIFFERENTIAL
            feedmessage.header.timestamp = int(time.mktime(iso8601.parse_date(packet['timestamp']).timetuple()))

            for message in packet['messages']:
                if message['messagetype'] == 'STOPERRORMESSAGE':
                    continue

                needle = 'A%08X' % (zlib.crc32('|'.join([message[x] for x in ['dataownercode', 'messagecodedate', 'messagecodenumber']])))

                if message['messagetype'] == 'DELETEMESSAGE':
                    feedentity = feedmessage.entity.add()
                    feedentity.id = str(needle)
                    removeAlert(feedentity, needle)

                elif message['messagetype'] == 'STOPMESSAGE' and message['messagedurationtype'] != "FIRSTVEJO":
                    feedentity = feedmessage.entity.add()
                    feedentity.id = str(needle)
                    getAlertKV15(feedentity, message, needle)

        elif kv17 in sockets and sockets[kv17] == zmq.POLLIN:
            packet = kv17.recv_json()

            feedmessage = FeedMessage()
            feedmessage.header.gtfs_realtime_version = "1.0"
            feedmessage.header.incrementality = gtfs_realtime_pb2.FeedHeader.DIFFERENTIAL
            feedmessage.header.timestamp = int(time.mktime(iso8601.parse_date(packet['timestamp']).timetuple()))

            for cvlinfo in packet['cvlinfo']:
                if 'journey' not in cvlinfo:
                    continue

                if 'mutatejourney' in cvlinfo:
                    needle = 'M%08X' % (zlib.crc32('|'.join([cvlinfo['journey'][x] for x in ['dataownercode', 'lineplanningnumber', 'operatingday', 'journeynumber', 'reinforcementnumber']])))

                    for operation in cvlinfo['mutatejourney']:
                        if operation['messagetype'] == 'CANCEL':
                            feedentity = feedmessage.entity.add()
                            feedentity.id = str(needle)
                            feedmessage.entity.append(getAlertKV17(feedentity, operation, cvlinfo['journey']))

                        elif operation['messagetype'] == 'RECOVER':
                            feedentity = feedmessage.entity.add()
                            feedentity.id = str(needle)
                            feedmessage.entity.append(removeAlert(feedentity))


                if 'mutatejourneystop' in cvlinfo:
                    for operation in cvlinfo['mutatejourneystop']:
                        if operation['messagetype'] == 'MUTATIONMESSAGE':
                            needle = 'M%08X' % (zlib.crc32('|'.join([cvlinfo['journey'][x] for x in ['dataownercode', 'lineplanningnumber', 'operatingday', 'journeynumber', 'reinforcementnumber']] + [operation['userstopcode']])))
                            feedentity = feedmessage.entity.add()
                            feedentity.id = str(needle)
                            feedmessage.entity.append(getAlertKV17(feedentity, operation, cvlinfo['journey'], operation['userstopcode']))

        else:
            break
