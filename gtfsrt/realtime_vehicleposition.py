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
from utils import getVehiclePosition

KV6_ZMQ = "tcp://127.0.0.1:6006"

sequence = 0

context = zmq.Context()

while True:
    sys.stderr.write("Connecting to %s...\n" % (KV6_ZMQ))
    receiver = context.socket(zmq.SUB)
    receiver.connect(KV6_ZMQ)
    receiver.setsockopt(zmq.SUBSCRIBE, '')

    poll = zmq.Poller()
    poll.register(receiver, zmq.POLLIN)

    while True:
        sockets = dict(poll.poll(60000))
        if receiver in sockets and sockets[receiver] == zmq.POLLIN:
            kv6 = receiver.recv_json()

            feedmessage = FeedMessage()
            feedmessage.header.gtfs_realtime_version = "1.0"
            feedmessage.header.incrementality = gtfs_realtime_pb2.FeedHeader.DIFFERENTIAL
            feedmessage.header.timestamp = int(time.mktime(iso8601.parse_date(kv6['timestamp']).timetuple()))

            for posinfo in kv6:
                if posinfo['messagetype'] in ['DELAY', 'INIT', 'END']:
                   continue

                feedentity = feedmessage.entity.add()
                feedentity.id = str(sequence)
                getVehiclePosition(feedentity, posinfo)
                sequence += 1;

        else:
            break
