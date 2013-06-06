from gtfs_realtime_pb2 import FeedMessage
from google.protobuf import text_format
import sys

f = open(sys.argv[1], "rb")
feedmessage = FeedMessage()
feedmessage.ParseFromString(f.read())
f.close()

print text_format.MessageToString(feedmessage)
