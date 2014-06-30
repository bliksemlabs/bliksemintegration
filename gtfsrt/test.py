from gtfs_realtime_pb2 import FeedEntity

import utils
from google.protobuf import text_format

print 'getTripId', utils.getTripId('CXX', 'M300', '340', '2013-06-03')

print 'getRouteId', utils.getStopId('ARR', '49005091')

print 'getStopSequenceTripId', utils.getStopSequenceTripId('CXX', 'M300', '340', '2013-06-03', '57430800', '0')

posinfo = {'operatingday':  '2013-06-03', 'dataownercode': 'CXX', 'lineplanningnumber': 'M300', 'journeynumber': '340', 'messagetype': 'ONSTOP', 'rd-x': 150000, 'rd-y': 300000, 'userstopcode': '57430800', 'passagesequencenumber': '0', 'timestamp': '2013-06-03T02:00:00'}
feedentity = FeedEntity()
utils.getVehiclePosition(feedentity, posinfo)
print 'getVehiclePosition', text_format.MessageToString(feedentity)

message = {'operatingday':  '2013-06-03', 'dataownercode': 'CXX', 'lineplanningnumber': 'M300', 'userstopcodes': ['57430800'], 'messagecontent': 'Hello World', 'messagestarttime': '2012-01-01T00:00:00', 'messagedurationtype': 'REMOVE'}
feedentity = FeedEntity()
utils.getAlertKV15(feedentity, message)
print 'getAlertKV15', text_format.MessageToString(feedentity)

operation = {'messagetype': 'CANCEL'}
journey = {'operatingday':  '2013-06-03', 'dataownercode': 'CXX', 'lineplanningnumber': 'M300', 'journeynumber': '340'}
feedentity = FeedEntity()
utils.getAlertKV17(feedentity, operation, journey)
print 'getAlertKV17', text_format.MessageToString(feedentity)
