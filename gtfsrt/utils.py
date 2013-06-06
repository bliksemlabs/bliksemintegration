from rdwgs84 import rdwgs84
import time
import iso8601
import psycopg2
from datetime import datetime
from gtfs_realtime_pb2 import Alert, TripDescriptor, VehiclePosition

conn = psycopg2.connect("dbname='rid2'")
cur = conn.cursor()

"""
Interesting stuff for later:

ScheduleRelationship: SCHEDULED, ADDED, UNSCHEDULED, CANCELED, REPLACEMENT

"""

def getTripJourneyPattern(dataownercode, lineplanningnumber, journeynumber, operatingday):
    print dataownercode, lineplanningnumber, journeynumber, operatingday
    privatecode = ':'.join([dataownercode, lineplanningnumber, str(journeynumber)])

    cur.execute("""SELECT journey.id, journeypatternref FROM journey 
                   LEFT JOIN availabilityconditionday USING (availabilityconditionref)
                   WHERE privatecode = %s and isavailable = true and validdate = %s LIMIT 1;""", (privatecode, operatingday,))

    result = cur.fetchone()

    if result is None:
        return None, None
    else:
        return str(result[0]), str(result[1])

def getFirstStopFromJourneyPattern(journeypatternref):
    cur.execute("""SELECT DISTINCT ON (journeypatternref) pointorder, pointref
                   FROM pointinjourneypattern as p_pt
                   WHERE journeypatternref = %s
                   ORDER BY journeypatternref, pointorder ASC LIMIT 1;""", (journeypatternref,))

    result = cur.fetchone()
    return result[0], str(result[1])

def getStopOrderFromJourneyPattern(journeypatternref, dataownercode, userstopcode, passagesequencenumber):
    privatecode = ':'.join([dataownercode, userstopcode])
    cur.execute("""SELECT pointorder, pointref
                   FROM pointinjourneypattern as p_pt
                   LEFT JOIN scheduledstoppoint as s ON (s.id = p_pt.pointref)
                   WHERE journeypatternref = %s and s.operator_id = %s
                   ORDER BY journeypatternref, pointorder ASC;""", (journeypatternref, privatecode,))

    result = cur.fetchall()[passagesequencenumber]
    return result[0], str(result[1])

def getTripId(dataownercode, lineplanningnumber, journeynumber, operatingday):
    privatecode = ':'.join([dataownercode, lineplanningnumber, journeynumber])

    cur.execute("""SELECT j.id FROM servicejourney AS j
                               JOIN availabilityconditionday AS ac 
                               ON (j.availabilityconditionref = ac.availabilityconditionref) 
                               WHERE privatecode = %s and isavailable = true and validdate = %s LIMIT 1;""", (privatecode, operatingday,))
    return str(cur.fetchone()[0])


def getStopId(dataownercode, userstopcode):
    # TODO: ideally we would return the code valid today
    privatecode = ':'.join([dataownercode, userstopcode])

    cur.execute("""SELECT id FROM scheduledstoppoint
                             WHERE operator_id = %s LIMIT 1;""", (privatecode,))
    return str(cur.fetchone()[0])

def getRouteId(dataownercode, lineplanningnumber):
    privatecode = ':'.join([dataownercode, lineplanningnumber])

    cur.execute("""SELECT id FROM line
                             WHERE operator_id = %s LIMIT 1;""", (privatecode,))
    return str(cur.fetchone()[0])

def getStopSequenceTripId(dataownercode, lineplanningnumber, journeynumber, operatingday, userstopcode, passagesequencenumber):
    # TODO: userstropcode, passagesequencenumber => stopid, order
    privatecode = ':'.join([dataownercode, lineplanningnumber, journeynumber])

    cur.execute("""SELECT sp.id, sp.operator_id, j.id FROM servicejourney AS j
                                                      JOIN availabilityconditionday AS ac 
                                                      ON (j.availabilityconditionref = ac.availabilityconditionref) 
                                                      JOIN pointinjourneypattern as pj_p
                                                      ON (j.journeypatternref = pj_p.journeypatternref), stoppoint AS sp
                                                      WHERE j.privatecode = %s AND
                                                            validdate = %s AND
                                                            isavailable = true AND
                                                            sp.id = pj_p.pointref
                                                      ORDER by j.id, pj_p.pointorder;""", (privatecode, operatingday))

    # TODO: cache this
    stops = cur.fetchall()

    privatecode = ':'.join([dataownercode, userstopcode])
    stop_order = -1
    passagesequencenumber = int(passagesequencenumber)
    while (passagesequencenumber >= 0 and stop_order < len(stops)):
        stop_order += 1
        if stops[stop_order][1] == privatecode:
            passagesequencenumber -= 1

    return str(stops[stop_order][0]), stop_order, str(stops[stop_order][2])

def getVehiclePosition(feedentity, posinfo):
    feedentity.vehicle.trip.start_date = posinfo['operatingday'].replace('-', '')
    feedentity.vehicle.trip.schedule_relationship = TripDescriptor.SCHEDULED
    feedentity.vehicle.position.longitude, feedentity.vehicle.position.latitude = rdwgs84(posinfo['rd-x'], posinfo['rd-y'])
    feedentity.vehicle.current_status = getStopStatusByType(posinfo['messagetype']) 

    feedentity.vehicle.stop_id, feedentity.vehicle.current_stop_sequence, feedentity.vehicle.trip.trip_id = getStopSequenceTripId(posinfo['dataownercode'], posinfo['lineplanningnumber'], posinfo['journeynumber'], posinfo['operatingday'], posinfo['userstopcode'], posinfo['passagesequencenumber'])
    feedentity.vehicle.timestamp = int(time.mktime(iso8601.parse_date(posinfo['timestamp']).timetuple()))

    return feedentity

def getCauseFromReason(message):
    """
    UNKNOWN_CAUSE, OTHER_CAUSE, TECHNICAL_PROBLEM, STRIKE, DEMONSTRATION, ACCIDENT
    HOLIDAY, WEATHER, MAINTENANCE, CONSTRUCTION, POLICE_ACTIVITY, MEDICAL_EMERGENCY
    """

    if 'subreasontype' in message and message['subreasontype'] is not None:
        return {
            '0_1':   Alert.OTHER_CAUSE,
            '26_1':  Alert.TECHNICAL_PROBLEM,
            '26_2':  Alert.MAINTENANCE,
            '23':    Alert.CONSTRUCTION,
            '6':     Alert.ACCIDENT,
            '6_6':   Alert.ACCIDENT,
            '15':    Alert.OTHER_CAUSE,
            '19_1':  Alert.OTHER_CAUSE,
            '7':     Alert.OTHER_CAUSE,
            '6_4':   Alert.MEDICAL_EMERGENCY,
            '20':    Alert.OTHER_CAUSE,
            '17':    Alert.OTHER_CAUSE,
            '3_9':   Alert.POLICE_ACTIVITY,
            '4':     Alert.POLICE_ACTIVITY,
            '3_15':  Alert.OTHER_CAUSE,
            '24_6':  Alert.OTHER_CAUSE,
            '24_7':  Alert.OTHER_CAUSE,
            '24_8':  Alert.HOLIDAY,
            '24_9':  Alert.OTHER_CAUSE,
            '24_10': Alert.OTHER_CAUSE,
            '24_11': Alert.HOLIDAY,
            '24_12': Alert.OTHER_CAUSE,
            '24_1':  Alert.OTHER_CAUSE,
            '24_13': Alert.OTHER_CAUSE,
            '3_17':  Alert.POLICE_ACTIVITY,
            '3_1':   Alert.POLICE_ACTIVITY,
            '3_11':  Alert.POLICE_ACTIVITY,
            '6_3':   Alert.ACCIDENT,
            '16':    Alert.TECHNICAL_PROBLEM,
            '24_14': Alert.OTHER_CAUSE,
            '18':    Alert.ACCIDENT,
            '23_1':  Alert.OTHER_CAUSE,
            '23_2':  Alert.OTHER_CAUSE,
            '23_3':  Alert.OTHER_CAUSE,
            '23_4':  Alert.OTHER_CAUSE,
            '16':    Alert.OTHER_CAUSE,
            '255':   Alert.UNKNOWN_CAUSE, # Ook Weersomstandigheden
            '24_15': Alert.OTHER_CAUSE,
            '24_16': Alert.OTHER_CAUSE,
            '5':     Alert.STRIKE,
            '4':     Alert.OTHER_CAUSE,
            '5':     Alert.STRIKE,
            '6':     Alert.STRIKE,
            '5_1':   Alert.STRIKE,
            '7':     Alert.MAINTENANCE,
            '14':    Alert.CONSTRUCTION,
            '14_1':  Alert.CONSTRUCTION,
            '8_4':   Alert.OTHER_CAUSE,
            '6_2':   Alert.TECHNICAL_PROBLEM,
            '12_1':  Alert.TECHNICAL_PROBLEM,
            '8_1':   Alert.TECHNICAL_PROBLEM,
            '5':     Alert.ACCIDENT,
            '4':     Alert.TECHNICAL_PROBLEM,
            '8_10':  Alert.TECHNICAL_PROBLEM,
            '12':    Alert.TECHNICAL_PROBLEM,
            '8_11':  Alert.TECHNICAL_PROBLEM,
            '4_1':   Alert.TECHNICAL_PROBLEM,
            '8_12':  Alert.TECHNICAL_PROBLEM,
            '8_13':  Alert.TECHNICAL_PROBLEM,
            '11_2':  Alert.CONSTRUCTION,
            '9':     Alert.CONSTRUCTION,
            '11_2':  Alert.CONSTRUCTION,
            '9_1':   Alert.WEATHER,
            '9_2':   Alert.WEATHER,
            '3':     Alert.WEATHER,
            '14':    Alert.OTHER_CAUSE,
            '5':     Alert.WEATHER,
            '9_3':   Alert.WEATHER,
            '255_1': Alert.WEATHER,

        }[message['subreasontype']]

    elif 'reasontype' in message and message['reasontype'] is not None:
        return {
            '0':   Alert.UNKNOWN_CAUSE,
            '1':   Alert.OTHER_CAUSE,
            '2':   Alert.OTHER_CAUSE,
            '3':   Alert.MAINTENANCE,
            '4':   Alert.OTHER_CAUSE,
            '255': Alert.UNKNOWN_CAUSE,

        }[message['reasontype']]

    else:
        return Alert.UNKNOWN_CAUSE

def getEffectFromEffect(message):
    """
    NO_SERVICE, REDUCED_SERVICE, SIGNIFICANT_DELAYS, DETOUR, ADDITIONAL_SERVICE,
    MODIFIED_SERVICE, OTHER_EFFECT, UNKNOWN_EFFECT, STOP_MOVED
    """

    if 'subeffecttype' in message and message['subeffecttype'] is not None:
        return {
            '0':    Alert.UNKNOWN_EFFECT,
            '11':   Alert.REDUCED_SERVICE,
            '5':    Alert.NO_SERVICE,
            '4':    Alert.DETOUR,
            '4_1':  Alert.DETOUR,
            '3_1':  Alert.UNKNOWN_EFFECT,
            '3_2':  Alert.SIGNIFICANT_DELAYS,
            '3_3':  Alert.SIGNIFICANT_DELAYS,
            '3_4':  Alert.SIGNIFICANT_DELAYS,
            '3_5':  Alert.SIGNIFICANT_DELAYS,
            '3_6':  Alert.SIGNIFICANT_DELAYS,
            '3_7':  Alert.SIGNIFICANT_DELAYS,
            '3_8':  Alert.SIGNIFICANT_DELAYS,
            '3_9':  Alert.SIGNIFICANT_DELAYS,
            '3_10': Alert.SIGNIFICANT_DELAYS,
            '3_11': Alert.SIGNIFICANT_DELAYS,
            '3_12': Alert.SIGNIFICANT_DELAYS,
            '5_1':  Alert.STOP_MOVED,
            '5_2':  Alert.MODIFIED_SERVICE,

        }[message['subeffecttype']]

    elif 'effecttype' in message and message['effecttype'] is not None:
        return {
            '0':   Alert.UNKNOWN_EFFECT,
            '1':   Alert.OTHER_EFFECT,
            '255': Alert.UNKNOWN_EFFECT,

        }[message['effecttype']]

    else:
        return Alert.UNKNOWN_EFFECT

def getStopStatusByType(messagetype):
    return {
        'ARRIVAL': VehiclePosition.INCOMING_AT,
        'ONSTOP':  VehiclePosition.STOPPED_AT,
        'DEPARTURE': VehiclePosition.IN_TRANSIT_TO,
        'ONROUTE': VehiclePosition.IN_TRANSIT_TO,
        'OFFROUTE': VehiclePosition.IN_TRANSIT_TO,
    }[messagetype]

def removeAlert(feedentity, needle):
    feedentity.id = needle
    feedentity.is_deleted = True

    return feedentity
 

def getAlertKV15(feedentity, message):
    active_period = feedentity.alert.active_period.add()

    active_period.start = int(time.mktime(iso8601.parse_date(message['messagestarttime']).timetuple()))

    if message['messagedurationtype'] == 'ENDTIME':
        active_period.end = int(time.mktime(iso8601.parse_date(message['messageendtime']).timetuple()))

    if 'lineplanningnumbers' not in message or message['lineplanningnumbers'] is None:
        for userstopcode in message['userstopcodes']:
            entityselector = feedentity.alert.informed_entity.add()
            entityselector.stop_id = getStopId(message['dataownercode'], userstopcode)

    else:
        for userstopcode in message['userstopcodes']:
            stop_id = getStopId(message['dataownercode'], userstopcode)
            for lineplanningnumber in message['lineplanningnumbers']:
                entityselector = feedentity.alert.informed_entity.add()
                entityselector.route_id = getRouteId(message['dataownercode'], lineplanningnumber)
                entityselector.stop_id = stop_id

    feedentity.alert.cause = getCauseFromReason(message)
    feedentity.alert.effect = getEffectFromEffect(message)

    if 'messagecontent' in message and message['messagecontent'] is not None and len(message['messagecontent']) > 0:
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

    return feedentity

def getAlertKV17(feedentity, operation, journey, userstopcode = None):
    entityselector = feedentity.alert.informed_entity.add()    
    entityselector.trip.trip_id = getTripId(journey['dataownercode'], journey['lineplanningnumber'], journey['journeynumber'], journey['operatingday'])
    entityselector.trip.start_date = journey['operatingday']
    if userstopcode is None:
        entityselector.trip.schedule_relationship = TripDescriptor.CANCELED
    else:
        entityselector.trip.stop_id = getStopId(journey['dataownercode'], userstopcode)

    feedentity.alert.cause = getCauseFromReason(operation)
    feedentity.alert.effect = getEffectFromEffect(operation)

    if 'reasoncontent' in operation and operation['reasoncontent'] is not None and len(message['reasoncontent']) > 0:
        translation = feedentity.alert.header_text.translation.add()
        translation.text = message['reasoncontent']
        translation.language = 'nl'

    if 'advicecontent' in operation and operation['advicecontent'] is not None and len(message['advicecontent']) > 0:
        translation = feedentity.alert.description_text.translation.add()
        translation.text = message['advicecontent']
        translation.language = 'nl'

    return feedentity
