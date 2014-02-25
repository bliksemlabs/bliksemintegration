from datetime import datetime, timedelta
import math

def make_daterange (monday, tuesday, wednesday, thursday, friday, saturday, sunday, fromdate, todate, exceptions = None):
	if not exceptions:
		exceptions = []

	d1 = datetime.strptime(fromdate, '%Y-%m-%d').date()
	d2 = datetime.strptime(  todate, '%Y-%m-%d').date()

	mask = monday << 0 | tuesday << 1 | wednesday << 2 | thursday << 3 | friday << 4 | saturday << 5 | sunday << 6
	days = 	[]


	"""
	We model here the original exceptions first for this daterange
	"""
	for (exception, weekday) in exceptions:
		if weekday & mask == weekday:
			exception_date = datetime.strptime(exception, '%Y-%m-%d').date()
			if exception_date >= d1 and exception_date <= d2:
				days.append(exception)

	exceptions = [exception for (exception, _as_day) in exceptions]

	"""
	This is the actual selected range, without the exceptions
	"""
	for x in range((d2 - d1).days + 1):
		newdate = d1 + timedelta(days=x)
		weekday = 1 << newdate.weekday()
		newdate = str(newdate)

		if newdate not in exceptions and weekday & mask == weekday:
			days.append(newdate)

	return {'validdates': sorted(days)}

def make_availabilitycondition (operator_id, privatecode, unitcode, versionref, fromdate, todate, DAYS):
	return {privatecode: locals()}

def make_availabilitycondition_range (operator_id, privatecode, unitcode, versionref, fromdate, todate, monday, tuesday, wednesday, thursday, friday, saturday, sunday, exceptions = None):
	return make_availabilitycondition (operator_id, privatecode, unitcode, versionref, fromdate, todate,
	        make_daterange (monday, tuesday, wednesday, thursday, friday, saturday, sunday, fromdate, todate, exceptions))

def make_stoppoint (privatecode, operator_id, isscheduled, stoparearef, name, town, latitude, longitude):
	return {privatecode: locals()}

def make_line (operatorref, privatecode, operator_id, transportmode, publiccode, name, url = None, monitored = False):
	return {privatecode: locals()}

def make_destinationdisplay (privatecode, operator_id, name, shortname, vianame = None):
	return {privatecode: locals()}

def make_version (privatecode, datasourceref, operator_id, startdate, enddate, description):
	return locals()

def make_datasource (operator_id, name, description, email, url):
	return locals()

def make_operator (privatecode, operator_id, name, phone, url, timezone, language):
	return {privatecode: locals()}

def make_mergestrategy (tip, datasourceref):
	return {'type': tip, 'datasourceref': datasourceref}

def make_productcategory (operator_id, privatecode, shortname, name):
	return {privatecode: locals()}

def make_administrativezone (operator_id, privatecode, name, description):
	return {privatecode: locals()}

def make_timedemandgroup (operator_id, privatecode, POINTS):
	return {privatecode: locals()}

def make_route (operator_id, lineref, POINTS):
	return {operator_id: locals()}

def make_journeypattern (operator_id, routeref, directiontype, destinationdisplayref, POINTS):
	return {operator_id: locals()}

def make_journey (privatecode, operator_id, availabilityconditionref, productcategoryref, noticeassignmentref, departuretime, blockref, timedemandgroupref, name, lowfloor, hasliftorramp, bicycleallowed, ondemand, journeypatternref):
	return {privatecode: locals()}

def make_timepoints (points):
	result = []
	pointorder = 0

	for (totaldrivetime, stopwaittime) in points:
		pointorder += 1
		result.append( {'pointorder': pointorder, 'totaldrivetime': totaldrivetime, 'stopwaittime': stopwaittime} )

	return result

def make_routepoints (points):
	result = []
	pointorder = 0

	for (latitude, longitude, distancefromstart) in points:
		pointorder += 1
		result.append( {'pointorder': pointorder, 'latitude': latitude, 'longitude': longitude, 'distancefromstart': distancefromstart} )

	return result

def make_journeypoints (points):
	result = []
	pointorder = 0

	for (pointref, iswaitpoint, forboarding, foralighting, destinationdisplayref) in points:
		pointorder += 1
		if len(points) < pointorder:
			onwardpointref = points[pointorder][0]
		else:
			onwardpointref = None

		result.append( {'pointorder': pointorder, 'pointref': pointref, 'onwardpointref': onwardpointref, 'iswaitpoint': iswaitpoint, 'forboarding': forboarding, 'foralighting': foralighting, 'destinationdisplayref': destinationdisplayref } )

	return result

def make_routepoints_from_journeypattern (journeypattern):
	global data
	results = []
	distancefromstart = 0

	last_point = None

	for item in data['JOURNEYPATTERN'][journeypattern]['POINTS']:
		pointref = item['pointref']
		if last_point:
			this_point = (math.radians(data['STOPPOINT'][pointref]['longitude']), math.radians(data['STOPPOINT'][pointref]['latitude']))
			d_long = last_point[0] - this_point[0]
			d_latt = last_point[1] - this_point[1]
			a = math.sin(d_latt/2)**2 + math.cos(last_point[1]) * math.cos(last_point[1]) * math.sin(d_long/2)**2
			c = 2 * math.asin(math.sqrt(a))
			distancefromstart += 6371 * c
		else:
			last_point = (math.radians(data['STOPPOINT'][pointref]['longitude']), math.radians(data['STOPPOINT'][pointref]['latitude']))

		results.append ( {'pointorder': item['pointorder'], 'latitude': data['STOPPOINT'][pointref]['latitude'], 'longitude': data['STOPPOINT'][pointref]['longitude'], 'distancefromstart': int(distancefromstart)} )

	return results

# print make_daterange ('20140101', '20141231', 1, 0, 0, 0, 0, 0, 0, None)

data = {'OPERATOR': {}, 'MERGESTRATEGY': [], 'DATASOURCE': {}, 'VERSION': {}, 'DESTINATIONDISPLAY': {}, 'LINE': {}, 'STOPPOINT': {},
	'AVAILABILITYCONDITION': {}, 'PRODUCTCATEGORY': {}, 'ADMINISTRATIVEZONE': {}, 'TIMEDEMANDGROUP': {}, 'ROUTE': {}, 'JOURNEYPATTERN': {},
	'JOURNEY': {}, 'NOTICEASSIGNMENT': {}, 'NOTICE': {}, 'NOTICEGROUP': {}}

datasourceref = '1'
versionref = '1'

data['OPERATOR'].update( make_operator ('WPD', 'WPD', 'Wagenborg', '+31854011008', 'http://www.wpd.nl/', 'Europe/Amsterdam', 'nl') )
data['MERGESTRATEGY'].append ( make_mergestrategy ('DATASOURCE', '1') )
data['DATASOURCE'][datasourceref] = make_datasource ('WPD', 'Wagenborg', 'Wagenborg Veren', None, None)
data['VERSION'][versionref] = make_version ('WPD:2014', datasourceref, 'WPD:2014', '2014-01-01', '2014-12-31', 'Handmatige dienstregeling van Wagenborg')

data['DESTINATIONDISPLAY'].update( make_destinationdisplay ('WPD:AM', 'WPD:AM', 'Holwerd - Ameland', 'Ameland') )
data['DESTINATIONDISPLAY'].update( make_destinationdisplay ('WPD:HO', 'WPD:HO', 'Ameland - Holwerd', 'Holwerd') )
data['DESTINATIONDISPLAY'].update( make_destinationdisplay ('WPD:LO', 'WPD:LO', 'Schiermonnikoog - Lauwersoog', 'Lauwersoog') )
data['DESTINATIONDISPLAY'].update( make_destinationdisplay ('WPD:SC', 'WPD:SC', 'Lauwersoog - Schiermonnikoog', 'Schiermonnikoog') )

data['LINE'].update( make_line ('WPD', 'WPD:HO|AM', 'WPD:HO|AM', 'BOAT', 'Ameland', 'Holwerd - Ameland', 'http://www.wpd.nl/ameland/dienstregeling/', False) )
data['LINE'].update( make_line ('WPD', 'WPD:LO|SC', 'WPD:LO|SC', 'BOAT', 'Schiermonnikoog', 'Schiermonnikoog - Lauwersoog', 'http://www.wpd.nl/schiermonnikoog/dienstregeling/', False) )

exceptions = [
	('2014-01-01', 6),
	('2014-04-21', 6),
	('2014-05-29', 6),
	('2014-06-09', 6),
	('2014-12-25', 6),
	('2014-12-26', 6),
]

data['AVAILABILITYCONDITION'].update( make_availabilitycondition_range ('WPD:LO|SC|MAZA', 'WPD:LO|SC|MAZA', 'WPD:LO|SC', versionref,
                                                                        '2014-01-01', '2014-12-31', 1, 1, 1, 1, 1, 1, 0, exceptions) )

data['AVAILABILITYCONDITION'].update( make_availabilitycondition_range ('WPD:LO|SC|WEEK', 'WPD:LO|SC|WEEK', 'WPD:LO|SC', versionref,
                                                                        '2014-01-01', '2014-12-31', 1, 1, 1, 1, 1, 1, 1, exceptions) )

data['AVAILABILITYCONDITION'].update( make_availabilitycondition_range ('WPD:AM|HO|WEEK', 'WPD:AM|HO|WEEK', 'WPD:AM|HO', versionref,
                                                                        '2014-01-01', '2014-12-31', 1, 1, 1, 1, 1, 1, 1, exceptions) )

data['AVAILABILITYCONDITION'].update( make_availabilitycondition_range ('WPD:AM|HO|MADO', 'WPD:AM|HO|MADO', 'WPD:AM|HO', versionref,
                                                                        '2014-01-01', '2014-12-31', 1, 0, 0, 1, 0, 0, 0, exceptions) )

data['AVAILABILITYCONDITION'].update( make_availabilitycondition_range ('WPD:AM|HO|VR|0608', 'WPD:AM|HO|VR|0608', 'WPD:AM|HO', versionref,
                                                                        '2014-06-01', '2014-08-31', 0, 0, 0, 0, 1, 0, 0, exceptions) )

data['AVAILABILITYCONDITION'].update( make_availabilitycondition_range ('WPD:AM|HO|VR|0610', 'WPD:AM|HO|VR|0610', 'WPD:AM|HO', versionref,
                                                                        '2014-06-01', '2014-10-31', 0, 0, 0, 0, 1, 0, 0, exceptions) )

data['AVAILABILITYCONDITION'].update( make_availabilitycondition_range ('WPD:AM|HO|VR|0708', 'WPD:AM|HO|VR|0708', 'WPD:AM|HO', versionref,
                                                                        '2014-07-01', '2014-08-31', 0, 0, 0, 0, 1, 0, 0, exceptions) )

data['AVAILABILITYCONDITION'].update( make_availabilitycondition_range ('WPD:AM|HO|ZA|0708', 'WPD:AM|HO|ZA|0708', 'WPD:AM|HO', versionref,
                                                                        '2014-07-01', '2014-08-31', 0, 0, 0, 0, 0, 1, 0, exceptions) )

data['AVAILABILITYCONDITION'].update( make_availabilitycondition_range ('WPD:AM|HO|ZA|0608', 'WPD:AM|HO|ZA|0608', 'WPD:AM|HO', versionref,
                                                                        '2014-06-01', '2014-08-31', 0, 0, 0, 0, 0, 1, 0, exceptions) )

data['AVAILABILITYCONDITION'].update( make_availabilitycondition_range ('WPD:AM|HO|ZAZO|0410', 'WPD:AM|HO|ZAZO|0410', 'WPD:AM|HO', versionref,
                                                                        '2014-04-01', '2014-10-31', 0, 0, 0, 0, 0, 1, 1, exceptions) )

data['AVAILABILITYCONDITION'].update( make_availabilitycondition_range ('WPD:AM|HO|ZO|0608', 'WPD:AM|HO|ZO|0608', 'WPD:AM|HO', versionref,
                                                                        '2014-06-01', '2014-08-31', 0, 0, 0, 0, 0, 0, 1, exceptions) )

data['AVAILABILITYCONDITION'].update( make_availabilitycondition_range ('WPD:AM|HO|ZO|0609', 'WPD:AM|HO|ZO|0609', 'WPD:AM|HO', versionref,
                                                                        '2014-06-01', '2014-09-30', 0, 0, 0, 0, 0, 0, 1, exceptions) )

data['PRODUCTCATEGORY'].update ( make_productcategory ('VEER', 'VEER', 'Veer', 'Veer') )

data['ADMINISTRATIVEZONE'].update ( make_administrativezone ('WPD:1', 'WPD:1', 'Veren Ameland/Schiermonnikoog', 'Ver Ameland/Schiermonnikoog') )

# Both Ferries take 45 mins
data['TIMEDEMANDGROUP'].update ( make_timedemandgroup ('WPD:45M', 'WPD:45M', make_timepoints( [ (0, 0), (60*45, 60*45) ] ) ) )

data['STOPPOINT'].update( make_stoppoint ('WPD:HO', 'WPD:HO', True, None, 'Holwerd', 'Holwerd', 53.3952, 5.8788) )
data['STOPPOINT'].update( make_stoppoint ('WPD:AM', 'WPD:AM', True, None, 'Ameland', 'Ameland', 53.4430, 5.7732) )
data['STOPPOINT'].update( make_stoppoint ('WPD:LO', 'WPD:LO', True, None, 'Lauwersoog', 'Lauwersoog', 53.4106, 6.1972) )
data['STOPPOINT'].update( make_stoppoint ('WPD:SC', 'WPD:SC', True, None, 'Schiermonnikoog', 'Schiermonnikoog', 53.4690, 6.2009) )

data['STOPAREA'] = {}

data['JOURNEYPATTERN'].update ( make_journeypattern ( 'WPD:HO|AM', 'WPD:HO|AM', '1', 'WPD:AM', make_journeypoints ( [('WPD:HO', True, True, False, 'WPD:AM'), ('WPD:AM', False, False, True, 'WPD:AM') ] ) ) )
data['JOURNEYPATTERN'].update ( make_journeypattern ( 'WPD:AM|HO', 'WPD:AM|HO', '2', 'WPD:HO', make_journeypoints ( [('WPD:AM', True, True, False, 'WPD:HO'), ('WPD:HO', False, False, True, 'WPD:HO') ] ) ) )

data['JOURNEYPATTERN'].update ( make_journeypattern ( 'WPD:LO|SC', 'WPD:LO|SC', '1', 'WPD:SC', make_journeypoints ( [('WPD:LO', True, True, False, 'WPD:SC'), ('WPD:SC', False, False, True, 'WPD:SC') ] ) ) )
data['JOURNEYPATTERN'].update ( make_journeypattern ( 'WPD:SC|LO', 'WPD:SC|LO', '2', 'WPD:LO', make_journeypoints ( [('WPD:SC', True, True, False, 'WPD:LO'), ('WPD:LO', False, False, True, 'WPD:LO') ] ) ) )

data['ROUTE'].update ( make_route ( 'WPD:AM|HO', 'WPD:HO|AM', make_routepoints_from_journeypattern ('WPD:AM|HO') ) )
data['ROUTE'].update ( make_route ( 'WPD:HO|AM', 'WPD:HO|AM', make_routepoints_from_journeypattern ('WPD:HO|AM') ) )
data['ROUTE'].update ( make_route ( 'WPD:LO|SC', 'WPD:LO|SC', make_routepoints_from_journeypattern ('WPD:LO|SC') ) )
data['ROUTE'].update ( make_route ( 'WPD:SC|LO', 'WPD:LO|SC', make_routepoints_from_journeypattern ('WPD:SC|LO') ) )


data['JOURNEY'].update ( make_journey ( 'WPD:LO|SC|06', 'WPD:LO|SC|06', 'WPD:LO|SC|MAZA', 'VEER', None,   6 * 3600 + 1800, None, 'WPD:45M', '06:30', True, False, True, False, 'WPD:LO|SC' ))
data['JOURNEY'].update ( make_journey ( 'WPD:LO|SC|09', 'WPD:LO|SC|09', 'WPD:LO|SC|WEEK', 'VEER', None,   9 * 3600 + 1800, None, 'WPD:45M', '09:30', True, False, True, False, 'WPD:LO|SC' ))
data['JOURNEY'].update ( make_journey ( 'WPD:LO|SC|12', 'WPD:LO|SC|12', 'WPD:LO|SC|WEEK', 'VEER', None,  12 * 3600 + 1800, None, 'WPD:45M', '12:30', True, False, True, False, 'WPD:LO|SC' ))
data['JOURNEY'].update ( make_journey ( 'WPD:LO|SC|15', 'WPD:LO|SC|15', 'WPD:LO|SC|WEEK', 'VEER', None,  15 * 3600 + 1800, None, 'WPD:45M', '15:30', True, False, True, False, 'WPD:LO|SC' ))
data['JOURNEY'].update ( make_journey ( 'WPD:LO|SC|18', 'WPD:LO|SC|18', 'WPD:LO|SC|WEEK', 'VEER', None,  18 * 3600 + 1800, None, 'WPD:45M', '18:30', True, False, True, False, 'WPD:LO|SC' ))

data['JOURNEY'].update ( make_journey ( 'WPD:SC|LO|07', 'WPD:SC|LO|07', 'WPD:LO|SC|MAZA', 'VEER', None,   7 * 3600 + 1800, None, 'WPD:45M', '07:30', True, False, True, False, 'WPD:SC|LO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:SC|LO|10', 'WPD:SC|LO|10', 'WPD:LO|SC|WEEK', 'VEER', None,  10 * 3600 + 1800, None, 'WPD:45M', '10:30', True, False, True, False, 'WPD:SC|LO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:SC|LO|13', 'WPD:SC|LO|13', 'WPD:LO|SC|WEEK', 'VEER', None,  13 * 3600 + 1800, None, 'WPD:45M', '13:30', True, False, True, False, 'WPD:SC|LO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:SC|LO|16', 'WPD:SC|LO|16', 'WPD:LO|SC|WEEK', 'VEER', None,  16 * 3600 + 1800, None, 'WPD:45M', '16:30', True, False, True, False, 'WPD:SC|LO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:SC|LO|19', 'WPD:SC|LO|19', 'WPD:LO|SC|WEEK', 'VEER', None,  19 * 3600 + 1800, None, 'WPD:45M', '19:30', True, False, True, False, 'WPD:SC|LO' ))

data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|06_1', 'WPD:AM|HO|06_1', 'WPD:AM|HO|MADO',      'VEER', None,   6 * 3600 + 1800, None, 'WPD:45M', '06:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|06_2', 'WPD:AM|HO|06_2', 'WPD:AM|HO|ZA|0608',   'VEER', None,   6 * 3600 + 1800, None, 'WPD:45M', '06:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|07_1', 'WPD:AM|HO|07_1', 'WPD:AM|HO|VR|0708',   'VEER', None,   7 * 3600 + 1800, None, 'WPD:45M', '07:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|07_2', 'WPD:AM|HO|07_2', 'WPD:AM|HO|ZA|0708',   'VEER', None,   7 * 3600 + 1800, None, 'WPD:45M', '07:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|08_1', 'WPD:AM|HO|08_1', 'WPD:AM|HO|WEEK',      'VEER', None,   8 * 3600 + 1800, None, 'WPD:45M', '08:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|09_1', 'WPD:AM|HO|09_1', 'WPD:AM|HO|VR|0608',   'VEER', None,   9 * 3600 + 1800, None, 'WPD:45M', '09:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|09_2', 'WPD:AM|HO|09_2', 'WPD:AM|HO|ZA|0608',   'VEER', None,   9 * 3600 + 1800, None, 'WPD:45M', '09:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|10_1', 'WPD:AM|HO|10_1', 'WPD:AM|HO|MADO',      'VEER', None,  10 * 3600 + 1800, None, 'WPD:45M', '10:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|10_2', 'WPD:AM|HO|10_2', 'WPD:AM|HO|ZAZO|0410', 'VEER', None,  10 * 3600 + 1800, None, 'WPD:45M', '10:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|11_1', 'WPD:AM|HO|11_1', 'WPD:AM|HO|VR|0608',   'VEER', None,  11 * 3600 + 1800, None, 'WPD:45M', '11:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|11_2', 'WPD:AM|HO|11_2', 'WPD:AM|HO|ZA|0608',   'VEER', None,  11 * 3600 + 1800, None, 'WPD:45M', '11:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|12_1', 'WPD:AM|HO|12_1', 'WPD:AM|HO|WEEK',      'VEER', None,  12 * 3600 + 1800, None, 'WPD:45M', '12:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|13_1', 'WPD:AM|HO|13_1', 'WPD:AM|HO|VR|0610',   'VEER', None,  13 * 3600 + 1800, None, 'WPD:45M', '13:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|13_2', 'WPD:AM|HO|13_2', 'WPD:AM|HO|ZA|0608',   'VEER', None,  13 * 3600 + 1800, None, 'WPD:45M', '13:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|14_1', 'WPD:AM|HO|14_1', 'WPD:AM|HO|WEEK',      'VEER', None,  14 * 3600 + 1800, None, 'WPD:45M', '14:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|15_1', 'WPD:AM|HO|15_1', 'WPD:AM|HO|VR|0610',   'VEER', None,  15 * 3600 + 1800, None, 'WPD:45M', '15:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|15_2', 'WPD:AM|HO|15_2', 'WPD:AM|HO|ZA|0608',   'VEER', None,  15 * 3600 + 1800, None, 'WPD:45M', '15:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|15_3', 'WPD:AM|HO|15_3', 'WPD:AM|HO|ZO|0608',   'VEER', None,  15 * 3600 + 1800, None, 'WPD:45M', '15:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|16_1', 'WPD:AM|HO|16_1', 'WPD:AM|HO|WEEK',      'VEER', None,  16 * 3600 + 1800, None, 'WPD:45M', '16:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|17_1', 'WPD:AM|HO|17_1', 'WPD:AM|HO|VR|0608',   'VEER', None,  17 * 3600 + 1800, None, 'WPD:45M', '17:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|17_2', 'WPD:AM|HO|17_2', 'WPD:AM|HO|ZA|0708',   'VEER', None,  17 * 3600 + 1800, None, 'WPD:45M', '17:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|17_3', 'WPD:AM|HO|17_3', 'WPD:AM|HO|ZO|0609',   'VEER', None,  17 * 3600 + 1800, None, 'WPD:45M', '17:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|18_1', 'WPD:AM|HO|18_1', 'WPD:AM|HO|WEEK',      'VEER', None,  18 * 3600 + 1800, None, 'WPD:45M', '18:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|19_1', 'WPD:AM|HO|19_1', 'WPD:AM|HO|VR|0608',   'VEER', None,  19 * 3600 + 1800, None, 'WPD:45M', '19:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:AM|HO|19_2', 'WPD:AM|HO|19_2', 'WPD:AM|HO|ZA|0708',   'VEER', None,  19 * 3600 + 1800, None, 'WPD:45M', '19:30', True, False, True, False, 'WPD:AM|HO' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|07_1', 'WPD:HO|AM|07_1', 'WPD:AM|HO|MADO',      'VEER', None,   7 * 3600 + 1800, None, 'WPD:45M', '07:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|07_2', 'WPD:HO|AM|07_2', 'WPD:AM|HO|ZA|0608',   'VEER', None,   7 * 3600 + 1800, None, 'WPD:45M', '07:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|08_1', 'WPD:HO|AM|08_1', 'WPD:AM|HO|VR|0708',   'VEER', None,   8 * 3600 + 1800, None, 'WPD:45M', '08:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|08_2', 'WPD:HO|AM|08_2', 'WPD:AM|HO|ZA|0708',   'VEER', None,   8 * 3600 + 1800, None, 'WPD:45M', '08:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|09_1', 'WPD:HO|AM|09_1', 'WPD:AM|HO|WEEK',      'VEER', None,   9 * 3600 + 1800, None, 'WPD:45M', '09:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|10_1', 'WPD:HO|AM|10_1', 'WPD:AM|HO|VR|0608',   'VEER', None,  10 * 3600 + 1800, None, 'WPD:45M', '10:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|10_2', 'WPD:HO|AM|10_2', 'WPD:AM|HO|ZA|0608',   'VEER', None,  10 * 3600 + 1800, None, 'WPD:45M', '10:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|11_1', 'WPD:HO|AM|11_1', 'WPD:AM|HO|MADO',      'VEER', None,  11 * 3600 + 1800, None, 'WPD:45M', '11:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|11_2', 'WPD:HO|AM|11_2', 'WPD:AM|HO|ZAZO|0410', 'VEER', None,  11 * 3600 + 1800, None, 'WPD:45M', '11:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|12_1', 'WPD:HO|AM|12_1', 'WPD:AM|HO|VR|0608',   'VEER', None,  12 * 3600 + 1800, None, 'WPD:45M', '12:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|12_2', 'WPD:HO|AM|12_2', 'WPD:AM|HO|ZA|0608',   'VEER', None,  12 * 3600 + 1800, None, 'WPD:45M', '12:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|13_1', 'WPD:HO|AM|13_1', 'WPD:AM|HO|WEEK',      'VEER', None,  13 * 3600 + 1800, None, 'WPD:45M', '13:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|14_1', 'WPD:HO|AM|14_1', 'WPD:AM|HO|VR|0610',   'VEER', None,  14 * 3600 + 1800, None, 'WPD:45M', '14:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|14_2', 'WPD:HO|AM|14_2', 'WPD:AM|HO|ZA|0608',   'VEER', None,  14 * 3600 + 1800, None, 'WPD:45M', '14:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|15_1', 'WPD:HO|AM|15_1', 'WPD:AM|HO|WEEK',      'VEER', None,  15 * 3600 + 1800, None, 'WPD:45M', '15:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|16_1', 'WPD:HO|AM|16_1', 'WPD:AM|HO|VR|0610',   'VEER', None,  16 * 3600 + 1800, None, 'WPD:45M', '16:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|16_2', 'WPD:HO|AM|16_2', 'WPD:AM|HO|ZA|0608',   'VEER', None,  16 * 3600 + 1800, None, 'WPD:45M', '16:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|16_3', 'WPD:HO|AM|16_3', 'WPD:AM|HO|ZO|0608',   'VEER', None,  16 * 3600 + 1800, None, 'WPD:45M', '16:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|17_1', 'WPD:HO|AM|17_1', 'WPD:AM|HO|WEEK',      'VEER', None,  17 * 3600 + 1800, None, 'WPD:45M', '17:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|18_1', 'WPD:HO|AM|18_1', 'WPD:AM|HO|VR|0608',   'VEER', None,  18 * 3600 + 1800, None, 'WPD:45M', '18:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|18_2', 'WPD:HO|AM|18_2', 'WPD:AM|HO|ZA|0708',   'VEER', None,  18 * 3600 + 1800, None, 'WPD:45M', '18:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|18_3', 'WPD:HO|AM|18_3', 'WPD:AM|HO|ZO|0609',   'VEER', None,  18 * 3600 + 1800, None, 'WPD:45M', '18:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|19_1', 'WPD:HO|AM|19_1', 'WPD:AM|HO|WEEK',      'VEER', None,  19 * 3600 + 1800, None, 'WPD:45M', '19:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|20_1', 'WPD:HO|AM|20_1', 'WPD:AM|HO|VR|0608',   'VEER', None,  20 * 3600 + 1800, None, 'WPD:45M', '20:30', True, False, True, False, 'WPD:HO|AM' ))
data['JOURNEY'].update ( make_journey ( 'WPD:HO|AM|20_2', 'WPD:HO|AM|20_2', 'WPD:AM|HO|ZA|0708',   'VEER', None,  20 * 3600 + 1800, None, 'WPD:45M', '20:30', True, False, True, False, 'WPD:HO|AM' ))

import simplejson as json
print json.dumps(data)
