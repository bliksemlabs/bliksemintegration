from inserter import insert,version_imported
from copy import deepcopy
from datetime import date, timedelta

def secondssincemidnight(time):
    values = time.split(':')
    seconds = int(values[0])*3600+int(values[1])*60
    if len(values) == 3:
        return seconds + int(values[2])
    else:
        return seconds
  

def getJourneys():
    journeys = []
    journey = { 'privatecode'              : 'OHM:',
                'operator_id'              : 'OHM:',
                'availabilityconditionref' : 'tussen',
                'productcategoryref'       : 'SHUTTLEBUS',
                'noticeassignmentref'      :  None, 
                'departuretime'            :  None,
                'blockref'                 :  None,
                'timedemandgroupref'       :  '1',
                'name'                     :  None,
                'lowfloor'                 :  False,
                'hasliftorramp'            :  False,
                'bicycleallowed'           :  False,
                'ondemand'                 :  False}
    vertrektijden_heen  = ['8:20', '8:33', '8:50', '9:03', '9:33', '10:03', '10:33', '11:03', '11:33', '12:03', '12:33', '13:03', '13:33', '14:03', '14:33', '15:03', '15:33', '16:03', '16:10', '16:33', '16:50', '17:03', '17:20', '17:33', '17:50', '18:03', '18:20', '18:33', '18:50', '19:03', '19:33', '20:03', '20:33', '21:03', '21:33', '22:03', '22:33']
    vertrektijden_terug = ['8:02', '8:15', '8:32', '8:45', '9:15', '9:45', '10:15', '10:45', '11:15', '11:45', '12:15', '12:45', '13:15', '13:45', '14:15', '14:45', '15:15', '15:45', '15:52', '16:15', '16:32', '16:45', '17:02', '17:15', '17:32', '17:45', '18:02', '18:15', '18:32', '18:45', '19:15', '19:45', '20:15', '20:45', '21:15', '21:45', '22:15']
    for tijd in vertrektijden_heen:
        v = deepcopy(journey)
        v['journeypatternref'] = 'HEEN'
        v['departuretime'] = secondssincemidnight(tijd)
        v['privatecode'] += str(secondssincemidnight(tijd))
        v['operator_id'] += str(secondssincemidnight(tijd))
        v['name'] = tijd
        journeys.append(v)
        if tijd >= '15:00':
            w = deepcopy(v)
            w['availabilityconditionref'] = 'start'
            journeys.append(w)
        else:
            w = deepcopy(v)
            w['availabilityconditionref'] = 'eind'
            journeys.append(w)
    for tijd in vertrektijden_terug:
        v = deepcopy(journey)
        v['journeypatternref'] = 'TERUG'
        v['departuretime'] = secondssincemidnight(tijd)
        v['privatecode'] += str(secondssincemidnight(tijd))
        v['operator_id'] += str(secondssincemidnight(tijd))
        v['name'] = tijd
        journeys.append(v)
        if tijd >= '15:00':
            w = deepcopy(v)
            w['availabilityconditionref'] = 'start'
            journeys.append(w)
        else:
            w = deepcopy(v)
            w['availabilityconditionref'] = 'eind'
            journeys.append(w)
    return journeys

def import_zip(path,filename,version):
    data = {}
    data['OPERATOR'] =  { 'OHMSHUTTLE' :   {'privatecode' : 'OHMSHUTTLE',
                               'operator_id' : 'OHMSHUTTLE',
                               'name'        : 'OHM shuttlebus',
                               'phone'       : '0',
                               'url'         : 'http://ohm2013.org/site/',
                               'timezone'    : 'Europe/Amsterdam',
                               'language'    : 'nl'}
                        }
    data['MERGESTRATEGY'] = [{'type' : 'DATASOURCE', 'datasourceref' : '1'}] #This replaces all previous timetables in the database.
    data['DATASOURCE'] = { '1' : {
                          'operator_id' : 'OHM',
                          'name'        : 'OHM shuttlebus',
                          'description' : 'OHM shuttlebus special',
                          'email'       : None,
                          'url'         : None}
                         }
    data['VERSION'] = { '1' : {'privatecode'   : 'OHMSHUTTLE:2013',
                               'datasourceref' : '1',
                               'operator_id'   : 'OHMSHUTTLE:2013',
                               'startdate'     : '2013-07-06',
                               'enddate'       : '2013-08-08',
                               'description'   : 'Speciale dienstregeling shuttle OHM2013'}
                   }
    data['DESTINATIONDISPLAY'] = {'OHMSHUTTLE:OHM2013' : { 'privatecode' : 'OHMSHUTTLE:OHM2013',
                                                           'operator_id' : 'OHMSHUTTLE:OHM2013',
                                                           'name'        : 'Shuttlebus naar OHM2013',
                                                           'shortname'   : 'OHM2013',
                                                           'vianame'     : None},
                                  'OHMSHUTTLE:STATION' : { 'privatecode' : 'OHMSHUTTLE:STATION',
                                                           'operator_id' : 'OHMSHUTTLE:STATION',
                                                           'name'        : 'Shuttlebus naar Station',
                                                           'shortname'   : 'Station',
                                                           'vianame'     : None}
                                  }
                                                        
    data['LINE'] = {'OHMSHUTTLE:1' : {'operatorref'   : 'OHMSHUTTLE',
                                      'privatecode'   : 'OHMSHUTTLE:1',
                                      'operator_id'   : 'OHMSHUTTLE:1',
                                      'transportmode' : 'BUS',
                                      'publiccode'    : 'OHMShuttle',
                                      'name'          : 'OHM2013 Shuttleservice',
                                      'monitored'     :  False}
                   }
    data['STOPPOINT'] = {'OHM2013' : {'privatecode' : 'OHM2013',
                                      'operator_id' : 'OHMSHUTTLE:OHM2013',
                                      'isscheduled' : True,
                                      'stoparearef' : None,
                                      'name' : 'Shuttle stop OHM2013',
                                      'town' : 'Warmenhuizen',
                                      'latitude'  : 52.694511,
                                      'longitude' : 4.755342},
                         'STATION' :  {'privatecode' : 'STATION',
                                      'operator_id' : 'OHMSHUTTLE:STATION',
                                      'isscheduled' : True,
                                      'stoparearef' : None,
                                      'name' : 'Station Heerhugowaard',
                                      'town' : 'Heerhugowaard',
                                      'latitude'  : 52.669693,
                                      'longitude' : 4.824374}
                         }
    data['STOPAREA'] = {}
    d1 = date(2013,7,27)
    d2 = date(2013,8,07)
    data['AVAILABILITYCONDITION'] = { 'start' : {'operator_id'  : 'OHMSHUTTLE:startdag',
                                                 'privatecode' : 'startdag',
                                                 'unitcode'     : 'OHM2013',
                                                 'versionref'   : '1',
                                                 'fromdate'     : '2013-07-26',
                                                 'todate'       : '2013-07-26',
                                                 'DAYS'         : {'validdates' : ['2013-07-26']}
                                                },
                                      'tussen' : {'operator_id'  : 'OHMSHUTTLE:tussendagen',
                                                 'unitcode'     : 'OHM2013',
                                                 'privatecode' : 'startdag',
                                                 'versionref'   : '1',
                                                 'fromdate'     : str(d1),
                                                 'todate'       : str(d2),
                                                 'DAYS'         : {'validdates' : [str(d1 + timedelta(days=x)) for x in range((d2-d1).days + 1)]}
                                                },
                                      'eind' :  {'operator_id'  : 'OHMSHUTTLE:einddag',
                                                 'unitcode'     : 'OHM2013',
                                                 'privatecode' : 'startdag',
                                                 'versionref'   : '1',
                                                 'fromdate'     : '2013-08-08',
                                                 'todate'       : '2013-08-08',
                                                 'DAYS'         : {'validdates' : ['2013-08-08']}
                                                }
                                    }
    data['PRODUCTCATEGORY'] = {'SHUTTLEBUS' : {'operator_id' : 'SHUTTLEBUS',
                                               'privatecode' : 'SHUTTLEBUS',
                                               'shortname'   : 'Shuttlebus',
                                               'name'        : 'Shuttlebus'}
                              }
    data['ADMINISTRATIVEZONE'] = {'1' : {'operator_id' : 'OHMSHUTTLE:1',
                                         'privatecode' : 'OHMSHUTTLE:1',
                                         'name'        : 'OHM2013 shuttlevervoer',
                                         'description' : 'OHM2013 organistatie'}
                                 }
    data['TIMEDEMANDGROUP'] = {'1' : {'operator_id' : 'OHMSHUTTLE:1',
                                      'privatecode' : 'OHMSHUTTLE',
                                      'POINTS' : [{'pointorder' : 1,'totaldrivetime' : 0    ,'stopwaittime':0},
                                                  {'pointorder' : 2,'totaldrivetime' : 780  ,'stopwaittime':0}]
                                     }
                              }
    data['ROUTE'] = {'HEEN' : {'operator_id' : 'OHMSHUTTLE:heen',
                              'lineref'     : 'OHMSHUTTLE:1',
                              'POINTS'      : [{'pointorder':  1, 'latitude' : 52.669693,'longitude' : 4.824374,'distancefromstart' : 0},
                                               {'pointorder':  2, 'latitude' : 52.694511,'longitude' : 4.755342,'distancefromstart' : 8200},
                                              ]
                              },
                     'TERUG' : {'operator_id' : 'OHMSHUTTLE:terug',
                              'lineref'     : 'OHMSHUTTLE:1',
                              'POINTS'      : [{'pointorder':  1, 'latitude' : 52.694511,'longitude' : 4.755342,'distancefromstart' : 0},
                                               {'pointorder':  2, 'latitude' : 52.669693,'longitude' : 4.824374,'distancefromstart' : 8200},
                                              ]
                             }
                    }
    data['JOURNEYPATTERN'] = {'HEEN' : {'operator_id'  : 'OHMSHUTTLE:heen',
                                        'routeref'     : 'HEEN',
                                        'directiontype': '1',
                                        'destinationdisplayref': 'OHMSHUTTLE:OHM2013',
                                        'POINTS'      : [{'pointorder':  1, 'pointref' : 'STATION', 'onwardpointref' : 'OHM2013', 'iswaitpoint' : True, 'forboarding' : True, 'foralighting' : False,'destinationdisplayref' : 'OHMSHUTTLE:OHM2013'},
                                                         {'pointorder':  2, 'pointref' : 'OHM2013', 'onwardpointref' : None, 'iswaitpoint' : False, 'forboarding' : False, 'foralighting' : True,'destinationdisplayref' : 'OHMSHUTTLE:OHM2013','distancefromstartroute':8200},
                                                        ]
                                       },
                              'TERUG' : {'operator_id' : 'OHMSHUTTLE:terug',
                                         'routeref'     : 'TERUG',
                                         'directiontype': '2',
                                         'destinationdisplayref': 'OHMSHUTTLE:STATION',
                                         'POINTS'      : [{'pointorder':  1, 'pointref' : 'OHM2013', 'onwardpointref' : 'STATION', 'iswaitpoint' : True, 'forboarding' : True, 'foralighting' : False,'destinationdisplayref' : 'OHMSHUTTLE:STATION'},
                                                          {'pointorder':  2, 'pointref' : 'STATION', 'onwardpointref' : None, 'iswaitpoint' : False, 'forboarding' : False, 'foralighting' : True,'destinationdisplayref' : 'OHMSHUTTLE:STATION','distancefromstartroute':8200},
                                                        ]
                                       }
                              }
    data['JOURNEY'] = getJourneys()
    data['NOTICEASSIGNMENT'] = {}
    data['NOTICE'] = {}
    data['NOTICEGROUP'] = {}
    insert(data)

import_zip(None,None,None)
