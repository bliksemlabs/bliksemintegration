from dino import *
from inserter import insert,version_imported
import psycopg2

def getDataSource():
    return { '1' : {
                          'operator_id' : 'AVV',
                          'name'        : 'AVV Dino leveringen',
                          'description' : 'AVV Dino levering',
                          'email'       : None,
                          'url'         : None}}

"""
delete from branch;
delete from calendar_of_the_company;
delete from day_type_2_day_attribute;
delete from lid_course;
delete from lid_travel_time_type;
delete from notice;
delete from rec_footpath;
delete from rec_lin_ber;
delete from rec_stop;
delete from rec_stop_area;
delete from rec_stopping_points;
delete from rec_trip;
delete from service_restriction;
delete from set_day_attribute;
delete from set_day_type;
delete from set_vehicle_type;
delete from set_version;
copy branch from '/home/thomas/rid/x/branch.din' CSV HEADER delimiter ';';
copy calendar_of_the_company from '/home/thomas/rid/x/calendar_of_the_company.din' CSV HEADER delimiter ';';
copy day_type_2_day_attribute from '/home/thomas/rid/x/day_type_2_day_attribute.din' CSV HEADER delimiter ';';
copy lid_course from '/home/thomas/rid/x/lid_course.din' CSV HEADER delimiter ';';
copy lid_travel_time_type from '/home/thomas/rid/x/lid_travel_time_type.din' CSV HEADER delimiter ';';
copy notice from '/home/thomas/rid/x/notice.din' CSV HEADER delimiter ';' ENCODING 'ISO-8859-1';
copy rec_footpath from '/home/thomas/rid/x/rec_footpath.din' CSV HEADER delimiter ';';
copy rec_lin_ber from '/home/thomas/rid/x/rec_lin_ber.din' CSV HEADER delimiter ';' ENCODING 'ISO-8859-1';
copy rec_stop from '/home/thomas/rid/x/rec_stop.din' CSV HEADER delimiter ';' ENCODING 'ISO-8859-1';
copy rec_stop_area from '/home/thomas/rid/x/rec_stop_area.din' CSV HEADER delimiter ';'  ENCODING 'ISO-8859-1';
copy rec_stopping_points from '/home/thomas/rid/x/rec_stopping_points.din' CSV HEADER delimiter ';';
copy rec_trip from '/home/thomas/rid/x/rec_trip.din' CSV HEADER delimiter ';';
copy service_restriction from '/home/thomas/rid/x/service_restriction.din' CSV HEADER delimiter ';' ENCODING 'ISO-8859-1';
copy set_day_attribute from '/home/thomas/rid/x/set_day_attribute.din' CSV HEADER delimiter ';';
copy set_day_type from '/home/thomas/rid/x/set_day_type.din' CSV HEADER delimiter ';';
copy set_vehicle_type from '/home/thomas/rid/x/set_vehicle_type.din' CSV HEADER delimiter ';' ENCODING 'ISO-8859-1';
copy set_version from '/home/thomas/rid/x/set_version.din' CSV HEADER delimiter ';' ENCODING 'ISO-8859-1';
"""

def import_zip(path,filename,version):
    conn = psycopg2.connect("dbname='dino'")
    prefix = 'AVV'
    try:
        data = {}
        data['OPERATOR'] = getOperator(conn,prefix=prefix,website='http://www.avv.de')
        data['MERGESTRATEGY'] = [{'type' : 'DATASOURCE', 'datasourceref' : '1'}]
        data['DATASOURCE'] = getDataSource()
        data['VERSION'] = getVersion(conn,prefix=prefix,filename=filename)
        data['DESTINATIONDISPLAY'] = getDestinationDisplays(conn,prefix=prefix)
        data['LINE'] = getLines(conn,prefix=prefix)
        data['STOPPOINT'] = getStopPoints(conn,prefix=prefix)
        data['STOPAREA'] = getStopAreas(conn,prefix=prefix)
        data['AVAILABILITYCONDITION'] = getAvailabilityConditions(conn,prefix=prefix)
        data['PRODUCTCATEGORY'] = getProductCategories(conn,prefix=prefix)
        data['ADMINISTRATIVEZONE'] = { 'AVV' : {'operator_id' : 'AVV', 'name' : 'Aachener Verkehrsverbund'} }
        data['TIMEDEMANDGROUP'] = getTimeDemandGroups(conn,prefix=prefix)
        data['ROUTE'] = clusterPatternsIntoRoute(conn,prefix=prefix)
        data['JOURNEYPATTERN'] = getJourneyPatterns(conn,data['ROUTE'],prefix=prefix)
        data['JOURNEY'] = getJourneys(conn,prefix=prefix)
        data['NOTICEASSIGNMENT'] = {}
        data['NOTICE'] = {}
        data['NOTICEGROUP'] = {}
        conn.close()
        insert(data)
    except:
        raise
