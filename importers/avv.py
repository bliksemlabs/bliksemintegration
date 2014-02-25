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


def import_zip(path,filename,version):
    conn = psycopg2.connect("dbname='dino'")
    prefix = 'AVV'
    try:
        data = {}
        data['OPERATOR'] = getOperator(conn,prefix=prefix,website='http://www.avv.de')
        data['MERGESTRATEGY'] = [{'type' : 'DATASOURCE', 'datasourceref' : '1'}]
        data['DATASOURCE'] = getDataSource()
        data['VERSION'] = getVersion(conn,prefix=prefix)
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
