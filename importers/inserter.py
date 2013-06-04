import psycopg2
import psycopg2.extras
from settings.const import *
import md5
import logging

logger = logging.getLogger("importer")

def dictequal(dictnew,dictold,ignore_keys=[]):
    for k in dictnew:
        if k in ignore_keys:
            continue
        if k not in dictold:
            print k + str(dictold) + str(dictnew)
            return False
        elif str(dictold[k]) != str(dictnew[k]):
            if k in ['latitude','longitude','rd_x','rd_y'] or (k not in ['stoparearef'] and len(k) > 3 and k[-3:] == 'ref'):
              try:
                if float(dictold[k]) != float(dictnew[k]):
                    return False
              except:
                logger.error('%s: %s==%s' % (k,dictold[k],dictnew[k]),exc_info=True)
                raise
            else:
                print k + str(dictold) + str(dictnew)
                return False
    return True

def simple_dict_insert(conn,table,dict_item,check_existing=True,return_id=True):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    columns = dict_item.keys()
    if table == 'STOPAREA':
        ignore_keys = ['latitude','longitude']
    else:
        ignore_keys = []
    if check_existing:
        query = "SELECT * from %s WHERE operator_id = %%s" % table
        cur.execute(query,[str(dict_item['operator_id'])])
        for dictold in cur.fetchall():
            if dictequal(dict_item,dictold,ignore_keys=ignore_keys):
                dict_item['id'] = dictold['id']
                return (True,dictold['id'])
    if not return_id:
        query = "INSERT INTO %s (%s) VALUES (%s)" % (table,','.join(columns),','.join(['%s' for i in range(len(columns))]))
        cur.execute(query,[dict_item[key] for key in columns])
        cur.close()
        return False
    else:
        query = "INSERT INTO %s (%s) VALUES (%s) returning id" % (table,','.join(columns),','.join(['%s' for i in range(len(columns))]))
        cur.execute(query,[dict_item[key] for key in columns])
        id = cur.fetchone()['id']
        dict_item['id'] = id
        cur.close()
        return (False,id)


def simple_dictdict_insert(conn,table,dictionary):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if table == 'STOPAREA':
        ignore_keys = ['latitude','longitude']
    else:
        ignore_keys = []
    for dict_key,item in dictionary.items():
        columns = item.keys()
        query = "SELECT * from %s WHERE operator_id = %%s" % table
        cur.execute(query,[str(item['operator_id'])])
        record_exists = False
        for dictold in cur.fetchall():
           if dictequal(item,dictold,ignore_keys=ignore_keys):
               record_exists = True
               dictionary[dict_key] = dictold['id']
        if record_exists:
           continue
        query = "INSERT INTO %s (%s) VALUES (%s) returning id" % (table,','.join(columns),','.join(['%s' for i in range(len(columns))]))
   
        cur.execute(query,[item[key] for key in columns])
        id = cur.fetchone()['id']
        item['id'] = id
        dictionary[dict_key] = id
    cur.close()

def checkIfExistingVersion(conn,dictionary):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    for key,item in dictionary.items():
        columns = item.keys()
        query = "SELECT * from version WHERE operator_id = %s"
        cur.execute(query,[item['operator_id']])
        if len(cur.fetchall()) > 0:
            raise Exception('Version already imported')

def import_availabilityconditions(conn,data):
    for key,item in data['AVAILABILITYCONDITION'].items():
        validdays = None
        if 'DAYS' in item:
            validdays = item['DAYS']
            del(item['DAYS'])
        item['versionref'] = data['VERSION'][item['versionref']]
        exists,id = simple_dict_insert(conn,'AVAILABILITYCONDITION',item)
        data['AVAILABILITYCONDITION'][key] = id
        if exists or validdays is None:
            continue
        availabilityday = {'availabilityconditionRef' : id, 'isavailable' : True}
        for day in validdays['validdates']:
            availabilityday['validdate'] = day
            simple_dict_insert(conn,'AVAILABILITYCONDITIONDAY',availabilityday,check_existing=False)

def import_routes(conn,routes):
    for key,item in routes.items():
        points = item['POINTS']
        del(item['POINTS'])
        exists,id = simple_dict_insert(conn,'ROUTE',item)
        routes[key] = id
        if exists:
            continue
        for point in points:
            point['routeref'] = id
            exists = simple_dict_insert(conn,'POINTINROUTE',point,check_existing=False,return_id=False)

def import_timedemandgroups(conn,timedemandgroups):
    for key,item in timedemandgroups.items():
        points = item['POINTS']
        del(item['POINTS'])
        exists,id = simple_dict_insert(conn,'TIMEDEMANDGROUP',item)
        timedemandgroups[key] = id
        if exists:
            continue
        for point in points:
            point['timedemandgroupref'] = id
            exists = simple_dict_insert(conn,'POINTINTIMEDEMANDGROUP',point,check_existing=False,return_id=False)

def setRefsDict(item,reftable,columnname,ignore_null=False):
    if columnname in item:
        if ignore_null and item[columnname] is None:
            return
        item[columnname] = reftable[item[columnname]]

def setRefs(table,reftable,columnname,ignore_null=False):
    for key,item in table.items():
        setRefsDict(item,reftable,columnname,ignore_null=ignore_null)

def import_journeypatterns(conn,data):
    setRefs(data['JOURNEYPATTERN'],data['ROUTE'],'routeref')
    setRefs(data['JOURNEYPATTERN'],data['DESTINATIONDISPLAY'],'destinationdisplayref')
    for code,pattern in data['JOURNEYPATTERN'].items():
        points = pattern['POINTS']
        for point in pattern['POINTS']:
            setRefsDict(point,data['DESTINATIONDISPLAY'],'destinationdisplayref')
            setRefsDict(point,data['STOPPOINT'],'pointref')
            setRefsDict(point,data['STOPPOINT'],'onwardpointref',ignore_null=True)
            if 'ADMINISTRATIVEZONE' in data:
                setRefsDict(point,data['ADMINISTRATIVEZONE'],'administrativezoneref',ignore_null=True)
            setRefsDict(point,data['NOTICEASSIGNMENT'],'noticeassignmentref',ignore_null=True)
        m = md5.new()
        m.update(str(pattern))
        pattern['privatecode'] = m.hexdigest()
        del(pattern['POINTS'])
        exists,id = simple_dict_insert(conn,'JOURNEYPATTERN',pattern)
        data['JOURNEYPATTERN'][code] = id
        if exists:
            continue
        for point in points:
            point['journeypatternref'] = id
            exists = simple_dict_insert(conn,'POINTINJOURNEYPATTERN',point,check_existing=False,return_id=False)

"""
{'haswifi': None, 'departuretime': None, 'operator_id': '5:EBS:w103:      3906:Sunday:22103:7001', 'operatorref': 'EBS', 'name': 7001, 'ondemand': 
None, 'lowfloor': True, 'blockref': None, 'departuretime': 32640, 'journeypatternref': 'EBS:22103:        58', 'noticeassignmentref': None, 
'productcategoryref': None, 'biycleallowed': None, 'hasliftorramp': True, 'availabilityconditionref': 'EBS:w103:      3906:Sunday', 
'timedemandgroupref': 'aad863b6220e60df872628cab1916c07', 'privatecode': 'EBS:22103:7001'}
"""

def import_journeys(conn,data):
    for journey in data['JOURNEY']:
        setRefsDict(journey,data['AVAILABILITYCONDITION'],'availabilityconditionref')
        setRefsDict(journey,data['JOURNEYPATTERN'],'journeypatternref')
        setRefsDict(journey,data['TIMEDEMANDGROUP'],'timedemandgroupref')
        setRefsDict(journey,data['NOTICEASSIGNMENT'],'noticeassignmentref',ignore_null=True)
        setRefsDict(journey,data['PRODUCTCATEGORY'],'productcategoryref')
        exists = simple_dict_insert(conn,'JOURNEY',journey,check_existing=False,return_id=False)
        if exists:
            raise Exception('duplicate journey')

"""
Merge strategies:

{'type' : 'DATASOURCE', datasourceref : '1'}
Datasource, replace entire datasource

{'type' : 'DATASOURCE', datasourceref : '1', fromdate : '2013-01-01'}
Datasource on x, replace entire datasoure starting on data x

{'type' : 'DATASOURCE', datasourceref : '1', fromdate : '2013-01-01', todate : '2014-01-12'}
Datasource between x and y, replace entire datasource between x and y

{'type' : 'UNITCODE', unitcode : 'ARR:LS', fromdate : '2013-01-01'}
Unitcode after x, replace unitcode on days >= x

{'type' : 'UNITCODE', unitcode : 'ARR:LS', fromdate : '2013-01-01', todate : '2014-01-14'}
Unitcode between x and y
"""

def merge(conn,data,mergestrategies):
    cur = conn.cursor()
    for item in mergestrategies:
        if item['type'] == 'DATASOURCE':
            datasource = data['DATASOURCE'][item['datasourceref']]
            print datasource
            if 'fromdate' in item and 'todate' in item:
                cur.execute("""
update availabilityconditionday set isavailable = false 
WHERE availabilityconditionref in (select ac.id from availabilitycondition as ac LEFT JOIN version as v ON (v.id = ac.versionref) WHERE datasourceref = %s)
AND validdate between %s and %s;
                """,[datasource,item['fromdate'],item['todate']])
            elif 'fromdate' in item:
                cur.execute("""
update availabilityconditionday set isavailable = false 
WHERE availabilityconditionref in (select ac.id from availabilitycondition as ac LEFT JOIN version as v ON (v.id = ac.versionref) WHERE datasourceref = %s)
AND validdate >= %s;
                """,[datasource,item['fromdate']])
            else:
                cur.execute("""
update availabilityconditionday set isavailable = false 
WHERE availabilityconditionref in (select ac.id from availabilitycondition as ac LEFT JOIN version as v ON (v.id = ac.versionref) WHERE datasourceref = %s)
                """,[datasource])
        elif item['type'] == 'UNITCODE':
            unitcode = item['unitcode']
            if 'fromdate' in item and 'todate' in item:
                cur.execute("""
update availabilityconditionday set isavailable = false where availabilityconditionref in (select id from availabilitycondition where unitcode = %s) and validdate between %s and %s;
                """,[unitcode,item['fromdate'],item['todate']])
            elif 'fromdate' in item:
                cur.execute("""
update availabilityconditionday set isavailable = false where availabilityconditionref in (select id from availabilitycondition where unitcode = %s) and validdate >= %s;
                """,[unitcode,item['fromdate']])
            else:
                cur.execute("""
update availabilityconditionday set isavailable = false where availabilityconditionref in (select id from availabilitycondition where unitcode = %s)
                """,[unitcode])
    cur.close()

def version_imported(operator_id):
    conn = psycopg2.connect(database_connect)
    cur = conn.cursor()
    cur.execute("""
SELECT true FROM version WHERE operator_id = %s
UNION
SELECT true FROM rejectedversion WHERE operator_id = %s
""",[operator_id] * 2)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return len(result) > 0

def reject(data):
    conn = psycopg2.connect(database_connect)
    try:
        simple_dictdict_insert(conn,'DATASOURCE',data['DATASOURCE'])
        setRefs(data['VERSION'],data['DATASOURCE'],'datasourceref')
        simple_dictdict_insert(conn,'REJECTEDVERSION',data['VERSION'])
    except:
        conn.rollback()
        conn.close()
        raise
    conn.commit()
    conn.close()
    
def insert(data):
    conn = psycopg2.connect(database_connect)
    try:
        checkIfExistingVersion(conn,data['VERSION'])
        simple_dictdict_insert(conn,'DATASOURCE',data['DATASOURCE'])
        merge(conn,data,data['MERGESTRATEGY'])
        simple_dictdict_insert(conn,'OPERATOR',data['OPERATOR'])
        setRefs(data['VERSION'],data['DATASOURCE'],'datasourceref')
        simple_dictdict_insert(conn,'VERSION',data['VERSION'])
        simple_dictdict_insert(conn,'DESTINATIONDISPLAY',data['DESTINATIONDISPLAY'])
        simple_dictdict_insert(conn,'PRODUCTCATEGORY',data['PRODUCTCATEGORY'])
        if 'ADMINISTRATIVEZONE' in data:
            simple_dictdict_insert(conn,'ADMINISTRATIVEZONE',data['ADMINISTRATIVEZONE'])
        setRefs(data['LINE'],data['OPERATOR'],'operatorref')
        simple_dictdict_insert(conn,'LINE',data['LINE'])
        setRefs(data['ROUTE'],data['LINE'],'lineref')
        import_routes(conn,data['ROUTE'])
        import_timedemandgroups(conn,data['TIMEDEMANDGROUP'])
        simple_dictdict_insert(conn,'STOPAREA',data['STOPAREA'])
        setRefs(data['STOPPOINT'],data['STOPAREA'],'stoparearef',ignore_null=True)
        simple_dictdict_insert(conn,'STOPPOINT',data['STOPPOINT'])
        import_availabilityconditions(conn,data) 
        import_journeypatterns(conn,data)
        import_journeys(conn,data)
        conn.commit()
    except:
        conn.rollback()
        conn.close()
        raise
    conn.close()
