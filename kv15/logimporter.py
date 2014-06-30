import psycopg2
import psycopg2.extras
from reader import parsemessage
import zmq
from gzip import GzipFile
from cStringIO import StringIO

ZMQ_SUB = "tcp://post.ndovloket.nl:7658"
ZMQ_SUB = "tcp://127.0.0.1:7806"

context = zmq.Context()
receiver = context.socket(zmq.SUB)
receiver.connect(ZMQ_SUB)
receiver.setsockopt(zmq.SUBSCRIBE, '/GVB/KV15')
receiver.setsockopt(zmq.SUBSCRIBE, '/EBS/KV15')
receiver.setsockopt(zmq.SUBSCRIBE, '/RIG/KV15')
receiver.setsockopt(zmq.SUBSCRIBE, '/VTN/KV15')
receiver.setsockopt(zmq.SUBSCRIBE, '/HTM/KV15')
receiver.setsockopt(zmq.SUBSCRIBE, '/OPENOV/KV15')

def simple_dict_insert(conn,table,dict_item):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    columns = dict_item.keys()
    query = "INSERT INTO %s (%s) VALUES (%s)" % (table,','.join(columns),','.join(['%s' for i in range(len(columns))]))
    cur.execute(query,[dict_item[key] for key in columns])
    cur.close()
    return False

def delete_message(conn,message):
    cur = conn.cursor()
    cur.execute("""
UPDATE kv15.stopmessage SET isdeleted = true
WHERE messagecodenumber = %(messagecodenumber)s AND messagecodedate = %(messagecodedate)s AND dataownercode = %(dataownercode)s;""",message)
    cur.close()
    return

def upsert_kv15(conn,message):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
DELETE FROM kv15.stopmessage 
WHERE messagecodenumber = %(messagecodenumber)s AND messagecodedate = %(messagecodedate)s AND dataownercode = %(dataownercode)s;""",message)
    columns = message.keys()
    query = "INSERT INTO kv15.stopmessage (%s) VALUES (%s)" % (','.join(columns),','.join(['%s' for i in range(len(columns))]))
    cur.execute(query,[message[key] for key in columns])
    cur.close()
    return False

def insert_kv15(conn,messages):
    if 'STOPMESSAGE' in messages:
        for message in messages['STOPMESSAGE']:
          try:
            userstopcodes = message['userstopcodes']
            del(message['userstopcodes'])
            lineplanningnumbers = message['lineplanningnumbers']
            del(message['lineplanningnumbers'])
            key = ':'.join(str(x) for x in [message['dataownercode'],message['messagecodedate'],message['messagecodenumber']])
            upsert_kv15(conn,message)
            conn.commit()
            for lineplanningnumber in lineplanningnumbers:
               try:
                dict = {'dataownercode'     : message['dataownercode'],
                        'messagecodedate'   : message['messagecodedate'],
                        'messagecodenumber' : message['messagecodenumber'],
                        'lineplanningnumber': lineplanningnumber}
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("""
DELETE FROM kv15.stopmessage_lineplanningnumber
WHERE messagecodenumber = %(messagecodenumber)s AND messagecodedate = %(messagecodedate)s AND dataownercode = %(dataownercode)s AND 
lineplanningnumber= %(lineplanningnumber)s;""",dict)
                simple_dict_insert(conn,'kv15.stopmessage_lineplanningnumber',dict)
                conn.commit()
               except:
                pass
            for stopcode in userstopcodes:
              try:
                dict = {'dataownercode'     : message['dataownercode'],
                        'messagecodedate'   : message['messagecodedate'],
                        'messagecodenumber' : message['messagecodenumber'],
                        'userstopcode'      : stopcode}
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("""
DELETE FROM kv15.stopmessage_userstopcode
WHERE messagecodenumber = %(messagecodenumber)s AND messagecodedate = %(messagecodedate)s AND dataownercode = %(dataownercode)s AND userstopcode = 
%(userstopcode)s;""",dict)
                simple_dict_insert(conn,'kv15.stopmessage_userstopcode',dict)
                conn.commit()
              except:
                pass
          except Exception as e:
                print e
    if 'DELETEMESSAGE' in messages:
        for message in messages['DELETEMESSAGE']:
            delete_message(conn,message)
            conn.commit()

while True:
    multipart = receiver.recv_multipart()
    contents = GzipFile('','r',0,StringIO(''.join(multipart[1:]))).read()
    try:
        print contents
        conn = psycopg2.connect("dbname=ridprod")
        insert_kv15(conn,parsemessage(contents))
        conn.commit()
    except Exception as e:
        print e
        conn.close()
