import psycopg2
import operator
from math import sqrt,fabs,floor

def distance_between (x1,y1,x2,y2):
    return fabs(sqrt( (x2 - x1)**2 + (y2 - y1)**2 ))

def getlocation(conn,version,userstopcode):
    cur = conn.cursor()
    cur.execute("""SELECT locationx_ew::integer,locationy_ns::integer FROM point WHERE version = %s and pointcode = %s""",[version,userstopcode])
    return cur.fetchone()

def set_beginend(conn,pool,userstopcodebegin,userstopcodeend,distance):
    version = pool[0][0]
    daow = pool[0][1]
    linkvalidfrom = pool[0][9]
    if pool[0][4] != userstopcodebegin:
        found = False
        for row in pool:
            if row[4] == userstopcodebegin:
                pool.remove(row)
                pool.insert(0,row)
                found = True
                break
        if found is False:
            x,y = getlocation(conn,version,userstopcodebegin)
            first = [version,daow,userstopcodebegin,userstopcodeend,userstopcodebegin,0,x,y,0]
            pool.insert(0,first)
            cur = conn.cursor()
            cur.execute("""INSERT INTO POOL VALUES ('POOL',%s,'I','VTN',%s,%s,%s,'VTN',%s,0,NULL,NULL,NULL)""",
                           [version,userstopcodebegin,userstopcodeend,linkvalidfrom,userstopcodebegin])
    if pool[-1][4] != userstopcodeend:
        found = False
        for row in pool:
            if row[4] == userstopcodeend:
                pool.remove(row)
                pool.append(row)
                found = True
                break
        if found is False:
            x,y = getlocation(conn,version,userstopcodeend)
            last = [version,daow,userstopcodebegin,userstopcodeend,userstopcodeend,distance,x,y,distance]
            pool.append(last)
            cur = conn.cursor()
            cur.execute("""INSERT INTO POOL VALUES ('POOL',%s,'I','VTN',%s,%s,%s,'VTN',%s,%s,NULL,NULL,NULL)""",
                           [version,userstopcodebegin,userstopcodeend,linkvalidfrom,userstopcodeend,distance])
    
def fix_pool(conn):
    cur = conn.cursor()
    cur.execute("""SELECT version,userstopcodebegin,userstopcodeend,distance::integer from link""")
    for version,userstopcodebegin,userstopcodeend,distance in cur.fetchall():
        print (version,userstopcodebegin,userstopcodeend,distance)
        cur.execute("""
SELECT
pl.version, --0
pl.dataownercode, --1
userstopcodebegin, --2
userstopcodeend, --3
pl.pointcode, --4
distancesincestartoflink::integer, --5
pt.locationx_ew::integer, --6
pt.locationy_ns::integer, --7
ST_DISTANCE(st_makepoint(pt.locationx_ew,pt.locationy_ns),st_makepoint(ut.locationx_ew,ut.locationy_ns))::integer, --8
linkvalidfrom --9
FROM pool as pl, point as pt,point as ut
WHERE
pl.pointdataownercode = pt.dataownercode AND
pl.version = pt.version AND
pl.pointcode = pt.pointcode AND
pl.version = ut.version AND
pl.userstopcodebegin = ut.pointcode AND
pl.userstopcodebegin = %s AND
pl.userstopcodeend = %s AND
pl.version = %s""",[userstopcodebegin,userstopcodeend,version])
        pool = [list(x) for x in cur.fetchall()]
        pool[0][1] = 3
        if len(pool) == 2:
            if pool[0][4] == userstopcodebegin and pool[1][4] == userstopcodebegin:
                if pool[1][5] != distance:
                    raise Exception('2-pool but not right distance')
                continue
        pool = sorted(pool, key=operator.itemgetter(8))
        set_beginend(conn,pool,userstopcodebegin,userstopcodeend,distance)
        clean_pool = [pool[0]]
        del(pool[0])
        while len(pool) > 1:
            pointer = 0
            min_dist = 999999991
            for i in range(len(pool)):
                dist = distance_between(pool[i][6],pool[i][7],clean_pool[-1][6],clean_pool[-1][7])
                if pointer is None or (dist < min_dist and pool[i][3] != pool[i][4]):
                    pointer = i
                    min_dist = dist
            pool[pointer][5] = int(floor(min_dist + clean_pool[-1][5]))
            clean_pool.append(pool[pointer])
            pool.remove(pool[pointer])
        clean_pool.append(pool[-1])
        if clean_pool[-1][5] < clean_pool[-2][5]:
            for j in range(len(clean_pool)):
                i = len(clean_pool)-j-1
                if i > 0 and clean_pool[i][5] < clean_pool[i-1][5]:
                    clean_pool[i-1][5] = clean_pool[i][5]-1
            print clean_pool
        for p in clean_pool[1:-1]:
            cur.execute("update pool set distancesincestartoflink = %s where version = %s and userstopcodebegin = %s and userstopcodeend = %s and pointcode = %s",[p[5],p[0],p[2],p[3],p[4]])
