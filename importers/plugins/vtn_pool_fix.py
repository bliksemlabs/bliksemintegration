import psycopg2
import operator
from math import sqrt,fabs,floor

def distance (x1,y1,x2,y2):
    return fabs(sqrt( (x2 - x1)**2 + (y2 - y1)**2 ))

def fix_pool(conn):
    cur = conn.cursor()
    cur.execute("""
select 
string_agg(concat_ws('|',pl.version,pl.dataownercode,userstopcodebegin,userstopcodeend,pl.pointcode,distancesincestartoflink,pt.locationx_ew,pt.locationy_ns,ST_DISTANCE(st_makepoint(pt.locationx_ew,pt.locationy_ns),st_makepoint(ut.locationx_ew,ut.locationy_ns))),'\n')
FROM pool as pl, point as pt,point as ut
WHERE
pl.pointdataownercode = pt.dataownercode AND
pl.version = pt.version AND
pl.pointcode = pt.pointcode AND
pl.version = ut.version AND
pl.userstopcodebegin = ut.pointcode
GROUP BY pl.version,pl.dataownercode,userstopcodebegin,userstopcodeend
""")
    for row in cur.fetchall():
        pool = [x.split('|') for x in row[0].split('\n')]
        for p in pool:
            for i in [0,5,6,7]:
                p[i] = int(p[i])
            p[8] = float(p[8])
        if len(pool) == 2 and pool[1][5] != 0:
            continue
        pool = sorted(pool, key=operator.itemgetter(8))
        pool[0][5] = 0
        clean_pool = [pool[0]]
        del(pool[0])
        while len(pool) > 0:
            pointer = 0
            min_dist = 999999991
            for i in range(len(pool)):
                dist = distance(pool[i][6],pool[i][7],clean_pool[-1][6],clean_pool[-1][7])
                if pointer is None or dist < min_dist:
                    pointer = i
                    min_dist = dist
            pool[pointer][5] = int(floor(min_dist + clean_pool[-1][5]))
            clean_pool.append(pool[pointer])
            pool.remove(pool[pointer])
        for p in clean_pool:
            cur.execute("update pool set distancesincestartoflink = %s where version = %s and userstopcodebegin = %s and userstopcodeend = %s and pointcode = %s",[p[5],p[0],p[2],p[3],p[4]])
