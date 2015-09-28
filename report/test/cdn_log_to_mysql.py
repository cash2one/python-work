#!/usr/bin/env python
import MySQLdb
import datetime
import sys
import subprocess
import string

dbinfos = {
    "qs": {
        "host": "10.10.58.45",
        "port": 3306,
        "user": "envideo9",
        "password": "Ro520564",
        "db": "envideo9",
        "mycharset": "utf8"
    },
    
    "qstest": {
        "host": "10.10.82.150",
        "port": 3307,
        "user": "P2pStatus",
        "password": "P2pReport",
        "db": "pstest",
        "mycharset": "gbk"
    }             
}

prehour = -1

def getDBConnection(db='qstest'):
    '''
    get connection
    '''
    dbinfo = dbinfos[db]
    try:
        conn = MySQLdb.connect(dbinfo["host"], dbinfo["user"], dbinfo["password"], dbinfo["db"], dbinfo["port"], charset=dbinfo["mycharset"])
        cursor = conn.cursor()
        return conn, cursor
    except Exception,e:
        import traceback
        print e
        traceback.print_exc()

def process(field,appendSql):
    dt = datetime.datetime.now()
    dt = dt + datetime.timedelta(hours = prehour)
    c_time = dt.strftime("%Y-%m-%d %H:00:00")
    ctime = dt.strftime("%Y%m%d%H")
    cmonth = dt.strftime("%Y%m")
    print ctime,c_time,cmonth
    try:
        rely = subprocess.Popen("/opt/scripts/rely/rely.sh rely cdn_log_to_hive.sh %s" % ctime,shell=True)
        rely.wait()
        sql = "set hive.cli.print.header=false;select plat,nettypecode,logipcode,sum(%s),count(%s) from cdntime where dt = '%s'%s group by plat,nettypecode,logipcode" % (field,field,ctime,appendSql)
        output = "/tmp/cdntime_%s" % ctime
        cmd = "/home/qs/scripts/loop_hive.sh -s \"%s\" -m 'cdntime2db %s error!' -o %s -c true" % (sql, c_time, output)
        print cmd
        p = subprocess.Popen(cmd,shell=True)
        p.wait()
        tfile = open(output)
        conn, cursor = getDBConnection()
        for line in tfile:    
            linepart = line.replace("\n","").split("\t")
            if len(linepart) == 5:
                try:
                    if len(linepart[0]) == 0:
                        linepart[0] = -1
                    if len(linepart[1]) == 0:
                        linepart[1] = -1
                    if len(linepart[2].strip()) > 2:
                        province = linepart[2][0:2]
                        city = linepart[2][2:]
                    else:
                        province = linepart[2][0:2]
                        if len(province.strip()) == 0:
                            province = -1
                        city = -1
                    pass
                    if len(linepart[3]) == 0:
                        linepart[3] = -1
                    if len(linepart[4]) == 0:
                        linepart[4] = -1
                    sql = string.Template("insert into b_cdntime_monthly_${month}(dtime,plat,nettype,province,city,${field}_all,${field}_count)values('${dtime}','${plat}',${nettype},${province},${city},${all},${count}) on duplicate key update ${field}_all = values(${field}_all), ${field}_count = values(${field}_count)")
                    sql = sql.safe_substitute({'month':cmonth,'dtime':c_time,'plat':linepart[0],'field':field,'province':province,'city':city,'nettype':linepart[1],'all':linepart[3],'count':linepart[4]})
                    print sql
                    result = cursor.execute(sql)
                    conn.commit()
                except Exception,sqle:
                    print sql
                    import traceback
                    print sqle
                    traceback.print_exc()
                    conn.rollback()
        conn.close()
        print "end insert ", field
    except Exception,e:
        import traceback
        print e
        traceback.print_exc()

if __name__ == '__main__':
    if len(sys.argv) == 2:
        prehour = int(sys.argv[1])
        print "current prehour:", prehour
    pass
    try:
        process("hotvrs",' and adinfo != -1')
    except Exception,e:
        import traceback
        print e
        traceback.print_exc()
    try:
        process("adinfo",' and adinfo != -1')
    except Exception,e:
        import traceback
        print e
        traceback.print_exc()
    try:
        process("adget",' and adget != -1')
    except Exception,e:
        import traceback
        print e
        traceback.print_exc()
    try:
        process("allot",' and allot != -1')
    except Exception,e:
        import traceback
        print e
        traceback.print_exc()
    try:
        process("cdnget",' and cdnget != -1')
    except Exception,e:
        import traceback
        print e
        traceback.print_exc()