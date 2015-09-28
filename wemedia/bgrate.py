#!/usr/bin/env python
# -*- coding: utf8 -*-
__author__ = 'zhou lin'
__version__ = "$Revision: 1.0 $"
__update__ = "$Date: 2015-04-18 $"
import MySQLdb
import datetime
from decimal import Decimal
import urllib
import time
import os
import re
import sys

#store all data
map={}

pattern =re.compile(r'^(?:[\x00-\x7f]|[\xe0-\xef][\x80-\xbf]{2})+$')
my=urllib.unquote
def unescape(str):
    try:
        code=None
        upperstr=str.upper()
        if upperstr.find(r"%U")==-1:
            value=my(str).decode('iso-8859-1')
            match = pattern.match(value) 
            if match:
                code="match:utf-8"
                return my(str).decode('utf8').encode('utf8')
            else:
                code="not match:gbk"
                try:
                    return my(str).decode('GBK').encode('utf8')
                except:
                    #pass
                    return my(str).decode('utf8').encode('utf8')                  
        else:
            code="utf8"
            return my(str).decode('utf8').encode('utf8')
    except Exception, e:
        #return 0
        print "code=%s || value=%s || exc=%s" % (code,str,e) 
    
   
    
def readKey(path='mts'):
    if not os.path.exists(path):
        return 
    file_object = open(path)
    lines= file_object.readlines()
    file_object.close()
    try:
        for line in lines:
            kvs=line.split()
            if len(kvs)<2:
                continue
            
            value=kvs[1].strip()
            if value == 'NULL':
                continue
            value=unescape(value)  
            rate={'keyword':'','search':0,'web':0,'sohu':0,'ifox':0,'box':0,'video1':0,'video2':0}
            if map.has_key(value):
                rate=map.get(value)
                rate['search']=rate['search'] + int(kvs[0])     
            else:
                rate['keyword']=value
                rate['search']=int(kvs[0])
            map.setdefault(value,rate)
    finally:
        file_object.close()
            
            

def readFile(path=None):
    if not os.path.exists(path):
        return
    file_object = open(path)
    lines= file_object.readlines()
    try:
        for line in lines:
            kvs=line.split()
            if len(kvs)<2:
                continue
            
            value=kvs[1].strip()
            if value == 'NULL':
                continue
            value=unescape(value)  
            rate={'keyword':'','search':0,'web':0,'sohu':0,'ifox':0,'box':0,'video1':0,'video2':0}
            if map.has_key(value):
                rate=map.get(value)
                if path.find("web")>-1:
                    rate["web"]=rate["web"]+int(kvs[0])
                elif path.find("sohu")>-1:
                    rate["sohu"]=rate["sohu"]+int(kvs[0])
                elif path.find("ifox")>-1:
                    rate["ifox"]=rate["ifox"]+int(kvs[0])
                elif path.find("box")>-1:
                    rate["box"]=rate["box"]+int(kvs[0])
                elif path.find("video1")>-1:
                    rate["video1"]=rate["video1"]+int(kvs[0])
                elif path.find("video2")>-1:
                    rate["video2"]=rate["video2"]+int(kvs[0])           
            else:
                rate['keyword']=value
                rate['search']=1
            map.setdefault(value,rate)
    finally:
        file_object.close()

conn4testParams = {"host": "10.16.15.178", \
              "user": "root", \
              "db": "test", \
              "passwd": "root", \
              "charset": "utf8", \
              "port": 3306
}

conn4btsParams = {"host": "192.168.60.70", \
              "user": "search_user", \
              "db": "search_bts", \
              "passwd": "search_user", \
              "charset": "utf8", \
              "port": 3306
}

def getConn4Qs(conn4rateParams=None):
    try:
        conn = MySQLdb.connect(host=conn4rateParams['host'], user=conn4rateParams['user'], passwd=conn4rateParams['passwd'],
                               db=conn4rateParams['db'], charset=conn4rateParams['charset'], port=conn4rateParams['port'])
        return conn
    except Exception, e:
        print e
        
        
def insertBgRate(conn, map=None):
    cursor = conn.cursor()
    sql = ' insert into bg_rate(keyword,search,passive,box,ifox,web,sohu,click,passive_rate, \
                box_rate,ifox_rate,web_rate,sohu_rate,click_rate,create_date,whole,page_one,page_other) \
                 values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    delsql = ' delete from bg_rate_whole where create_date=%s '
    try:
        datalist=[]
        for rate in map.values() :
            datalist.append((str(rate['keyword']),\
                    rate['search'], \
                    rate['search']-rate['box'], \
                    rate['box'], \
                    rate['ifox'], \
                    rate['web'], \
                    rate['sohu'], \
                    rate['sohu']+rate['web']+rate['ifox'],\
                    float(rate['search']-rate['box'])/rate['search'], \
                    float(rate['box'])/rate['search'], \
                    float(rate['ifox'])/rate['search'], \
                    float(rate['web'])/rate['search'],  \
                    float(rate['sohu'])/rate['search'],  \
                    float(rate['sohu']+rate['web']+rate['ifox'])/rate['search'], \
                    getYesterday(), \
                    0, \
                    rate['video1'] , \
                    rate['video2']))
            
        cursor.execute(delsql,getYesterday())
        datalen=len(datalist)+1
        #print datalen
        for i in range(datalen):
            if i % 500 == 0:
                n = cursor.executemany(sql,datalist[i:500+i])
                conn.commit()
        
        
        cursor.close()
    except Exception, e:
        print e
        
def insertBgRateWhole(conn):
    cursor = conn.cursor()
    sql = ' insert into bg_rate_whole(search,passive,box,ifox,web,sohu,click,passive_rate,box_rate,ifox_rate,\
            web_rate,sohu_rate,click_rate,create_date) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            
    searchSql = "SELECT SUM(search) FROM bg_rate WHERE create_date=%s AND click>0"
    sumSql='SELECT sum(box) as box,sum(ifox) as ifox,sum(web) as web,sum(sohu) as sohu,SUM(click) as click FROM bg_rate WHERE create_date=%s and click>0'

    try:
        n=cursor.execute(searchSql,getYesterday())
        search=None
        if n != None:
            search=cursor.fetchone()
        
        n=cursor.execute(sumSql,getYesterday())
        
        if n != None and search != None:
            sum=cursor.fetchone()
            search=int(search[0])
            cursor.execute(sql, (search ,\
            search-sum[0], \
            sum[0], \
            sum[1], \
            sum[2], \
            sum[3], \
            sum[4], \
            float(search-sum[0])/search , \
            float(sum[0])/search , \
            float(sum[1])/search , \
            float(sum[2])/search , \
            float(sum[3])/search , \
            float(sum[4])/search , \
            getYesterday()))
        conn.commit()
        cursor.close()
    except Exception, e:
        print e
        
def getYesterday(format=None,delta=1):
    today = datetime.date.today()
    yesterday = today + datetime.timedelta(days=-delta)
    if len(sys.argv) > 3:
        year=int(sys.argv[1])
        month=int(sys.argv[2])
        day=int(sys.argv[3])
        yesterday=datetime.date(year,month,day)
    if format == None:
        return yesterday.strftime('%Y%m%d')
    elif format == '-':
        return yesterday.strftime('%Y-%m-%d')
        
if __name__ == '__main__':
    map.clear()
    temp = sys.stdout
    sys.stdout = open('/home/qs/scripts/search_daily/error.log','w')
    dt = getYesterday()
    logdir="/home/qs/scripts/search_daily/"+str(dt)+"/"
    print logdir 
    readKey(logdir+"mts_key.log")
    #print map
    readFile(logdir+"sohu_key.log")
    readFile(logdir+"web_key.log")
    readFile(logdir+"ifox_key.log")
    readFile(logdir+"video1_key.log")
    readFile(logdir+"video2_key.log")
    readFile(logdir+"box_key.log")
    
    #print map
    insertBgRate(getConn4Qs(conn4btsParams),map)
    insertBgRateWhole(getConn4Qs(conn4btsParams))

    sys.stdout.close()
    sys.stdout = temp #resotre print

    
