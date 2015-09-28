#!/usr/bin/env python
# -*- coding: utf8 -*-
import urllib2
import json
import logging
import MySQLdb

__author__ = 'linzhou208438'
__update__ = '2015/9/6'

logging.basicConfig(format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')

url_test = "http://10.16.15.36:8804/api/bid/auction/recommending.do"
url_online = "http://10.16.15.36:8804/api/bid/auction/recommending.do"

conn_test_params = {"host": "192.168.21.54", "user": "league_stat", "db": "league_front",
                        "passwd": "123456", "charset": "gbk", "port": 3306}

conn_online_params = {"host": "10.10.57.125", "user": "league_front", "db": "league_front",
                        "passwd": "uf0yJ6Gmq0mK3gG", "charset": "gbk", "port": 3306}

phones = ["13693046284"]

def send_message(phone_message):
    url = "http://qs.hd.sohu.com.cn/ppp/sns.php?key=x1@9eng&dest=%s&mess=%s"
    url_fomat = url % phone_message
    urllib2.urlopen(url_fomat).read()


def send_message_group(message):
    map(send_message,[(p,message) for p in phones])

def get_conn_qs(conn_params):
    try:
        host = conn_params.get('host', None)
        user = conn_params.get('user', None)
        db = conn_params.get('db', None)
        passwd = conn_params.get('passwd', None)
        charset = conn_params.get('charset', 'utf8')
        port = conn_params.get('port', 3306)
        return MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset=charset, port=port)
    except Exception, e:
        logging.error(e)

def mysql_select(sql,conn_params):
    conn = get_conn_qs(conn_params)
    cursor = conn.cursor()
    try:
        n = cursor.execute(sql)
        return cursor.fetchall()
    except Exception, e:
        logging.error(e)
        send_message_group("wl_media_ad_bidding_select_error")
    finally:
        cursor.close()
        conn.close()

def mysql_executemany( conn_params,sql, datalist):
    conn = get_conn_qs(conn_params)
    cursor = conn.cursor()
    try:
        datalen = len(datalist) + 1
        for i in range(datalen):
            if i % 500 == 0:
                cursor.executemany(sql, datalist[i:500 + i])
                conn.commit()
    except Exception, e:
        logging.error(e)
        send_message_group("wl_media_ad_bidding_insert_error")
    finally:
        cursor.close()
        conn.close()

def json_request(url,timeout=3):
        try:
            open_url = urllib2.urlopen(url, timeout=timeout)
            if open_url.getcode() == 200:
                page = open_url.read()
                return json.loads(page)
            else:
                logging.error("url request failure !")
        except Exception, e:
            logging.error(e)

def json_parse(jsonVal):
    json_status = jsonVal['status']
    mappings = json_mapping_columns()
    data_map = dict()

    if json_status == 200:
        for msg in jsonVal['message']:
            data = dict()
            for k,v in mappings.items():
                data.setdefault(v,msg.get(k))
            data_map.setdefault(data.get("order_no"),data)
        return data_map
    else:
        logging.error("json return status not equals 200 !")
        logging.error(jsonVal['message'])
        send_message_group("wl_media_ad_bidding_url_request_error")
        return None

def json_mapping_columns():
    mappings = dict()
    mappings.setdefault("orderNo","order_no")
    mappings.setdefault("recommendVideoId","recommend_video_id")
    mappings.setdefault("userId","user_id")
    mappings.setdefault("planRecommendStartTime","plan_recommend_start_time")
    mappings.setdefault("planRecommendEndTime","plan_recommend_end_time")
    mappings.setdefault("planRecommendPlaycount","plan_recommend_playcount")
    mappings.setdefault("recommendUrl","recommend_url")
    return mappings

def generate_insert_sql():
    mappings = json_mapping_columns()
    field_exclude = ["order_no"]
    columns = ",".join(mappings.values())
    values = ", %s" * (mappings.__len__() - 1)
    duplicate_values =  ",".join([ "%s=values(%s)" % (v,v) for v in mappings.values() if v not in field_exclude])
    sql = "insert into wl_media_ad_bidding ("+columns+") values (%s "+values+") ON DUPLICATE KEY UPDATE "+duplicate_values
    return sql

def save_json(jsonParse,conn):
    mappings = json_mapping_columns()
    data_list = []
    for v in jsonParse.values():
        d = list()
        for m in mappings.values():
            d.append(v.get(m))
        data_list.append(tuple(d))

    mysql_executemany(conn,generate_insert_sql(),data_list)


def get_update_order(jsonParse,conn):
    sql = "select order_no from wl_media_ad_bidding"
    mysql_order = [v[0] for v in mysql_select(sql,conn)]
    json_order = jsonParse.keys()
    return set(mysql_order).difference(json_order)

def update_status(jsonParse,conn):
    orders = get_update_order(jsonParse,conn)
    sql = "update wl_media_ad_bidding set status = 0 where order_no=%s"
    if orders is not None and orders.__len__() > 0 :
        mysql_executemany(conn,sql,list(orders))


if __name__ == "__main__":
    url = url_test
    conn = conn_test_params

    #url = url_online
    #conn = conn_online_params

    jsonVal = json_request(url)
    jsonParse = json_parse(jsonVal)

    save_json(jsonParse,conn)
    update_status(jsonParse,conn)

    logging.warning("sucessfull !")

