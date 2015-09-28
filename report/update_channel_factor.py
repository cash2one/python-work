import sys
sys.path.append('/home/qs/scripts/video_report_yyyymm')

import urllib2
import json
from pythonFrame.dbEngine.MysqlWrap import MysqlWrap


__author__ = 'linzhou208438'
__update__ = '2015/6/19'


class ChannelFactorMysql(MysqlWrap):

    conn_formal_params = {"host": "10.10.57.125", "user": "my_sharing", "db": "my_sharing",
                          "passwd": "jdfcbw73Kru", "charset": "utf8", "port": 3306}

    def __init__(self, conn_params):
        MysqlWrap.__init__(self, conn_params)

    def select_channel_id(self,sql):
        retList = []
        ret = self.mysql_select(sql)
        for r in ret:
            retList.append(r[0])
        return retList


    def query_channel_url(self,url,timeout=3):
        open_url = urllib2.urlopen(url, timeout=timeout)
        if open_url.getcode() == 200:
            page = open_url.read()
            return json.loads(page)

    def parse_json_content(self,json_content):
        id_title={}
        ids = []
        if json_content["status"] == 1:
            data = json_content["data"]
            for d in data:
                if d["level"] == 1:
                    id = d["id"]
                    title = d["title"]
                    id_title.setdefault(id,title)
                    ids.append(id)
        return ids,id_title

    def find_new_channel_map(self,ids,mysqlList,id_title_map):
        retList = [id for id in ids if id not in mysqlList]
        new_ids_title = {}
        for id in retList:
            new_ids_title.setdefault(id,id_title_map.get(id))
        return new_ids_title

    def insert_channel_mysql(self,sql,data):
        dataList = [(id,title,1.0) for id,title in data.items()]
        self.mysql_execute_new(sql,dataList)

if __name__ == '__main__':

    select_sql = ' SELECT channel_id FROM my_sharing.channel_factor '
    insert_sql = ' insert into channel_factor (channel_id,channel_title,factor) values (%s,%s,%s)'
    url = 'http://api.my.tv.sohu.com/cate/getFirst.do'

    #get disticnt channel_id from channel_factor
    channel=ChannelFactorMysql(ChannelFactorMysql.conn_formal_params)
    mysqlList = channel.select_channel_id(select_sql)

    #get json {id,title}
    json_content = channel.query_channel_url(url)
    ids,id_title = channel.parse_json_content(json_content)

    #insert mysql
    new_ids_title = channel.find_new_channel_map(ids,mysqlList,id_title)
    channel.insert_channel_mysql(insert_sql,new_ids_title)




