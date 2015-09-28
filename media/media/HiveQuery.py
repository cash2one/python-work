# -*- coding: utf8 -*-
from types import StringType
from pythonFrame.actionShell.HiveShell import HiveShell
from pythonFrame.myUtils.MetaClass import DynamicMethod
from pythonFrame.myUtils.Util import Util

__author__ = 'linzhou208438'
__update__ = '2015/8/20'

class HiveMetaclass(type):

    def __new__(cls, name, bases, attrs):
        mappings = dict()

        for k, v in attrs.iteritems():
             if k not in ('__metaclass__', '__init__', '__module__') and type(v) == StringType:
                v = v.replace("?","%s")
                v = v.replace("\n"," ")
                v = v.replace("\t"," ")
                mappings[k] = v + "&"+ k
        attrs['__sqls__'] = mappings

        return type.__new__(cls, name, bases, attrs)

class HiveQuery(HiveShell):

    __metaclass__ = HiveMetaclass

    HiveCdnDownload = """ select ugu uid,encry_vid vid,is56,sum(bytes) download_bytes from ugc_cdn where substr(dt,0,8)=?
                                and sz=encry_sz and sz!='0_0' and ugu=encry_uid group by ugu,encry_vid,is56"""

    HiveCdnSingleVv = """ select ugu uid,encry_vid vid,is56,count(uid) vv from ( select distinct ugu,encry_vid,uid,is56 from ugc_cdn
                            where substr(dt,0,8)=? and sz=encry_sz and sz!='0_0' and ugu=encry_uid ) t1 group by ugu,encry_vid,is56"""

    HiveCdnUv = """ select ugu uid,encry_vid vid,is56,count(distinct uid) w_cdn_uv from ugc_cdn
                        where substr(dt,0,8)=? and sz=encry_sz and sz!='0_0' and ugu=encry_uid group by ugu,encry_vid,is56"""

    HiveDepth = """ select ugu uid,encry_vid vid, is56, count(case depth when 1 then depth end) as d1,
                         count(case depth when 2 then depth end) as d2, count(case depth when 3 then depth end) as d3,
                         count(case depth when 4 then depth end) as d4, count(case depth when 5 then depth end) as d5,
                         count(case depth when 6 then depth end) as d6, count(case when depth > 6 then depth end) as dother
                         from ugc_pvlog where dt=? and ugu=encry_uid and islast=1 group by ugu,encry_vid,is56"""

    HiveDmSingleVv = """select ugu uid,encry_vid vid,is56,count(uid) vv from ( select distinct ugu,encry_vid,uid,is56 from ugc_vvlog
                          where dt=? and msg='playCount' and sz=md_sz and sz!='0_0' and ugu=encry_uid ) t1 group by ugu,encry_vid,is56"""

    HiveFuv = """select ugu uid,encry_vid vid,is56, count(distinct case msg when 'playCount' then uid end) playcount,
                          count(distinct case msg when 'videoStart' then uid end) videostart,
                          count(distinct case msg when 'videoEnds' then uid end) videoends
                          from ugc_vvlog where dt=? and sz=md_sz and sz!='0_0' and ugu=encry_uid group by ugu,encry_vid,is56"""

    HivePlaytime = """select ugu uid,encry_vid vid,is56,sum(heart_interval/60) playtime from ugc_ptlog
                        where dt=? and sz=md_sz and sz!='0_0' and ugu=encry_uid group by ugu,encry_vid,is56"""

    HivePv = """select ugu uid,encry_vid vid,is56, count(*) as pv from ugc_pvlog
                    where dt=? and ugu=encry_uid group by ugu,encry_vid,is56"""

    HiveUv = """select ugu uid,encry_vid vid,is56, count(distinct fuid) as uv
					 from ugc_pvlog where dt=? and ugu=encry_uid group by ugu,encry_vid,is56"""

    HiveVv = """select ugu uid,encry_vid vid,is56,count(uid) vv from ugc_vvlog where dt=?
                  and msg='playCount' and sz=md_sz and sz!='0_0' and ugu=encry_uid group by ugu,encry_vid,is56"""


    HiveVvIp = """select ugu uid,encry_vid vid,is56,count(distinct ip) ip_count, count(distinct case net_type when '9' then ip end) ip_count_netbar
					 from ugc_vvlog where dt=? and msg='playCount' and sz=md_sz and sz!='0_0' and ugu=encry_uid group by ugu,encry_vid,is56"""



    def __init__(self,path,q,cpu=8,dt=Util.get_yesterday()):
        HiveShell.__init__(self,cpu)
        self.dt = dt
        self.path = path
        HiveShell.__init__(self,q)

    @DynamicMethod.timefn
    def query_hive(self):
        self.batch_hive_shell(self.__sqls__.values(),self.path,self.dt)


if __name__ == "__main__":
    base_log = "/opt/video_report_yyyymm/data"
    hiveQuery = HiveQuery(base_log)
    print hiveQuery.__sqls__