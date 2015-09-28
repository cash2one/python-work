import time

from pythonFrame.myUtils.Util import Util


__author__ = 'linzhou208438'
__update__ = '2015/5/9'

class NiceDict(dict):

    def __init__(self):
        super(NiceDict,self).__init__()


    def __getattr__(self,name):
        if name in self:
            return self[name]
        n=NiceDict()
        super(NiceDict,self).__setitem__(name, n)
        return n

    def __getitem__(self,name):
        if name not in self:
            super(NiceDict,self).__setitem__(name,NiceDict())
        return super(NiceDict,self).__getitem__(name)

    def __setattr__(self,name,value):
        super(NiceDict,self).__setitem__(name,value)



class ContantsMappingColumn(object):
    def __init__(self):
        constants = NiceDict()
        default_values = NiceDict()

        constants.HiveCdnDownload.uid=0
        constants.HiveCdnDownload.video_id=1
        constants.HiveCdnDownload.is56=2
        constants.HiveCdnDownload.down_load_bytes=3

        constants.HiveCdnSingleVv.uid=0
        constants.HiveCdnSingleVv.video_id=1
        constants.HiveCdnSingleVv.is56=2
        constants.HiveCdnSingleVv.w_cdn_single_vv=3

        constants.HiveCdnUv.uid=0
        constants.HiveCdnUv.video_id=1
        constants.HiveCdnUv.is56=2
        constants.HiveCdnUv.w_cdn_uv=3


        constants.HiveDepth.uid=0
        constants.HiveDepth.video_id=1
        constants.HiveDepth.is56=2
        constants.HiveDepth.w_step1=3
        constants.HiveDepth.w_step2=4
        constants.HiveDepth.w_step3=5
        constants.HiveDepth.w_step4=6
        constants.HiveDepth.w_step5=7
        constants.HiveDepth.w_step6=8
        constants.HiveDepth.w_step_other=9

        constants.HiveDmSingleVv.uid=0
        constants.HiveDmSingleVv.video_id=1
        constants.HiveDmSingleVv.is56=2
        constants.HiveDmSingleVv.w_dm_single_vv=3

        constants.HiveFuv.uid=0
        constants.HiveFuv.video_id=1
        constants.HiveFuv.is56=2
        constants.HiveFuv.w_fuv_playcount=3
        constants.HiveFuv.w_fuv_videostart=4
        constants.HiveFuv.w_fuv_videoends=5

        constants.HivePlaytime.uid=0
        constants.HivePlaytime.video_id=1
        constants.HivePlaytime.is56=2
        constants.HivePlaytime.play_time=3

        constants.HivePv.uid=0
        constants.HivePv.video_id=1
        constants.HivePv.is56=2
        constants.HivePv.w_pv=3

        constants.HiveUv.uid=0
        constants.HiveUv.video_id=1
        constants.HiveUv.is56=2
        constants.HiveUv.w_uv=3

        constants.HiveVv.uid=0
        constants.HiveVv.video_id=1
        constants.HiveVv.is56=2
        constants.HiveVv.w_vv=3

        constants.HiveVvIp.uid=0
        constants.HiveVvIp.video_id=1
        constants.HiveVvIp.is56=2
        constants.HiveVvIp.w_ip_count=3

        constants.MysqlDonate.uid=0
        constants.MysqlDonate.video_id=1
        constants.MysqlDonate.is56=2
        constants.MysqlDonate.donate_order_income=3

        constants.OracleAd.uid=0
        constants.OracleAd.video_id=1
        constants.OracleAd.is56=2
        constants.OracleAd.ad_max_adv=3
        constants.OracleAd.ad_uv=4
        constants.OracleAd.ad_view_all_uv=5
        constants.OracleAd.ad_stock=6
        constants.OracleAd.ad_occu=7
        constants.OracleAd.ad_pos1=8
        constants.OracleAd.ad_pos2=9
        constants.OracleAd.ad_pos3=10
        constants.OracleAd.ad_pos4=11
        constants.OracleAd.ad_pos5=12
        constants.OracleAd.ad_t_occu=13

        # constants.URLRequest.is_user_share=None
        # constants.URLRequest.is_video_share=None
        # constants.URLRequest.cate_code=None
        # constants.URLRequest.play_type=None
        # constants.URLRequest.video_play_time=None
        # constants.URLRequest.video_title=None

        default_values.video_title=""
        default_values.is_video_share=1
        default_values.is_user_share=1


        self.myContants = constants
        self.default_values = default_values

    def get_value_by_key(self,key):
        return self.myContants.__getattr__(key)

    def get_unique_value(self):
        ret = set()
        for v in self.myContants.values():
            for k in v.keys():
                ret.add(k)
        return ret

    def get_default_value(self,key):
        return self.default_values.get(key,0)

class ReportADEntity():
    def __init__(self):
        self.ad_view_all_uv = 0
        self.ad_max_adv = 0
        self.ad_uv = 0
        self.ad_stock = 0
        self.ad_occu = 0
        self.ad_t_occu = 0
        self.ad_pos1 = 0
        self.ad_pos2 = 0
        self.ad_pos3 = 0
        self.ad_pos4 = 0
        self.ad_pos5 = 0


class ReportCDNEntity():
    def __init__(self):
        self.down_load_bytes = 0
        self.w_cdn_uv = 0
        self.w_cdn_single_vv = 0

class ReportDMEntity():
    def __init__(self):
        self.w_dm_single_vv = 0
        self.w_vv = 0
        self.invalid_vv = 0
        self.play_time = 0
        self.w_fuv_playcount = 0
        self.w_fuv_videostart = 0
        self.w_fuv_videoends = 0
        self.w_ip_count = 0

class ReportURLEntity():
    def __init__(self):
        self.cate_code = 0
        self.play_type = 0
        self.video_play_time = 0
        self.video_title = ''
        self.uid = 0
        self.is_video_share = 2

class ReportEntity(ReportCDNEntity,ReportDMEntity,ReportADEntity,ReportURLEntity):
    def __init__(self):
        ReportCDNEntity.__init__(self)
        ReportDMEntity.__init__(self)
        ReportADEntity.__init__(self)
        ReportURLEntity.__init__(self)
        self.video_id = 0
        self.play_plat = 1
        self.is_user_share = 1
        self.create_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.dtime = Util.get_yesterday()


if __name__ == '__main__':
     hiveContans = ContantsMappingColumn()
     print hiveContans.myContants.keys().__len__()
     #print hiveContans.get_value_by_key("OracleAd")
     for k,v in hiveContans.myContants.items():
                print "  %s %s" % (str(k).ljust(40),str(v.keys()).rjust(20))

     ret = hiveContans.get_default_value("video_title1")
     print ret

