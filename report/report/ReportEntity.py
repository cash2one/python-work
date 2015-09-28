import time

from pythonFrame.myUtils.Util import Util


__author__ = 'linzhou208438'
__update__ = '2015/5/9'

class BaseReportEntity(object):
     def __init__(self):
        pass

     def setValueBySelfField(self,srcObj,destObj):
        vid_exclude={}
        for vid,report in destObj.iteritems():
            if not srcObj.has_key(vid):
                vid_exclude.setdefault(vid,report)
                continue

            srcReport=srcObj.get(vid)
            destReport=destObj.get(vid)
            for field in self.__dict__.keys():
                if not srcReport.has_key(field):continue
                destReport[field]=srcReport[field]

        return vid_exclude


class ReportADEntity(BaseReportEntity):
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

class ReportADExtraEntity(object):
    def __init__(self):
        self.dtime=Util.get_yesterday()
        self.is56=0
        self.all_stock=0
        self.stock=0
        self.all_occu=0
        self.all_t_occu=0
        self.occu=0
        self.t_occu=0
        self.vv=0
        self.all_vv=0

class ReportCDNEntity(BaseReportEntity):
    def __init__(self):
        self.down_load_bytes = 0
        self.w_cdn_uv = 0
        self.w_cdn_single_vv = 0

class ReportDMEntity(BaseReportEntity):
    def __init__(self):
        self.w_dm_single_vv = 0
        self.w_vv = 0
        self.invalid_vv = 0
        self.play_time = 0
        self.w_fuv_playcount = 0
        self.w_fuv_videostart = 0
        self.w_fuv_videoends = 0
        self.w_ip_count = 0

class ReportURLEntity(BaseReportEntity):
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
        self.play_plat = 2
        self.is_user_share = 1
        self.create_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.dtime = Util.get_yesterday()


if __name__ == '__main__':
    re=ReportEntity()
    print ReportEntity().__dict__

    url=ReportEntity()
    srcObj={"123":{"cate_code":1}}
    destObj={"123":{"video_title":2,"cate_code":0},"124":{"video_title":2,"cate_code":0}}
    vid=url.setValueBySelfField(srcObj,destObj)
    print vid
    print destObj