from pythonFrame.urlRequest.UrlParallel import JsonParallel
from pythonFrame.urlRequest.UrlSerial import JsonSerial

__author__ = 'linzhou208438'
__update__ = '2015/5/10'

class ReportJsonParallel(JsonParallel):
    def __init__(self, monitor,cpu=8):
        JsonParallel.__init__(self, monitor, cpu)
        self.url = 'http://my.tv.sohu.com/wm/u/vids.do?vid=%s'


    def process(self, jsonVal, result):
        dict_attr = jsonVal['msg']['list']
        if dict_attr is None or len(dict_attr) == 0: return (None, -1)
        dict_attr = dict_attr[0]

        report={}
        report.setdefault('cate_code',str(dict_attr['cate_code'])[:3])
        report.setdefault('play_type',dict_attr['single'])
        report.setdefault('video_play_time',dict_attr['length'])
        report.setdefault('is_video_share',2 if str(dict_attr['status']) == "0" else 1)
        report.setdefault('video_title',dict_attr['title'].encode('gbk'))

        self.monitor.map_store.setdefault(str(dict_attr['vid']),report)

        return (None, 0)

    def post_invoke(self):
        self.retry_timeout()

        # union_store = self.monitor.union_store
        # for k,v in self.monitor.map_store.items():
        #     sohu_key = k + "_0"
        #     wuliu_key = k + "_1"
        #
        #     if union_store.has_key(sohu_key):
        #         report = union_store.get(sohu_key)
        #         report.setdefault('cate_code',v.get('cate_code'))
        #         report.setdefault('play_type',v.get('play_type'))
        #         report.setdefault('video_play_time',v.get('video_play_time'))
        #         report.setdefault('video_title',v.get('video_title'))
        #         self.monitor.union_store[sohu_key]=report
        #
        #     if union_store.has_key(wuliu_key):
        #         report = union_store.get(wuliu_key)
        #         report.setdefault('cate_code',v.get('cate_code'))
        #         report.setdefault('play_type',v.get('play_type'))
        #         report.setdefault('video_play_time',v.get('video_play_time'))
        #         report.setdefault('video_title',v.get('video_title'))
        #         self.monitor.union_store[wuliu_key]=report

        #self.mysqlWrap.save_url_vid(self.monitor.map_store)


class ReportJsonSerial(JsonSerial):
    def __init__(self,  monitor,cpu=8):
        JsonSerial.__init__(self, monitor, cpu)
        self.url = 'http://my.tv.sohu.com/user/a/media/userGet.do?uid=%s'

    def process(self, jsonVal, result):
        isAlias = jsonVal['data']['isAlias']
        status = jsonVal['data']['status']
        uid = jsonVal['data']['uid']
        is_user_share = 2
        if isAlias == 0 and status == 1:
            is_user_share = 1

        if is_user_share ==2:
            self.monitor.uid_share_map.setdefault(str(uid), 2)

        return (None, 0)

    def post_invoke(self):
        self.retry_timeout()

        # union_store = self.monitor.union_store
        # for k,v in union_store.items():
        #     uid = str(v.get('uid'))
        #     if self.monitor.uid_share_map.has_key(uid):
        #         v["is_video_share"] = 2
        #     else:
        #         v["is_video_share"] = 1
        #     self.monitor.union_store[k]=v
        #self.mysqlWrap.save_url_uid(self.monitor.uid_share_map)