from pythonFrame.urlRequest.UrlParallel import JsonParallel
from pythonFrame.urlRequest.UrlSerial import JsonSerial
import time

__author__ = 'linzhou208438'
__update__ = '2015/5/10'


class ReportJsonParallel(JsonParallel):
    def __init__(self, monitor, cpu=8):
        JsonParallel.__init__(self, monitor, cpu)
        self.vid_json_map={}

    def process(self, jsonVal, result):
        if result.find("vids.do") > -1:
            dict_attr_list = jsonVal['msg']['list']
            if dict_attr_list is None or len(dict_attr_list) == 0: return (None, -1)
            dict_attr = dict_attr_list[0]

            report={}
            report.setdefault('cate_code',str(dict_attr['cate_code'])[:3])
            report.setdefault('play_type',dict_attr['single'])
            report.setdefault('video_play_time',dict_attr['length'])
            report.setdefault('is_video_share',2 if str(dict_attr['status']) == "0" else 1)
            report.setdefault('video_title',dict_attr['title'].encode('gbk'))
            self.thread_lock(lambda x,y:self.vid_json_map.setdefault(x,y),str(dict_attr['vid']),report)

        elif result.find("videoinfolist.do") > -1:
            if jsonVal['data'] is None or len(jsonVal['data']) == 0: return (None, -1)
            vid = jsonVal['data'][0]['id']
            uid = jsonVal['data'][0]['userId']
            self.thread_lock(lambda x,y:self.monitor.uids.setdefault(x,y),str(vid), str(uid))

        return (None, 0)

    def post_invoke(self):
        self.retry_timeout()
        self.merge_report()

    def merge_report(self):
        for k,v in self.vid_json_map.items():
            if self.map_store.has_key(k):
                report = self.map_store.get(k)
                for kk in v.keys():
                    report[kk]=v[kk]

        for k,v in self.monitor.uids.items():
            if self.map_store.has_key(k):
                report = self.map_store.get(k)
                report["uid"] = v



class ReportJsonSerial(JsonSerial):
    def __init__(self, monitor, cpu=8):
        JsonSerial.__init__(self, monitor, cpu)
        self.vid_list = []

    def preprocess(self, url):
        base_split = url.split('?')[1]
        and_split = base_split.split('&')
        uid = and_split[0].split('=')[1]
        vid = and_split[1].split('=')[1]
        exitcode = 0

        if self.monitor.uid_share_map.has_key(uid):
            self.thread_lock(lambda x:self.vid_list.append(x),vid)
            exitcode = -1

        return ({'uid': uid, 'vid': vid}, exitcode)

    def process(self, jsonVal, result):
        isAlias = jsonVal['data']['isAlias']
        status = jsonVal['data']['status']
        vid = result.get('vid', 0)
        uid = result.get('uid', 0)
        is_user_share = 2

        if isAlias == 0 and status == 1:
            is_user_share = 1
        if is_user_share == 2:
            self.monitor.uid_share_map.setdefault(uid, is_user_share)
            self.thread_lock(lambda x:self.vid_list.append(x),vid)

        return (None, 0)

    def post_invoke(self):
        self.retry_timeout()
        self.merge_report()

    def merge_report(self):
        for k in self.vid_list:
            if self.map_store.has_key(k):
                report = self.map_store.get(k)
                report["is_user_share"] = 2
