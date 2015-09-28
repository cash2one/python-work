from pythonFrame.urlRequest.UrlParallel import JsonParallel
from pythonFrame.urlRequest.UrlSerial import JsonSerial

__author__ = 'linzhou208438'
__update__ = '2015/5/10'


class ReportJsonParallel(JsonParallel):
    def __init__(self, monitor, cpu=8):
        JsonParallel.__init__(self, monitor, cpu)

    def process(self, jsonVal, result):
        if result.find("vids.do") > -1:
            dict_attr_list = jsonVal['msg']['list']
            if dict_attr_list is None or len(dict_attr_list) == 0: return (None, -1)
            dict_attr = dict_attr_list[0]
            report = self.map_store.get(str(dict_attr['vid']))
            report['cate_code'] = str(dict_attr['cate_code'])[:3]
            report['play_type'] = dict_attr['single']
            report['video_play_time'] = dict_attr['length']
            report['video_title'] = dict_attr['title'].encode('gbk')
        elif result.find("videoinfolist.do") > -1:
            if jsonVal['data'] is None or len(jsonVal['data']) == 0: return (None, -1)
            vid = jsonVal['data'][0]['id']
            uid = jsonVal['data'][0]['userId']
            self.monitor.uids.setdefault(str(vid), str(uid))
            report = self.map_store.get(str(vid))
            report['uid'] = uid
        return (None, 0)

class ReportJsonSerial(JsonSerial):
    def __init__(self, monitor, cpu=8):
        JsonSerial.__init__(self, monitor, cpu)
        self.uids = monitor.uids

    def preprocess(self, url):
        base_split = url.split('?')[1]
        and_split = base_split.split('&')
        uid = and_split[0].split('=')[1]
        vid = and_split[1].split('=')[1]
        report = self.map_store.get(vid)
        exitcode = 0

        if self.monitor.uid_share_map.has_key(uid):
            report['is_video_share'] = self.monitor.uid_share_map.get(uid, 2)
            exitcode = -1
        return ({'uid': uid, 'vid': vid, 'report': report}, exitcode)

    def process(self, jsonVal, result):
        isAlias = jsonVal['data']['isAlias']
        status = jsonVal['data']['status']
        uid = result.get('uid', 0)
        report = result.get('report')
        is_video_share = 2
        if isAlias == 0 and status == 1:
            is_video_share = 1
        self.monitor.uid_share_map.setdefault(uid, is_video_share)
        report['is_video_share'] = is_video_share
        return (None, 0)