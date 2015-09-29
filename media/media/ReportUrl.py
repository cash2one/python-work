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

class ReportJsonSerial(JsonSerial):
    def __init__(self,  monitor,cpu=8):
        JsonSerial.__init__(self, monitor, cpu)
        self.url = 'http://my.tv.sohu.com/user/a/media/userGet.do?uid=%s'

    def preprocess(self, url):
        base_split = url.split('?')[1]
        uid = base_split.split('=')[1]
        return ({'uid': uid}, 0)

    def process(self, jsonVal, result):
        uid = result.get('uid', 0)
        is_user_share = 2

        if jsonVal['data'] is None or len(jsonVal['data']) == 0:
            self.monitor.uid_share_map.setdefault(str(uid), 2)
            return (None, -1)

        isAlias = jsonVal['data']['isAlias']
        status = jsonVal['data']['status']

        if isAlias == 0 and status == 1:
            is_user_share = 1

        if is_user_share ==2:
            self.monitor.uid_share_map.setdefault(str(uid), 2)

        return (None, 0)

    def post_invoke(self):
        self.retry_timeout()
