import threading
from pythonFrame.myUtils.Util import Util, MyDate

__author__ = 'linzhou208438'


class Monitor(object):
    def __init__(self,map_store={}):
        self.mydate=MyDate()
        self.map_store = map_store  # storage data row
        self.uids = {}  # vid:uid
        self.urls_parallel_total = 0  # parallel urls length
        self.urls_serial_total = 0  # serial urls length
        self.urls_parallel_running = []  # parallel urls (needed to be request)
        self.urls_serial_running = []  # serial urls (needed to be request)
        self.urls_parallel_timeout = []  # parallel request url timeout list
        self.urls_serial_timeout = []  # serial request url timeout list
        self.urls_parallel_timeout_retry = []  # parallel request url timeout retry list
        self.urls_serial_timeout_retry = []  # serial request url timeout retry list
        self.urls_retry_failure = []  # urls request retry failure list
        self.begin_time = None  # json begin request record
        self.uid_share_map = {}  # record  uid:is_video_share

    def monitor_start(self, path):
        t1 = threading.Thread(target=self.monitor_run, args=(path,))
        t1.setDaemon(True)
        threading.currentThread().setDaemon(True)
        t1.start()


class Template(object):
    def __init__(self):
        pass