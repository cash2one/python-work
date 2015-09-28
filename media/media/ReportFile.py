from pythonFrame.fileTools.FileRead import FileRead
from pythonFrame.myUtils.Util import Util

__author__ = 'linzhou208438'
__update__ = '2015/8/10'


class ReportReadFile(FileRead):

    def __init__(self,path,filed,contants,monitor = None,flag = False):
        FileRead.__init__(self,path+"/"+filed)
        self.data_store = []
        self.monitor = monitor
        self.flag = flag
        self.columns_mapping =contants.get_value_by_key(filed)

    def callback(self, line, context):
        kvs = line.split()

        if self.flag:
            self.monitor.uids.add(kvs[0])
            self.monitor.vids.add(kvs[1])
            return None

        if len(kvs) < 3: return 0
        vid = self.columns_mapping.get("video_id")
        is56 = self.columns_mapping.get("is56")
        key = kvs[vid] + "_"+ kvs[is56]

        report = self.monitor.union_store.get(key)
        if report is None:
            report = {}
        for k,v in self.columns_mapping.items():
            report.setdefault(k,kvs[v])

        self.monitor.union_store[key]=report
        return None

    def get_data_store(self):
        return self.data_store


class DataMonitor():
    def __init__(self,path,contants,monitor):
        self.monitor = monitor
        self.path = path
        self.contants = contants
        self.files =contants.myContants.keys()

    def init_monitor(self):
        for f in self.files:
            reportFile = ReportReadFile(self.path,f,self.contants,self.monitor,True)
            reportFile.read_file()