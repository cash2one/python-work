from pythonFrame.dbEngine.MysqlWrap import MysqlWrap
from pythonFrame.fileTools.FileRead import FileRead
from pythonFrame.myUtils.Util import Util
from report.ReportEntity import ReportADExtraEntity

__author__ = 'linzhou208438'
__update__ = '2015/7/3'


class AdReadFile(FileRead):
    def __init__(self, monitorReport):
        FileRead.__init__(self, monitorReport)
        self.monitorReport=monitorReport

    def callback(self, line, context, path):
        kvs = line.split()
        if len(kvs) < 2: return 0
        vid = kvs[0]
        if len(vid) < 7: return 0
        if self.map_store.has_key(vid):
            report = self.map_store.get(vid)
        else:
            report = ReportADExtraEntity().__dict__
        try:
            report['video_id'] = int(vid)
        except:
            return 0

        if path.find("ORACLE_SQL") > -1:
            report['all_stock'] = int(kvs[12])
            report['stock'] = int(kvs[13])
            report['all_occu'] = int(kvs[14])
            report['all_t_occu'] = int(kvs[15])
            report['occu'] = int(kvs[16])
            report['t_occu'] = int(kvs[17])
            report['vv'] = int(kvs[18])
            report['all_vv'] = int(kvs[19])
        report['dtime']=self.monitorReport.mydate.get_now()
        self.map_store.setdefault(vid, report)
        return None


