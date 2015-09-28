from pythonFrame.fileTools.FileRead import FileRead
from pythonFrame.myUtils.Util import Util
from report.ReportEntity import ReportEntity

__author__ = 'linzhou208438'
__update__ = '2015/5/10'


class ReportReadFile(FileRead):

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
            report = ReportEntity().__dict__
        try:
            report['video_id'] = int(vid)
        except:
            return 0

        if path.find("CDN_SQL") > -1:
            try:
                download = int(kvs[1])
            except:
                download = int(float(kvs[1]))
            report['down_load_bytes'] = download
            report['w_cdn_uv'] = int(kvs[2])
        elif path.find("VV_SQL") > -1:
            report['play_time'] = int(kvs[1])
            report['w_fuv_playcount'] = int(kvs[2])
            report['w_fuv_videostart'] = int(kvs[3])
            report['w_fuv_videoends'] = int(kvs[4])
        elif path.find("INVALID_SQL") > -1:
            report['invalid_vv'] = int(kvs[1])
        elif path.find("W_SQL") > -1:
            report['w_vv'] = int(kvs[1])
        elif path.find("DM_SQL") > -1:
            report['w_dm_single_vv'] = int(kvs[1])
        elif path.find("CDN_VV_DISTINCT_SQL") > -1:
            report['w_cdn_single_vv'] = int(kvs[1])
        elif path.find("IP_SQL") > -1:
            report['w_ip_count'] = int(kvs[1])
        elif path.find("ORACLE_SQL") > -1:
            if not self.map_store.has_key(vid): return 0
            report['ad_max_adv'] = int(kvs[1])
            report['ad_uv'] = int(kvs[2])
            report['ad_view_all_uv'] = int(kvs[3])
            report['ad_stock'] = int(kvs[4])
            report['ad_occu'] = int(kvs[5])
            report['ad_pos1'] = int(kvs[6])
            report['ad_pos2'] = int(kvs[7])
            report['ad_pos3'] = int(kvs[8])
            report['ad_pos4'] = int(kvs[9])
            report['ad_pos5'] = int(kvs[10])
            report['ad_t_occu'] = int(kvs[11])
        report['dtime']=self.monitorReport.mydate.get_now()
        self.map_store.setdefault(vid, report)
        return None


    def read_keyvalue(self, path):
        update_map = {}
        self.file_exist(path)
        file_object = open(path)
        try:
            for line in file_object:
                kvs = line.split()
                if len(kvs) < 2: continue
                update_map.setdefault(kvs[0], kvs[1])
            return update_map
        except Exception, e:
            Util.printf(e)
        finally:
            file_object.close()