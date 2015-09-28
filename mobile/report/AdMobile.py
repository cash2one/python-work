from pythonFrame.dbEngine.OracleWrap import OracleWrap
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

        if path.find("ORACLE_STOCK_SQL") > -1:
            report['all_stock'] = int(kvs[1])
            report['stock'] = int(kvs[2])
            report['all_occu'] = int(kvs[3])
            report['all_t_occu'] = int(kvs[4])
            report['occu'] = int(kvs[5])
            report['t_occu'] = int(kvs[6])
            report['vv'] = int(kvs[7])
            report['all_vv'] = int(kvs[8])
        report['dtime']=self.monitorReport.mydate.get_now()
        self.map_store.setdefault(vid, report)
        return None

class ReportOracleStock(OracleWrap):
    conn_oracle_params = {"host": "10.10.34.48", "user": "leilinhai", "db": "videodb",
                          "passwd": "linhailei"}

    def __init__(self, monitorReport,conn_report_params=None):
        OracleWrap.__init__(self, conn_report_params)
        self.monitorReport=monitorReport

    def write_rows_file(self, path):
        #sql = 'select v_id,max_adv,uv,view_all_uv,stock,occu,pos1,pos2,pos3,pos4,pos5,t_occu from dwpdata.core_mobile_pgc where sver is not null and substr(sver,0,1)>=5 and  data_date=%s'
        sql = 'select v_id,  sum(allstock),sum(stock),SUM(alloccu),sum(alltoccu),sum(occu),sum(t_occu),sum(vv),sum(allvv)  from dwpdata.core_mobile_pgc where  data_date=%s group by v_id '
        Util.printf(sql % self.monitorReport.mydate.get_now() )
        rows = self.oracle_fetchall(sql % self.monitorReport.mydate.get_now())
        self.save_rows(rows, path)

    def save_rows(self, rows, path):
        file_write = open(path, 'w')
        val = []
        for row in rows:
            for i in range(9):
                val.append(str(row[i]))
            file_write.write("  ".join(val) + "\n")
            val = []
        file_write.flush()
        file_write.close()


