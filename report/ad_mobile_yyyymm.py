from pythonFrame.dbEngine.OracleWrap import OracleWrap
from pythonFrame.myUtils.Util import Util

__author__ = 'linzhou208438'
__update__ = '2015/7/3'


class ReportOracle(OracleWrap):
    conn_oracle_params = {"host": "10.10.34.48", "user": "leilinhai", "db": "videodb",
                          "passwd": "linhailei"}

    def __init__(self, mydate,conn_report_params=None):
        OracleWrap.__init__(self, conn_report_params)
        self.mydate=mydate

    def write_rows_file(self, path):
        sql = 'select v_id,sum(max_adv),sum(uv),sum(view_all_uv),sum(stock),sum(occu),sum(pos1),sum(pos2),sum(pos3),sum(pos4),sum(pos5),sum(t_occu) from dwpdata.core_mobile_pgc where sver is not null and substr(sver,0,1)>=5 and  data_date=%s group by v_id '
        Util.printf(sql % self.monitorReport.mydate.get_now() )
        rows = self.oracle_fetchall(sql % self.mydate.get_now())
        self.save_rows(rows, path)

    def save_rows(self, rows, path):
        file_write = open(path, 'w')
        val = []
        for row in rows:
            for i in range(12):
                val.append(str(row[i]))
            file_write.write("  ".join(val) + "\n")
            val = []
        file_write.close()