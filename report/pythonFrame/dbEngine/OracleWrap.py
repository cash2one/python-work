import cx_Oracle
from pythonFrame.dbEngine.DataBase import DataBase
from pythonFrame.myUtils.Util import Util, DynamicMethod

__author__ = 'linzhou208438'


class OracleWrap(DataBase):

    def __init__(self, conn_report_params=None):
        DataBase.__init__(self, conn_report_params)

    def get_conn(self):
        try:
            conn = cx_Oracle.connect('%s/%s@%s/%s' % (self.user, self.passwd, self.host, self.db))
            return conn
        except Exception, e:
            Util.printf(e)

    @DynamicMethod.timefn
    def oracle_fetchall(self, sql):
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            return rows
        except Exception, e:
            Util.printf(e)
        finally:
            cursor.close()
            conn.close()

    def read_rows(self, rows, path):
        pass

