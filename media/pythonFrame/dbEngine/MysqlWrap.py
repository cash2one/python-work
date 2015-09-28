import MySQLdb
from pythonFrame.dbEngine.DataBase import DataBase
from pythonFrame.myUtils.MetaClass import DynamicMethod
from pythonFrame.myUtils.Util import Util

__author__ = 'linzhou208438'


class MysqlWrap(DataBase):

    __metaclass__ = DynamicMethod

    def __init__(self, conn_params):
        DataBase.__init__(self, conn_params)

    def get_conn_qs(self):
        try:
            conn = MySQLdb.connect(host=self.host, user=self.user, passwd=self.passwd,
                                   db=self.db, charset=self.charset, port=self.port)
            return conn
        except Exception, e:
            Util.printf(e)

    def mysql_executemany(self, sql, datalist):
        conn = self.get_conn_qs()
        cursor = conn.cursor()
        try:
            datalen = len(datalist) + 1
            for i in range(datalen):
                if i % 500 == 0:
                    n = cursor.executemany(sql, datalist[i:500 + i])
                    data_leave = datalen - i - 500
                    prefix = str("data_leave:%s" % data_leave).ljust(20)
                    postfix = str("| commit_already:%s" % n).ljust(20)
                    Util.printf(prefix + postfix)
                    conn.commit()
        except Exception, e:
            Util.printf(e)
            import traceback
            traceback.print_exc()
        finally:
            cursor.close()
            conn.close()

    def mysql_execute(self, sqllist, datalist):
        conn = self.get_conn_qs()
        cursor = conn.cursor()
        try:
            for  sql in sqllist:
                tmpsql = sql % tuple(datalist)
                n = cursor.execute(tmpsql)
                conn.commit()
                Util.printf('n=%s | sql=%s' % (n, tmpsql))
        except Exception, e:
            Util.printf(e)
        finally:
            cursor.close()
            conn.close()

    def mysql_select(self,sql):
        conn = self.get_conn_qs()
        cursor = conn.cursor()
        try:
            n = cursor.execute(sql)
            Util.printf('n=%s | sql=%s' % (n, sql))
            return cursor.fetchall()
        except Exception, e:
            Util.printf(e)
        finally:
            cursor.close()
            conn.close()

    def mysql_execute_new(self, sql, datalist):
        conn = self.get_conn_qs()
        cursor = conn.cursor()
        try:
            for  data in datalist:
                tmpsql = sql % tuple(data)
                n = cursor.execute(sql,tuple(data))
                conn.commit()
                Util.printf('n=%s | sql=%s' % (n, tmpsql))
        except Exception, e:
            Util.printf(e)
        finally:
            cursor.close()
            conn.close()






