import os
from pythonFrame.actionShell.ThreadPool import ThreadPool
from pythonFrame.myUtils.MetaClass import DynamicMethod
from pythonFrame.myUtils.Util import Util

__author__ = 'linzhou208438'
__update__ = '2015/8/10'


class HiveShell(ThreadPool):


    def __init__(self,q,cpu=8):
        self.q = q
        ThreadPool.__init__(self,cpu)

    def hive_shell_exe(self, sql, log_path):
        Util.file_mkdirs(os.path.dirname(log_path))
        exec_sql = 'touch %s' % log_path
        os.popen(exec_sql)

        exec_sql = str('$HIVE_HOME/bin/hive -S -e "%s" > %s' % (sql, log_path))
        os.popen(exec_sql)

        self.q.put_singal(log_path[os.path.dirname(log_path).__len__()+1:])

    @DynamicMethod.timefn
    def hive_shell_wrap(self,sqls):
        sql = sqls.split("&")
        Util.printf('sql=%s | log=%s \n' % (sql[0], sql[1]))
        self.hive_shell_exe(str(sql[0]),sql[1])

    @DynamicMethod.timefn
    def batch_hive_shell(self,sqls,path,dt):
        params = []
        for sql in sqls:
            sql = sql.replace("&","&"+path)
            params.append(sql % str(dt))

        self.thread_start(self.hive_shell_wrap,params)


if __name__ == "__main__":
     path = "/opt/logs/a.txt"
     print path[os.path.dirname(path).__len__()+1:]
