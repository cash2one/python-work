import os
from pythonFrame.fileTools.FileBase import FileBase
from pythonFrame.fileTools.FileRead import FileRead
from pythonFrame.myUtils.Util import Util
from string import Template

__author__ = 'linzhou208438'
__update__ = '2015/5/10'


class HiveShell(object):
    def __init__(self):
        pass

    def write_hive_shell(self, sql_path, log_path):
        FileBase.file_exist(sql_path)
        Util.file_remove(log_path)
        exec_sql = 'touch %s' % log_path
        os.popen(exec_sql)

        sql = Template(FileRead.file_readlines(sql_path)[0])
        temp_sql = sql.substitute(dtime=Util.get_yesterday())
        exec_sql = "$HIVE_HOME/bin/hive -S -e '%s' > %s" % (temp_sql, log_path)
        os.popen(exec_sql)