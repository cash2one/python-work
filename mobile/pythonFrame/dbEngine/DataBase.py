import functools
from pythonFrame.myUtils.Util import Util

__author__ = 'linzhou208438'
__update__ = '2015/5/9'


class DataBase(object):

    def __init__(self, conn_params):
        self.host = conn_params.get('host', None)
        self.user = conn_params.get('user', None)
        self.db = conn_params.get('db', None)
        self.passwd = conn_params.get('passwd', None)
        self.charset = conn_params.get('charset', 'utf8')
        self.port = conn_params.get('port', 3306)

    @classmethod
    def logMethod(self,func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            Util.printf('call %s():' % func.__name__)
            return func(*args, **kw)
        return wrapper



