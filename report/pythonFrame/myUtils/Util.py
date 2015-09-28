# -*- coding: utf-8 -*-
from itertools import izip
import os
import time
import datetime
from types import FunctionType
import urllib
import urllib2
import functools


__author__ = 'linzhou208438'
__update__ = '2015/5/9'


class Util(object):

    @classmethod
    def printf(self, value, debug=True):
        if debug:
            print(time.strftime('%Y-%m-%d %H:%M:%S') + ' %s' % value)

    @classmethod
    def get_yesterday(cls, format=None, delta=1):
        today = datetime.date.today()
        yesterday = today + datetime.timedelta(days=-delta)
        if format == None:
            return yesterday.strftime('%Y%m%d')
        elif format == '-':
            return yesterday.strftime('%Y-%m-%d')
        elif format == 'ym':
            return yesterday.strftime('%Y%m')

    @classmethod
    def file_remove(self, path):
        if os.path.exists(path):
            os.remove(path)

    @classmethod
    def invert_dict(self, d):
        return dict(izip(d.itervalues(), d.iterkeys()))

    @classmethod
    def send_message(self, phones=(), msg=''):
        url = 'http://qs.hd.sohu.com.cn/ppp/sns.php'
        for phone in phones:
            params = {'key': 'x1@9eng', 'dest': '', 'mess': ''}
            params['dest'] = phone
            params['mess'] = msg
            data = urllib.urlencode(params)
            req = urllib2.Request(url, data)
            response = urllib2.urlopen(req)
            the_page = response.read()

class MyDate(object):

     def __init__(self,date_var=datetime.date.today().strftime('%Y%m%d')):
        self.year=str(date_var)[0:4]
        self.month=str(date_var)[4:6]
        self.day=str(date_var)[6:8]
        self.now = datetime.date(int(self.year), int(self.month) ,int(self.day))

     def get_now(self, format=None):
        yesterday = self.now
        if format == None:
            return yesterday.strftime('%Y%m%d')
        elif format == '-':
            return yesterday.strftime('%Y-%m-%d')
        elif format == 'ym':
            return yesterday.strftime('%Y%m')

class DynamicMethod(type):
    def __new__(cls, name, bases, dct):
        for name, value in dct.iteritems():
            if name not in ('__metaclass__', '__init__', '__module__') and\
                type(value) == FunctionType:
                value = DynamicMethod.timefn(value)

            dct[name] = value
        return type.__new__(cls, name, bases, dct)

    @classmethod
    def check_required(self,func):
        Util.printf('check some condition')
        return func

    @classmethod
    def timefn(self,fn):
        @functools.wraps(fn)
        def measure_time(*args,**kwargs):
            t1=time.time()
            result=fn(*args,**kwargs)
            t2=time.time()
            Util.printf('call %s(): took %s seconds' % (fn.__name__,str(t2-t1)))
            return result
        return measure_time


if __name__ == '__main__':
    msg = u"this is a illegal param".encode("utf-8")
    Util.send_message(phones="13693046284".split(","),msg=msg)