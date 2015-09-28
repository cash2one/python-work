# -*- coding: utf-8 -*-
from itertools import izip
import os
import time
import datetime
import urllib
import urllib2
import pickle
import multiprocessing


__author__ = 'linzhou208438'
__update__ = '2015/5/9'


class Util(object):

    @classmethod
    def printf(self, value, debug=True):
        if debug:
            name = multiprocessing.current_process().name
            prefix = str(time.strftime('%Y-%m-%d %H:%M:%S') + ',current_process:%s' % name+"-"+str(os.getpid())).ljust(60)
            postfix = str("| %s" % value).ljust(10)
            print prefix + postfix

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
    def file_mkdirs(self, path):
        if not os.path.exists(path):
             os.makedirs(path)


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

    @classmethod
    def store_object(self,inputTree,filename):
        fw = open(filename,'w')
        pickle.dump(inputTree,fw)
        fw.close()

    @classmethod
    def grab_object(self,filename):
        fr = open(filename)
        return pickle.load(fr)

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





if __name__ == '__main__':
    msg = u"this is a illegal param".encode("utf-8")
    str1= "HiveCdnUv"
    str2 = "HiveDmSingleVv"
    Util.printf(str1)
    Util.printf(str2)

    prefix = str(" data_leave:1232").ljust(20)
    postfix = str("| commit_already:%s" % 500).ljust(20)
    Util.printf(prefix + postfix)

    prefix = str("data_leave:732").ljust(20)
    postfix = str("| commit_already:%s" % 500).ljust(20)
    Util.printf(prefix + postfix)
    Util.printf("proccess running".center(50,"."))
    #Util.send_message(phones="13693046284".split(","),msg=msg)