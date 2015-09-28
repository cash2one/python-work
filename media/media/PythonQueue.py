import os
import time
import thread
import Queue
import multiprocessing
import threading
from media.ReportEntity import ContantsMappingColumn
from media.ReportFile import ReportReadFile,DataMonitor
from media.ReportUrl import ReportJsonSerial,ReportJsonParallel
from pythonFrame.myUtils.Log import Log
from pythonFrame.myUtils.Util import Util

__author__ = 'linzhou208438'
__update__ = '2015/8/21'

class MyQueue(object):

    def __init__(self):
        self.q = Queue.Queue()
        self.lock = thread.allocate_lock()

    def isEmpty(self):
        return self.q.empty()

    def get_singal(self):
        if not self.q.empty():
            return self.q.get(False)
        return None

    def put_singal(self,value):
        self.lock.acquire()
        self.q.put(value)
        self.lock.release()

class MyProcess(multiprocessing.Process):

    def __init__(self,singal,path,mysqlWrap,constants,monitor,lock):
        self.singal = singal
        self.path = path
        self.mysqlWrap = mysqlWrap
        self.constants = constants
        self.monitor = monitor
        self.lock = lock
        multiprocessing.Process.__init__(self,name=singal)

    def run(self):
        if self.singal == "URLRequest":
            dataMonitor = DataMonitor(self.path,self.constants,self.monitor)
            dataMonitor.init_monitor()
            self.monitor.monitor_start(self.path+"/monitor.log")
            log = Log(self.path+"/url_error.log")
            log.begin_log()
            ReportJsonSerial(self.monitor).json_start()
            ReportJsonParallel(self.monitor).json_start()
            time.sleep(5)
            log.end_log()
        else:
            reportReadFile = ReportReadFile(self.path,self.singal,self.constants,self.monitor)
            self.lock.acquire()
            Util.printf("lock acquire".ljust(50,"."))
            reportReadFile.read_file()
            self.lock.release()
            Util.printf("lock release".ljust(50,"."))

        Util.printf("proccess stop".ljust(50,"."))


class MyThread(threading.Thread):

    def __init__(self,q,constants,jobs_size,monitor,path=None,mysqlWrap=None):
        self.q = q
        self.path = path
        self.isRun = True
        self.mysqlWrap = mysqlWrap
        self.constants =constants
        self.jobs_size =jobs_size
        self.jobs = {}
        self.monitor = monitor
        self.lock = multiprocessing.Lock()
        threading.Thread.__init__(self)

    def sub_job_join(self):
        while True:
            if self.jobs.__len__() == self.jobs_size:
                for k,job in self.jobs.items() :
                    if job.is_alive():
                        Util.printf(("%s proccess join".ljust(50,".")) % k)
                        job.join()
                break
            else:
                Util.printf("main proccess sleeping".ljust(50,"."))
                time.sleep(10)


    def run(self):
        Util.printf("thread running".ljust(50,"."))

        while self.isRun:
            if not self.q.isEmpty():
                Util.printf("proccess running".ljust(50,"."))
                singal =  str(self.q.get_singal())
                p = MyProcess(singal,self.path,self.mysqlWrap,self.constants,self.monitor,self.lock)
                self.jobs.setdefault(singal,p)
                p.start()
                Util.printf("queque pop %s" % singal)
            else:
                jbs = [job.is_alive() for job in self.jobs.values() ]
                if jbs.count(False) == self.jobs_size :
                    self.isRun = False

                if jbs.__len__() == self.jobs_size:
                    self.isRun = False

                if jbs.__len__() > 0 and jbs.__len__() == self.jobs_size - 1:
                    self.q.put_singal("URLRequest")

                jps_show = [(name,job.is_alive()) for name,job in self.jobs.items() ]
                Util.printf("current process is_alive:%s" % jps_show)

                time.sleep(2)

        Util.printf("thread stop".ljust(50,"."))



if __name__ == "__main__":
    q=MyQueue(ContantsMappingColumn())
    q.put_singal("HiveUv")
    q.put_singal("HiveVv")

    m = MyThread(q,None,2)
    m.start()

