import threadpool
import threading

__author__ = 'linzhou208438'
__update__ = '2015/5/10'


class ThreadPool(object):
    def __init__(self, cpu=8):
        self.cpu = cpu
        self.lock  = threading.Lock()

    def thread_start(self, thread_run, args):
        if len(args) > 0:
            pool = threadpool.ThreadPool(self.cpu)
            requests = threadpool.makeRequests(thread_run, args)
            [pool.putRequest(req) for req in requests]
            pool.wait()

        self.post_invoke()

    def thread_run(self, args):
        pass

    def preAction(self, args):
        pass

    def post_invoke(self):
        pass

    def thread_lock(self,fn,*args):
        self.lock.acquire()
        try:
            fn(*args)
        except Exception, e:
            import traceback
            traceback.print_exc()
        finally:
            self.lock.release()

if __name__ == '__main__':
    d = []
    t = ThreadPool()
    t.thread_lock(lambda x,y: d.append(x),1,2)
    #t.thread_start(lambda x:d.append(x),[x for x in range(300)])
    for i in d:
        print i
