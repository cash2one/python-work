import threadpool

__author__ = 'linzhou208438'
__update__ = '2015/5/10'


class ThreadPool(object):
    def __init__(self, cpu=8):
        self.cpu = cpu

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