from multiprocessing.dummy import Pool

__author__ = 'linzhou208438'
__update__ = '2015/5/10'


class ThreadPool(object):
    def __init__(self, cpu=8):
        self.cpu = cpu

    def thread_start(self, thread_run, args):
        if len(args) > 0:
            self.preAction(args)
            pool = Pool(self.cpu)
            pool.map_async(thread_run, args)
            pool.close()
            pool.join()

    def thread_run(self, args):
        pass

    def preAction(self, args):
        pass