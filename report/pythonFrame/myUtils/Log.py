import sys

__author__ = 'linzhou208438'
__update__ = '2015/5/10'


class Log(object):
    def __init__(self, path):
        self.path = path
        self.stdout = None


    def begin_log(self):
        self.stdout = sys.stdout
        sys.stdout = open(self.path, 'w')

    def end_log(self):
        sys.stdout.close()
        sys.stdout = self.stdout


    def flush_log(self):
        sys.stdout.flush()