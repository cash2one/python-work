import threading
import time
from time import sleep
from pythonFrame.monitor.Monitor import Monitor

__author__ = 'linzhou208438'
__update__ = '2015/5/10'


class ReportMonitor(Monitor):
    def __init__(self,map_store={}):
        Monitor.__init__(self,map_store)
        self.is_stop = False

    def monitor_run(self, path):
        file_write = open(path, 'w')
        while not self.is_stop:
            notes = []
            titles = []
            contents = []
            columns = []

            notes.append(('start_time:%s' % self.begin_time).ljust(60))
            notes.append(('running_time:%s' % time.strftime('%Y-%m-%d %H:%M:%S')).rjust(60))

            titles.append('vids&videoinfolist(Count)'.center(60))
            titles.append('userGet(Count)'.center(60))

            columns.append('total'.center(15))
            columns.append('running'.center(15))
            columns.append('timeout'.center(15))
            columns.append('retry'.center(15))
            columns.append('total'.center(15))
            columns.append('running'.center(15))
            columns.append('timeout'.center(15))
            columns.append('retry'.center(15))

            contents.append('%s'.center(12) % self.urls_parallel_total)
            contents.append('%s'.center(12) % len(self.urls_parallel_running))
            contents.append('%s'.center(16) % len(self.urls_parallel_timeout))
            contents.append('%s'.center(16) % len(self.urls_parallel_timeout_retry))
            contents.append('%s'.center(15) % self.urls_serial_total)
            contents.append('%s'.center(14) % len(self.urls_serial_running))
            contents.append('%s'.center(16) % len(self.urls_serial_timeout))
            contents.append('%s'.center(16) % len(self.urls_serial_timeout_retry))

            self.print_monitor(file_write, notes=notes, titles=titles, contents=contents, columns=columns)
            sleep(1)

        file_write.close()
        print "monitor close file ..."

    def print_monitor(self,file_write, **args):
        file_write.write('\n')
        self.iter_list(args.get('notes', []),file_write)
        self.iter_list(args.get('titles', []),file_write)
        self.iter_list(args.get('columns', []),file_write)
        self.iter_list(args.get('contents', []),file_write)

    def iter_list(self, ls, file_write):
        file_write.write('|'.join(ls))
        file_write.write('\n')
        file_write.write(''.center(120, '-'))
        file_write.write('\n')

    def stop_monitor(self):
        sleep(5)
        self.is_stop = True
        print "monitor stop ..."





