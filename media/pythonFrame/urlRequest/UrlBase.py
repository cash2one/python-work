import json
import time
import urllib2
from pythonFrame.urlRequest.ThreadPool import ThreadPool

__author__ = 'linzhou208438'


class JsonBase(ThreadPool):
    def __init__(self, monitor, cpu=8, timeout=10):
        ThreadPool.__init__(self,cpu)
        self.monitor = monitor
        self.timeout = timeout
        self.format_urls=[]
        self.urls=[]
        self.url = None


    def json_start(self):
        self.url_append()
        self.monitor_begin_time()
        self.monitor_urls_len(self.format_urls)
        self.thread_start(self.json_run, self.format_urls)

    def json_run(self, url, isTimeout_retry=False):
        self.monitor_url_count(isTimeout_retry, url)
        try:
            result, exitcode = self.preprocess(url)
            if exitcode < 0: return
            open_url = urllib2.urlopen(url, timeout=self.timeout)
            if open_url.getcode() == 200:
                page = open_url.read()
                jsonVal = json.loads(page)
                result, exitcode = self.process(jsonVal, result)
                if exitcode < 0: return
            else:
                raise Exception("code<>200", url)
            return self.postprocess(result)
        except Exception, e:
            self.monitor_exception_count(e, isTimeout_retry, url)


    def url_append(self):
        return None

    def preprocess(self, url):
        return (None, 0)

    def process(self, jsonVal, result):
        return (None, 0)

    def postprocess(self, result):
        return (None, 0)

    def monitor_exception_count(self, e, isTimeout, url):
        pass

    def monitor_url_count(self, isTimeout, url):
        pass

    def monitor_urls_len(self, urls):
        pass

    def monitor_begin_time(self):
        if self.monitor.begin_time is None:
            self.monitor.begin_time = time.strftime('%Y-%m-%d %H:%M:%S')

