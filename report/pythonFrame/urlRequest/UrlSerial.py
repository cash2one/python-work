from pythonFrame.urlRequest.UrlBase import JsonBase
from pythonFrame.myUtils.Util import Util

__author__ = 'linzhou208438'


class JsonSerial(JsonBase):
    def __init__(self, monitor, cpu=8):
        JsonBase.__init__(self, monitor, cpu)
        self.uids = monitor.uids

    def url_append(self):
        for k, v in self.uids.items():
            for url in self.urls:
                self.format_urls.append(url % (v, k))


    def monitor_exception_count(self, e, isTimeout_retry, url):
        if str(e).find('urlopen error timed out') > -1:
            if not isTimeout_retry:
                self.monitor.urls_serial_timeout.append(url)
            else:
                self.monitor.urls_retry_failure.append(url)
        Util.printf('urlopen-->%s | exception--> %s' % (url, e))


    def monitor_url_count(self, isTimeout, url):
        if not isTimeout:
            self.monitor.urls_serial_running.append(url)

    def monitor_urls_len(self,urls):
        self.monitor.urls_serial_total = len(self.format_urls)

    def retry_timeout(self):
        for url1 in self.monitor.urls_serial_timeout:
            self.json_run(url1, True)
            self.monitor.urls_serial_timeout_retry.append(url1)