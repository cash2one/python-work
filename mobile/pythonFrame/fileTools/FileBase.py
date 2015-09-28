
import os
from pythonFrame.myUtils.Util import Util


__author__ = 'linzhou208438'
__update__ = '2015/5/9'


class FileBase(object):
    def __init__(self):
        pass

    def proxy(self, path, func):
        if not self.file_exist(path,True):
            return

        if func.has_key('preprocess'):
            self.preprocess = func.get('preprocess')
        if func.has_key('process'):
            self.process = func.get('process')
        if func.has_key('postprocess'):
            self.postprocess = func.get('postprocess')

        context = self.preprocess()
        file_object = open(path)
        try:
            for line in file_object:
                try:
                    exitcode = self.process(line, context, path)
                    if exitcode == 0:
                        continue
                    elif exitcode == -1:
                        return
                except Exception, e:
                    Util.printf(e)
            return self.postprocess(context)
        except Exception, e:
            Util.printf(e)
        finally:
            file_object.close()

    def file_exist(self, path, ignore=False):
        if not os.path.exists(path):
            if not ignore:
                raise Exception("file exception", 'path is not exist')
            else:
                return False
        return True

    def preprocess(self):
        return None

    def process(self, line, context, path):
        return None

    def postprocess(self, context):
        return None

