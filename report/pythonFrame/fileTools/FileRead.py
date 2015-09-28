import os
import pickle
from pythonFrame.fileTools.FileBase import FileBase
from pythonFrame.myUtils.Util import DynamicMethod


__author__ = 'linzhou208438'


class FileRead(FileBase):

    #__metaclass__ = DynamicMethod

    def __init__(self, monitorReport):
        self.map_store = monitorReport.map_store

    @DynamicMethod.timefn
    def read_file(self, path):
        self.proxy(path, {'process': self.callback})

    def read_obj_file(self, path):
        file_read = open(path)
        uids = pickle.load(file_read)
        file_read.close()
        return uids

    def map_store_recover(self, path):
         if  os.path.exists(path):
            return self.read_obj_file(path)
         return {}

    def file_readlines(self, path):
        return open(path).readlines()
