import os
import pickle
from pythonFrame.myUtils.Util import Util, DynamicMethod


__author__ = 'linzhou208438'


class FileWrite(object):

    __metaclass__ = DynamicMethod

    def __init__(self):
        pass

    def write_uids(self, path, uids):
        self.__persist_content(path, uids)

    def checkpoint_save(self, path, map_store):
        Util.file_remove(path)
        self.__persist_content(path, map_store)

    def __persist_content(self, path, content):
        file_write = open(path, 'w')
        pickle.dump(content, file_write)
        file_write.close()

    def print_file_append(self, path, content, func):
        file_write = open(path, 'a')
        func(content)
        file_write.close()


    def print_file_write(self, path, content, func):
        file_write = open(path, 'w')
        func(content)
        file_write.close()
