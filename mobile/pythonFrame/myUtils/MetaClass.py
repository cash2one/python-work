# -*- coding: utf8 -*-

from types import FunctionType
import functools
import time
from pythonFrame.myUtils.Util import Util

__author__ = 'linzhou208438'
__update__ = '2015/7/13'

class Field(object):
    def __init__(self, name, column_type):
        self.name = name
        self.column_type = column_type
    def __str__(self):
        return '<%s:%s>' % (self.__class__.__name__, self.name)

class StringField(Field):
    def __init__(self, name):
        super(StringField, self).__init__(name, 'varchar(100)')

class IntegerField(Field):
    def __init__(self, name):
        super(IntegerField, self).__init__(name, 'bigint')

class DynamicMethod(type):
    def __new__(cls, name, bases, dct):
        for name, value in dct.iteritems():
            if name not in ('__metaclass__', '__init__', '__module__') and\
                type(value) == FunctionType:
                value = DynamicMethod.timefn(value)

            dct[name] = value
        return type.__new__(cls, name, bases, dct)

    @classmethod
    def check_required(self,func):
        Util.printf('check some condition')
        return func

    @classmethod
    def timefn(self,fn):
        @functools.wraps(fn)
        def measure_time(*args,**kwargs):
            t1=time.time()
            result=fn(*args,**kwargs)
            t2=time.time()
            Util.printf('call %s(): took %s seconds' % (fn.__name__,str(t2-t1)))
            return result
        return measure_time

class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
        print('Found model: %s' % name)
        mappings = dict()
        for k, v in attrs.iteritems():
            if isinstance(v, Field):
                print('Found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
        for k in mappings.iterkeys():
            attrs.pop(k)
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = name # 假设表名和类名一致
        return type.__new__(cls, name, bases, attrs)