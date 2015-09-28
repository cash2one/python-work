
__author__ = 'linzhou208438'


class A(object):
    def m(self):
        print "a"



class B(A):
    def m(self):
        print "b"


class D(B):

    def __init__(self):
        pass



d=D()
d.m()
