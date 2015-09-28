from flask.ext.wtf import Form


__author__ = 'linzhou208438'
__update__ = '2015/7/15'



class IndexForm(Form):
    openid = TextField('openid')