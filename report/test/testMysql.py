from pythonFrame.myUtils.Util import MyDate
from report.ReportEntity import ReportEntity, ReportDMEntity
from report.ReportMonitor import ReportMonitor

__author__ = 'linzhou208438'
__update__ = '2015/6/24'

class A(object):
    def __init__(self,map):
        print map.map_store


if __name__ == '__main__':

    # reportMysql=ReportMysql(ReportMysql.conn_space_params)
    # re = reportMysql.select_map_store()
    #print re.__len__()
    # mysqlWrap = ReportMysql(ReportMysql.conn_space_params)
    # mysqlWrap.insert_update_report(map_store=re,update_fields=ReportDMEntity().__dict__)
    monitorReport = ReportMonitor()

    mydatea=20150623
    monitorReport.__setattr__("11",MyDate(mydatea))
    print monitorReport.mydate.get_now()
    print monitorReport.__dict__
    print monitorReport.map_store
    print monitorReport.__getattribute__("map_store")
    a=A(monitorReport)
