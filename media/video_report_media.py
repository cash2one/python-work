#!/usr/bin/env python
# -*- coding: utf8 -*-
import copy
import os
import sys
sys.path.append('/opt/video_report_yyyymm')
from media.ReportFile import DataMonitor
from media.ReportUrl import ReportJsonSerial
from pythonFrame.fileTools.FileRead import FileRead
from media.PythonQueue import MyQueue, MyThread
from media.ReportEntity import ContantsMappingColumn
from media.ReportMonitor import ReportMonitor
from pythonFrame.fileTools.FileWrite import FileWrite
from pythonFrame.myUtils.Log import Log
from media.MysqlDB import ReportMysql,ReportOracle
from media.HiveQuery import HiveQuery
from pythonFrame.myUtils.Util import Util, MyDate



__author__ = 'linzhou208438'
__update__ = '2015/8/24'

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print '''
        usage:[ -Dsave [date] |  -DqueryOracle [date] | -DqueryDonate [date] | -DupdateAssignField HiveFuv|... [date]]'''
        print '''
         -Dsave	                insert on duplicate...
         -DupdateAssignField    eg  .py -DupdateAssignField will show all params
         -DqueryOracle	        query oracle data to generate a file
         -DqueryDonate	        query donate data to generate a file
         -Drecovery	            disaster recovery if mysql no response
         -DcheckParent          check parent son account
         -Dtest	                test python script
         -DsendMsg              eg: .py -DsendMsg phone1,phone2 message
         -DupdateAll            include url request

         '''
        exit()

    base_log = "/opt/video_report_yyyymm/data"
    myQueue = MyQueue()
    monitor = ReportMonitor()
    fileWrite = FileWrite()
    constants = ContantsMappingColumn()
    donateMysql = ReportMysql(ReportMysql.conn_donate_params,q=myQueue)
    reportOracle = ReportOracle(myQueue,conn_report_params=ReportOracle.conn_oracle_params)
    #reportMysql = ReportMysql(ReportMysql.conn_space_params)
    reportMysql = ReportMysql(ReportMysql.conn_formal_params)

    if cmp(sys.argv[1], '-Dsave') == 0:
        mydate = Util.get_yesterday()
        if len(sys.argv) == 3:
            mydate = sys.argv[2]

        reportOracle.__setattr__("mydate",MyDate(mydate))
        donateMysql.__setattr__("mydate",MyDate(mydate))
        reportMysql.__setattr__("mydate",MyDate(mydate))

        base_log = base_log+ "/" + mydate + "/"
        Util.file_mkdirs(os.path.dirname(base_log))
        log = Log(base_log+"/stdout.log")
        log.begin_log()

        hiveQuery = HiveQuery(base_log,myQueue,dt=mydate)
        jobs_size = constants.myContants.keys().__len__() + 1

        myThread = MyThread(myQueue,constants,jobs_size,monitor,base_log,reportMysql)
        myThread.start()

        donateMysql.query_all_donate(base_log + "MysqlDonate")
        reportOracle.write_rows_file(base_log + "OracleAd")
        hiveQuery.query_hive()

        myThread.sub_job_join()
        fileWrite.checkpoint_save(base_log+"checkpoint.log",copy.deepcopy(monitor))

        reportMysql.check_create_table(26)
        reportMysql.insert_mysql_duplicate(monitor)
        reportMysql.update_report_patch()

        log.end_log()

    elif cmp(sys.argv[1], '-DupdateAssignField') == 0:
        updateAssignField = 'all'
        mydate = Util.get_yesterday()
        if len(sys.argv) < 3:
            for k,v in constants.myContants.items():
                print " %s %s" % (str(k).ljust(40),str(v.keys()).rjust(20))
            exit()
        if len(sys.argv) >= 3:
            updateAssignField=sys.argv[2]
        if len(sys.argv) == 4:
            mydate = sys.argv[3]

        base_log = base_log+ "/" + mydate + "/"
        Util.file_mkdirs(os.path.dirname(base_log))
        reportMysql.__setattr__("mydate",MyDate(mydate))
        myThread = MyThread(myQueue,constants,1,monitor,base_log,MyDate(mydate))
        myThread.start()

        myQueue.put_singal(updateAssignField)
        myThread.sub_job_join()

        reportMysql.insert_mysql_duplicate_field(monitor)

    elif cmp(sys.argv[1], '-DupdateAll') == 0:
        mydate = Util.get_yesterday()
        if len(sys.argv) == 3:
            mydate = sys.argv[2]

        base_log = base_log+ "/" + mydate + "/"
        jobs_size = constants.myContants.keys().__len__() + 1
        Util.file_mkdirs(os.path.dirname(base_log))
        reportMysql.__setattr__("mydate",MyDate(mydate))

        myThread = MyThread(myQueue,constants,jobs_size,monitor,base_log,MyDate(mydate))
        myThread.start()

        for k in constants.myContants.keys():
            myQueue.put_singal(k)
        myThread.sub_job_join()

        reportMysql.insert_mysql_duplicate(monitor)

    elif cmp(sys.argv[1], '-Drecovery') == 0:
        mydate = Util.get_yesterday()
        if len(sys.argv) == 3:
            mydate = sys.argv[2]

        base_log = base_log+ "/" + mydate + "/"
        reportMysql.__setattr__("mydate",MyDate(mydate))

        monitor_log = FileRead(None).map_store_recover(base_log+"checkpoint.log")
        reportMysql.insert_mysql_duplicate(monitor_log)
        reportMysql.update_report_patch()

    elif cmp(sys.argv[1], '-DqueryOracle') == 0:
        mydate = Util.get_yesterday()
        if len(sys.argv) == 3:
            mydate = sys.argv[2]

        reportOracle.__setattr__("mydate",MyDate(mydate))
        base_log = base_log+ "/" + mydate
        reportOracle.write_rows_file(base_log + "/OracleAd")

    elif cmp(sys.argv[1], '-DqueryDonate') == 0:
        mydate = Util.get_yesterday()
        if len(sys.argv) == 3:
            mydate = sys.argv[2]

        reportMysql.__setattr__("mydate",MyDate(mydate))
        base_log = base_log+ "/" + mydate
        reportMysql.query_all_donate(base_log + "/MysqlDonate")

    elif cmp(sys.argv[1], '-Dpatch') == 0:
        mydate = Util.get_yesterday()
        if len(sys.argv) == 3:
            mydate = sys.argv[2]

        reportMysql.__setattr__("mydate",MyDate(mydate))
        reportMysql.update_report_patch()

    elif cmp(sys.argv[1], '-DsendMsg') == 0:
         msg = u"这是一个测试".encode("utf-8")
         phone='13693046284'
         if len(sys.argv) >= 3:
            phone=sys.argv[2]
         if len(sys.argv) == 4:
            msg = str(sys.argv[3]).encode("utf-8")

         Util.send_message(phones=phone.split(","),msg=msg)

    elif cmp(sys.argv[1], '-DcheckParent') == 0:
         mydate = Util.get_yesterday()
         msg = u"parent son has problem !".encode("utf-8")
         phone=None
         if len(sys.argv) >= 3:
            phone=sys.argv[2]

         #reportMysql = ReportMysql(ReportMysql.conn_formal_params)
         reportMysql.__setattr__("mydate",MyDate(mydate))

         check_ret = reportMysql.check_parent_son_message()
         if check_ret < 0:
            Util.send_message(phones=phone.split(","),msg=msg)

    elif cmp(sys.argv[1], '-Dtest') == 0:
        mydate = Util.get_yesterday()
        if len(sys.argv) == 3:
            mydate = sys.argv[2]

        base_log = base_log+ "/" + mydate + "/"
        jobs_size = constants.myContants.keys().__len__() + 1
        Util.file_mkdirs(os.path.dirname(base_log))

        myThread = MyThread(myQueue,constants,jobs_size,monitor,base_log,MyDate(mydate))
        myThread.start()

        myQueue.put_singal('URLRequest')
        myThread.sub_job_join()

    else:
        print "don't make trouble to me !"
