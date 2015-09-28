#!/usr/bin/env python
# -*- coding: utf8 -*-
import sys

sys.path.append('/home/qs/scripts/video_report_yyyymm')
from report.AdMobile import AdReadFile
from report.ReportEntity import ReportURLEntity, ReportEntity, ReportCDNEntity, ReportDMEntity, ReportADEntity
from pythonFrame.fileTools.FileWrite import FileWrite
from pythonFrame.myUtils.Log import Log
from pythonFrame.myUtils.Util import Util, MyDate
from report.ReportDB import ReportMysql, ReportOracle
from report.ReportFile import ReportReadFile
from report.ReportMonitor import ReportMonitor
from report.ReportUrl import ReportJsonParallel, ReportJsonSerial


__author__ = 'linzhou208438'
__update__ = '2015/4/24'

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print '''

        usage:[ -Dsave [date] |  -DqueryOracle [date] | -Drecovery [date] |  -DupdateAssignField [all|cdn|dm|ad] [date] | -Dadmobile [date]]'''

        print '''

         -Dsave	                include three stages(readfile,fetchurl,insertmysql)
         -DqueryOracle	        query oracle data to generate a file
         -Drecovery	            disaster recovery if mysql no response
         -Dtest	                test python script
         -DwebConsole	        eg: http://10.10.79.193:8087/console
         -DsendMsg              eg: .py -DsendMsg phone1,phone2 message
         -DupdateAssignField    default all
         -Dadmobile             ad mobile data from oracle to mysql

         if you want to monitor python url request please exec :
         tail -f /home/qs/scripts/video_report_yyyymm/yyyyMMdd/monitor.log

         notice: all reference code in video_report_yyyymm directory
         eg: pythonFrame        provide basic component
             report             business service closely related

         '''

        exit()

    monitorReport = ReportMonitor()

    fileRead = ReportReadFile(monitorReport)
    fileWrite = FileWrite()

    jsonParallel = ReportJsonParallel(monitorReport)
    jsonSerial = ReportJsonSerial(monitorReport)

    mysqlWrap = ReportMysql(ReportMysql.conn_formal_params,monitorReport)
    #mysqlWrap = ReportMysql(ReportMysql.conn_space_params,monitorReport)

    if cmp(sys.argv[1], '-Dsave') == 0:
        mydate = Util.get_yesterday()
        if len(sys.argv) == 3:
            mydate = sys.argv[2]
        monitorReport.__setattr__("mydate",MyDate(mydate))

        logdir = "/home/qs/scripts/video_report_yyyymm/"+monitorReport.mydate.get_now()+"/"
        error_log = logdir + 'save_error.log'
        retry_timeout_log = logdir + 'retry_timeout.log'
        monitor_log = logdir + 'monitor.log'
        checkpoint_log = logdir + 'checkpoint.log'
        oracle_log = logdir + 'ORACLE_SQL.log'
        uids_log = logdir + "uids.log"

        Util.file_remove(error_log)
        Util.file_remove(retry_timeout_log)
        Util.file_remove(monitor_log)
        Util.file_remove(checkpoint_log)

        log = Log(error_log)
        log.begin_log()

        fileRead.read_file(logdir + "CDN_SQL.log")
        fileRead.read_file(logdir + "VV_SQL.log")
        fileRead.read_file(logdir + "INVALID_SQL.log")
        fileRead.read_file(logdir + "W_SQL.log")
        fileRead.read_file(logdir + "DM_SQL.log")
        fileRead.read_file(logdir + "IP_SQL.log")
        fileRead.read_file(logdir + "CDN_VV_DISTINCT_SQL.log")
        fileRead.read_file(logdir + "ORACLE_SQL.log")
        log.flush_log()

        monitorReport.monitor_start(monitor_log)
        jsonParallel.urls.append('http://my.tv.sohu.com/wm/u/vids.do?vid=%s')
        jsonParallel.urls.append('http://api.my.tv.sohu.com/video/videoinfolist.do?vid=%s')
        jsonSerial.urls.append('http://my.tv.sohu.com/user/a/media/userGet.do?uid=%s&vid=%s')
        jsonParallel.json_start()
        jsonSerial.json_start()

        jsonParallel.retry_timeout()
        jsonSerial.retry_timeout()

        fileWrite.checkpoint_save(checkpoint_log, monitorReport.map_store)
        fileWrite.write_uids(uids_log, monitorReport.uids)

        mysqlWrap.insert_report(monitorReport.map_store)
        mysqlWrap.update_report_patch()

        log.end_log()

    elif cmp(sys.argv[1], '-DqueryOracle') == 0:
        mydate = Util.get_yesterday()
        if len(sys.argv) == 3:
            mydate = sys.argv[2]
        monitorReport.mydate=MyDate(mydate)

        logdir = "/home/qs/scripts/video_report_yyyymm/"+monitorReport.mydate.get_now()+"/"
        oracle_log = logdir + 'ORACLE_SQL.log'

        Util.file_remove(oracle_log)
        oracleWrap = ReportOracle(monitorReport,ReportOracle.conn_oracle_params)
        oracleWrap.write_rows_file(oracle_log)

    elif cmp(sys.argv[1], '-Drecovery') == 0:
        mydate = Util.get_yesterday()
        if len(sys.argv) == 3:
            mydate = sys.argv[2]
        monitorReport.mydate=MyDate(mydate)

        logdir = "/home/qs/scripts/video_report_yyyymm/"+monitorReport.mydate.get_now()+"/"
        checkpoint_log = logdir + 'checkpoint.log'

        map_store = fileRead.map_store_recover(checkpoint_log)
        mysqlWrap.insert_report(map_store)
        mysqlWrap.update_report_patch()

    elif cmp(sys.argv[1], '-Dadmobile') == 0:
        mydate = Util.get_yesterday()
        if len(sys.argv) == 3:
            mydate = sys.argv[2]
        monitorReport.__setattr__("mydate",MyDate(mydate))
        adfileRead = AdReadFile(monitorReport)
        mysqlWrap = ReportMysql(ReportMysql.conn_mobile_params,monitorReport)

        logdir = "/home/qs/scripts/video_report_yyyymm/"+monitorReport.mydate.get_now()+"/"
        adfileRead.read_file(logdir + "ORACLE_SQL.log")

        mysqlWrap.insert_ad(monitorReport.map_store)

    elif cmp(sys.argv[1], '-DupdateAssignField') == 0:
        updateAssignField = 'all'
        mydate = Util.get_yesterday()
        if len(sys.argv) >= 3:
            updateAssignField=sys.argv[2]
        if len(sys.argv) == 4:
            mydate = sys.argv[3]
        monitorReport.mydate=MyDate(mydate)

        logdir = "/home/qs/scripts/video_report_yyyymm/"+monitorReport.mydate.get_now()+"/"
        error_log = logdir + 'save_error.log'
        retry_timeout_log = logdir + 'retry_timeout.log'
        monitor_log = logdir + 'monitor.log'
        checkpoint_log = logdir + 'checkpoint.log'
        oracle_log = logdir + 'ORACLE_SQL.log'
        uids_log = logdir + "uids.log"

        fileRead.read_file(logdir + "CDN_SQL.log")
        fileRead.read_file(logdir + "VV_SQL.log")
        fileRead.read_file(logdir + "INVALID_SQL.log")
        fileRead.read_file(logdir + "W_SQL.log")
        fileRead.read_file(logdir + "DM_SQL.log")
        fileRead.read_file(logdir + "IP_SQL.log")
        fileRead.read_file(logdir + "CDN_VV_DISTINCT_SQL.log")
        fileRead.read_file(logdir + "ORACLE_SQL.log")

        #map_store = fileRead.map_store_recover(checkpoint_log)
        map_store = mysqlWrap.select_map_store()
        reportURLEntity=ReportURLEntity()
        vid_exclude=reportURLEntity.setValueBySelfField(map_store,monitorReport.map_store)

        monitorReportNew = ReportMonitor(map_store=vid_exclude)
        jsonParallel = ReportJsonParallel(monitorReportNew)
        jsonSerial = ReportJsonSerial(monitorReportNew)

        monitorReportNew.monitor_start(monitor_log)
        jsonParallel.urls.append('http://my.tv.sohu.com/wm/u/vids.do?vid=%s')
        jsonParallel.urls.append('http://api.my.tv.sohu.com/video/videoinfolist.do?vid=%s')
        jsonSerial.urls.append('http://my.tv.sohu.com/user/a/media/userGet.do?uid=%s&vid=%s')
        jsonParallel.json_start()
        jsonSerial.json_start()

        jsonParallel.retry_timeout()
        jsonSerial.retry_timeout()

        reportURLEntity.setValueBySelfField(monitorReportNew.map_store,monitorReport.map_store)
        fileWrite.checkpoint_save(checkpoint_log, monitorReport.map_store)

        if updateAssignField == 'all':
            mysqlWrap.insert_update_report(map_store=monitorReport.map_store,update_fields=ReportEntity().__dict__)
        elif updateAssignField == 'cdn':
            mysqlWrap.insert_update_report(map_store=monitorReport.map_store,update_fields=ReportCDNEntity().__dict__)
        elif updateAssignField == 'dm':
            mysqlWrap.insert_update_report(map_store=monitorReport.map_store,update_fields=ReportDMEntity().__dict__)
        elif updateAssignField == 'ad':
            mysqlWrap.insert_update_report(map_store=monitorReport.map_store,update_fields=ReportADEntity().__dict__)

        mysqlWrap.update_report_patch()

    elif cmp(sys.argv[1], '-DsendMsg') == 0:
         msg = u"这是一个测试".encode("utf-8")
         phone='13693046284'
         if len(sys.argv) >= 3:
            phone=sys.argv[2]
         if len(sys.argv) == 4:
            msg = str(sys.argv[3]).encode("utf-8")

         Util.send_message(phones=phone.split(","),msg=msg)

    elif cmp(sys.argv[1], '-Dtest') == 0:

        logdir = "/home/qs/scripts/video_report_yyyymm/"+monitorReport.mydate.get_now()+"/"
        fileRead.read_file(logdir + "CDN_SQL.log")

    else:
        print "don't make trouble to me !"
