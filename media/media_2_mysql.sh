#!/bin/sh
#!/usr/bin/env python
. /etc/profile
. ~/.bash_profile


YESTODAY=`date -d last-day +%Y%m%d`


python /home/qs/scripts/video_report_media.py -Dsave 


java -cp /opt/logs/ugc/exe_libs/ugc-statistics-hive-to-mysql-0.0.1-SNAPSHOT.jar com.sohu.videos.ugc.statistics.exe.ParentSonAcountExe


wget http://qs.hd.sohu.com.cn/new/wesharing/set_suggest_rate.do?plat=1


python /home/qs/scripts/video_report_media.py -DcheckParent


sleep 5
ERROR_COUNT=`grep 'Traceback\|Error' /opt/video_report_yyyymm/cron_error.log  | wc -l`
if [ $ERROR_COUNT -gt 0 ]
then
    python /home/qs/scripts/video_report_media.py -DsendMsg 13693046284 'ugc media  error !'
fi
