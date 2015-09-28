#!/bin/sh
#!/usr/bin/env python
. /etc/profile
. ~/.bash_profile

VEDIO_REPORT_DIR_BASE=/home/qs/scripts/video_report_yyyymm/

YESTODAY=$1
if [ "$YESTODAY" == "" ]
then
YESTODAY=`date -d last-day +%Y%m%d`
fi

if [ ! -d "$VEDIO_REPORT_DIR_BASE/$YESTODAY" ]
then
  mkdir -p $VEDIO_REPORT_DIR_BASE/$YESTODAY
fi

VEDIO_REPORT_DIR=$VEDIO_REPORT_DIR_BASE/$YESTODAY/

CDN_SQL="select decrypt_vid, \
	SUM(download_bytes) , \
	count(distinct uid)  \
	from ugc_pgc_cdn where substr(dt,0,8)=${YESTODAY} and uid=decrypt_uid and cv<>'' and substr(cv,0,1)>=5 group by decrypt_vid"

VV_SQL="select video_id ,
	count(case when message_type='caltime' then  uid  end)*2 as playCountFuv, 
	count(distinct case when message_type='playCount' then  uid  end) as playCountFuv, 
	count(distinct case when message_type='videoStart' then  uid end)  as playCountFuv, 
	count(distinct case when message_type='videoends' then  uid  end) as playCountFuv 
	from ugc_pgc_vv_pt where dt=${YESTODAY} and is_use=1 and video_id=decrypt_vid   
        and uid=decrypt_uid and client_version<>'' and substr(client_version,0,1)>=5  group by video_id"

INVALID_VV_SQL="select video_id , count(1)\
	from ugc_pgc_vv_pt where dt=${YESTODAY} and is_use=0 \
	and video_id=decrypt_vid and uid=decrypt_uid \
	and message_type='playCount' and  client_version<>'' and substr(client_version,0,1)>=5  group by video_id"

W_VV_SQL="select video_id , count(1)\
	from ugc_pgc_vv_pt where dt=${YESTODAY} and is_use=1 \
	and video_id=decrypt_vid and uid=decrypt_uid and client_version<>'' and substr(client_version,0,1)>=5 and message_type='playCount'  group by video_id"

CDN_VV_SQL="select decrypt_vid,  count(distinct uid,decrypt_vid )\
	from ugc_pgc_cdn where substr(dt,0,8)=${YESTODAY}  \
	and  uid=decrypt_uid and cv<>'' and substr(cv,0,1)>=5 group by decrypt_vid"

DM_VV_SQL="select video_id , count(distinct uid, video_id)\
	from ugc_pgc_vv_pt where dt=${YESTODAY} and is_use=1 \
	and video_id=decrypt_vid and uid=decrypt_uid and  client_version<>'' and substr(client_version,0,1)>=5 and message_type='playCount' group by video_id"

IP_SQL=" select video_id, count(distinct ip)
         from ugc_pgc_vv_pt where dt=20150624 and is_use=1 
         and video_id=decrypt_vid and uid=decrypt_uid and  client_version<>'' and substr(client_version,0,1)>=5 and message_type='playCount' group by video_id" 

if [  -d "${VEDIO_REPORT_DIR}CDN_SQL.log" ]
then
  rm -f ${VEDIO_REPORT_DIR}CDN_SQL.log
fi

if [  -d "${VEDIO_REPORT_DIR}VV_SQL.log" ]
then
  rm -f ${VEDIO_REPORT_DIR}VV_SQL.log
fi

if [  -d "${VEDIO_REPORT_DIR}INVALID_VV_SQL.log" ]
then
  rm -f ${VEDIO_REPORT_DIR}INVALID_VV_SQL.log
fi

if [  -d "${VEDIO_REPORT_DIR}W_VV_SQL.log" ]
then
  rm -f ${VEDIO_REPORT_DIR}W_VV_SQL.log
fi

if [  -d "${VEDIO_REPORT_DIR}ORACLE_SQL.log" ]
then
  rm -f ${VEDIO_REPORT_DIR}ORACLE_SQL.log
fi


for((i=1;i<=8;i++))
do
	{
		if [ ${i} -eq 1 ]
		then
			$HIVE_HOME/bin/hive -S -e "${CDN_SQL}" > ${VEDIO_REPORT_DIR}CDN_SQL.log
		elif [ ${i} -eq 2 ]
                then
			$HIVE_HOME/bin/hive -S -e "${VV_SQL}" > ${VEDIO_REPORT_DIR}VV_SQL.log
                elif [ ${i} -eq 3 ]
                then
			$HIVE_HOME/bin/hive -S -e "${INVALID_VV_SQL}" > ${VEDIO_REPORT_DIR}INVALID_SQL.log
                elif [ ${i} -eq 4 ]
                then
			$HIVE_HOME/bin/hive -S -e "${W_VV_SQL}" > ${VEDIO_REPORT_DIR}W_SQL.log
                elif [ ${i} -eq 5 ]
                then
			$HIVE_HOME/bin/hive -S -e "${CDN_VV_SQL}" > ${VEDIO_REPORT_DIR}CDN_VV_DISTINCT_SQL.log
                elif [ ${i} -eq 6 ]
                then
			$HIVE_HOME/bin/hive -S -e "${DM_VV_SQL}" > ${VEDIO_REPORT_DIR}DM_SQL.log
                elif [ ${i} -eq 7 ]
                then
                       $HIVE_HOME/bin/hive -S -e "${IP_SQL}" > ${VEDIO_REPORT_DIR}IP_SQL.log
		else
			python /home/qs/scripts/video_report_mysql.py -DqueryOracle ${YESTODAY}
	  fi
		
	}&
done	

wait

python /home/qs/scripts/video_report_mysql.py -Dsave ${YESTODAY}

python /home/qs/scripts/video_report_mysql.py -Dadmobile   ${YESTODAY}

wget http://qs.hd.sohu.com.cn/new/wesharing/set_suggest_rate.do?plat=2

java -cp /opt/logs/ugc/exe_libs/ugc-statistics-hive-to-mysql-0.0.1-SNAPSHOT.jar com.sohu.videos.ugc.statistics.exe.ParentSonAcountExe

python /home/qs/scripts/video_report_media.py -DcheckParent

sleep 5
ERROR_COUNT=`grep 'Traceback\|Error' /home/qs/scripts/video_report_yyyymm/cron_error.log  | wc -l`
if [ $ERROR_COUNT -gt 0 ]
then
    python /home/qs/scripts/video_report_mysql.py -DsendMsg 13693046284 'ugc mobile  error !'
fi





