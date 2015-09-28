#!/bin/bash
##################
. /etc/profile 2> /dev/null
. ~/.bash_profile 2> /dev/null
##################

#hour=`date -d "-1 hour" +%Y%m%d%H`
hour=$1

if [ "$hour" == "" ]
then
        hour=`date -d "-1 hour" +%Y%m%d%H`
fi
day=${hour:0:8}
HDFS_PATH=/user/qs/cdntime/$hour
/opt/scripts/rely/rely.sh rely qs_bak_nginx.sh $hour.192.168.160.133
/opt/scripts/rely/rely.sh rely qs_bak_nginx.sh $hour.192.168.160.134
echo "end rely"

echo "start create lzoIndex"
time hadoop jar /opt/tvhadoop/hadoop/lib/hadoop-lzo-*.jar com.hadoop.compression.lzo.LzoIndexer $HDFS_PATH
echo "end create lzoIndex"
echo "start mapreduce"
/home/qs/scripts/LogUniversalTool.hewei.sh hdfs:$HDFS_PATH/* /home/qs/scripts/hive_cdntime_config.xml $hour /opt/tvhadoop/hadoop/LogUniversalToolFn.jar
echo "end mapreduce"

hadoop fs -rmr /user/qs/cdntime/`date -d "-3day" +%Y%m%d`

hadoop fs -ls /user/qs/warehouse/cdntime/dt=$hour
result=$?

if [ "$result" -eq "0" ]
then
	/opt/scripts/rely/rely.sh cdn_log_to_hive.sh $hour
else
	mbnumber="15801661246"
	mbarray=($mbnumber)
	mblength=${#mbarray[@]}
	for((j=0;j<mblength;j++))
	do
		curl -d  key="x1@9eng" -d src="${mbarray[$j]}" -d fee="${mbarray[$j]}" -d dest="${mbarray[$j]}" -d mess="load cdntime part $hour error!(cron 1186)" http://dev.hd.sohu.com.cn/ppp/sns.php
	done
fi