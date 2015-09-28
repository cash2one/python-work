#!/bin/bash
##################
. /etc/profile 2>/dev/null
. ~/.bash_profile 2>/dev/null
##################

set +x

LOCAL_IP=`/sbin/ifconfig eth0 | grep -oE '([0-9]{1,3}\.?){4}' | head -n1`

NGINX_HOME=/opt/nginx/logs

BAK_HOME=/opt/data

PID=$$

EXE_NAME=`basename $0`

hour=`date -d "-1 hour" +%Y%m%d%H`
day=${hour:0:8}

HDFS_PATH=/user/qs/cdntime/$hour

echo $day $hour $LOCAL_IP

if ! test -f $BAK_HOME/$day/access.log.$LOCAL_IP.$hour.lzo 
then
    mv $NGINX_HOME/access.log $NGINX_HOME/access.log.$PID.inMove.$hour

    kill -USR1 `cat $NGINX_HOME/nginx.pid` 2> /dev/null

    mkdir -p $BAK_HOME/$day

    mv $NGINX_HOME/access.log.$PID.inMove.$hour $BAK_HOME/$day/access.log.$LOCAL_IP.$hour

    /usr/local/bin/lzop -fU $BAK_HOME/$day/access.log.$LOCAL_IP.$hour
fi

hadoop fs -mkdir $HDFS_PATH

LOOP=0

while true
do
    echo "start put"
    hadoop fs -put $BAK_HOME/$day/access.log.$LOCAL_IP.$hour.lzo $HDFS_PATH
    result=$?
    echo "end put $result"
    if [ "$result" -eq "0" ]
    then
        /opt/scripts/rely/rely.sh qs_$EXE_NAME $hour.$LOCAL_IP
        break;
    fi
    if [ "$LOOP" -eq "2" ]
    then
        break;
    fi
    ((LOOP++));
done

set -x