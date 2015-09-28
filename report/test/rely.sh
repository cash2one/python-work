#!/bin/sh
###################
. /etc/profile
. ~/.bash_profile
##################
echo =====================================`date`===============================================
if [[ -z $3 ]]
   then
   java -jar /opt/scripts/rely/jobkeeper-tools.jar $1 $2
   else
   java -jar /opt/scripts/rely/jobkeeper-tools.jar $1 $2 $3
fi
echo =====================================`date`===============================================
