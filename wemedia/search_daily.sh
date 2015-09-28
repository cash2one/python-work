#!/bin/sh
#!/usr/bin/env python
. /etc/profile
. ~/.bash_profile

SEARCH_DAILY_DIR=/home/qs/scripts/search_daily

BEFOR_YESTODAY=`date -d "2 days ago" +%Y%m%d`
YESTODAY=`date -d last-day +%Y%m%d`
TODAY=`date +%Y%m%d`


if [ -n "$1" ]
then
    YESTODAY="$1"
fi


if [ ! -d "$SEARCH_DAILY_DIR" ]
then
  mkdir -p $SEARCH_DAILY_DIR
fi

if [  -d "$SEARCH_DAILY_DIR/$BEFOR_YESTODAY" ]
then
  rm -r $SEARCH_DAILY_DIR/$BEFOR_YESTODAY
fi


BASE_SQL="select  count(1),expand1 from logtbl_hour where  type like 'search%' and substr(partdt,0,8) =${YESTODAY} and expand1 is not null and length(expand1)!=0 "
SQL_CONDITION=(search_ifox search_web search_sohu search_mts 8 9)
for((i = 0; i < 6 ;i ++))
do
	 mkdir -p $SEARCH_DAILY_DIR/$YESTODAY
	 cd $SEARCH_DAILY_DIR/$YESTODAY
	 case  "${SQL_CONDITION[i]}"  in   
		"search_ifox" | "search_sohu" | "search_web"  )
		EXE_SQL="${BASE_SQL} and type='${SQL_CONDITION[i]}' group by expand1"
		#cho ${EXE_SQL} >> sql.log
		 $HIVE_HOME/bin/hive -S -e "${EXE_SQL}" > ${SQL_CONDITION[i]#*search_}_key.log  ;;
		"search_mts"   ) 
		 $HIVE_HOME/bin/hive -S -e "${BASE_SQL} and type='${SQL_CONDITION[i]}' and urlrefall not like '%user=share%' group by expand1" > mts_key.log   
		 $HIVE_HOME/bin/hive -S -e "${BASE_SQL} and type='${SQL_CONDITION[i]}' and expand2=1 group by expand1" > box_key.log  ;;
		"8"   ) 
		 $HIVE_HOME/bin/hive -S -e "${BASE_SQL} and expand2=${SQL_CONDITION[i]} group by expand1" > video2_key.log  ;;
		"9"   ) 
		 $HIVE_HOME/bin/hive -S -e "${BASE_SQL} and expand2=${SQL_CONDITION[i]} group by expand1" > video1_key.log  ;;
	 esac 
done



python /home/qs/scripts/bgrate.py
	 
	
