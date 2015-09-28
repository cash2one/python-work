. /etc/profile
. ~/.bash_profile
SCRIPT_NAME=$(readlink -f "$0")
LOCAL_IP=`/sbin/ifconfig | grep -oE '([0-9]{1,3}\.?){4}' | head -n1`
LOCAL_USER=`whoami`
LOCAL_PWD=`dirname ${SCRIPT_NAME}`

ERRORCODE=100

#trap 'echo "before execute line:$LINENO, s=$sql,o=$output,m=$message,c=$cleanfile"' DEBUG

sql=
message=
output=
cleanfile=
while getopts :s:o:m:c: PARA 2>/dev/null
do
        case $PARA in
                s) sql=$OPTARG
                ;;
                o) output=$OPTARG
                ;;
                m) message=$OPTARG
                ;;
                c) cleanfile=true
                ;;
                \?) echo "Error: can't recognize!"
                exit 101
                ;;
        esac
done

if [[ -z "$sql" || -z "$message" ]]; then
        echo "Error: sql and message must be given!"
        exit 102
fi

if [[ -n "$cleanfile" && -w "$output" ]]; then
        > $output
fi

return_status=1
if [[ ! -z "$output" ]]; then
    for i in 1 2 3 4 5; do
        if [[ -s "$output" ]]; then
            return_status=0
            break
        else
            $HIVE_HOME/bin/hive -e "$sql" > $output
        fi
    done
else
    for i in 1 2 3 4 5; do
        if (( ${return_status} != 0 )); then
            $HIVE_HOME/bin/hive -e "$sql"
            return_status=$?
        else
            break
        fi
    done
fi

if (( ${return_status} != 0 )); then
    curl -s -d key="x1@9eng" -d src="18210675915" -d fee="18210675915" -d dest="18210675915" -d mess="${LOCAL_USER}@${LOCAL_IP} : ${message}" http://dev.hd.sohu.com.cn/ppp/sns.php > /dev/null
    exit ${ERRORCODE}
fi
