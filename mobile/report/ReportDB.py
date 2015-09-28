# -*- coding: utf8 -*-
from pythonFrame.dbEngine.MysqlWrap import MysqlWrap
from pythonFrame.dbEngine.OracleWrap import OracleWrap
from pythonFrame.myUtils.Util import Util, MyDate
import datetime
from report import ReportEntity
from report.ReportMonitor import ReportMonitor

__author__ = 'linzhou208438'
__update__ = '2015/5/10'


class ReportMysql(MysqlWrap):
    conn_test_params = {"host": "10.16.15.178", "user": "root", "db": "test",
                        "passwd": "root", "charset": "gbk", "port": 3306}

    conn_space_params = {"host": "10.10.77.47", "user": "wemedia_space", "db": "wemedia_sharing",
                         "passwd": "20!21@14", "charset": "gbk", "port": 3307}

    conn_formal_params = {"host": "10.10.57.125", "user": "my_sharing", "db": "my_sharing",
                          "passwd": "jdfcbw73Kru", "charset": "gbk", "port": 3306}

    conn_mobile_params = {"host": "10.10.12.48", "user": "ad_mobile_rw", "db": "ad_mobile",
                        "passwd": "7NpabDEoafCMvHH", "charset": "utf8", "port": 3306}

    def __init__(self, conn_params,monitorReport):
        MysqlWrap.__init__(self, conn_params)
        self.monitorReport=monitorReport

    def select_map_store(self):
        map_store={}
        table = "video_report_%d" % int(self.monitorReport.mydate.get_now(format='ym'))
        fields = []
        fiedls_position={}
        re=ReportEntity.ReportEntity()
        count = 0
        for f in re.__dict__:
            fields.append(f)
            fiedls_position.setdefault(f,count)
            count+=1

        sql = ' select '
        sql += ','.join(fields)
        sql += ' from '+ table
        sql += ' where dtime='+ self.monitorReport.mydate.get_now() +' and play_plat=2 '
        #sql += ' and is_video_share=1 and is_user_share=1'
        Util.printf(sql)
        rows = self.mysql_select(sql)

        for row in rows:
            video_id= row[fiedls_position.get('video_id')]
            report = {}
            for k , v  in fiedls_position.iteritems():
                report.setdefault(k,row[v])
            map_store.setdefault(('%d' % video_id),report)

        return map_store




    def insert_report(self, map_store):
        table_name = "video_report_%d" % int(self.monitorReport.mydate.get_now(format='ym'))
        table_field = []
        datalist = []
        table_field_len = 0

        for report in map_store.values():
            if table_field_len == 0:
                for k in report.keys():
                    table_field.append(k)
                table_field_len = len(table_field)

            table_value = []
            for v in table_field:
                table_value.append(report[v])
            datalist.append(tuple(table_value))

        sql_head = ' insert into ' + table_name
        sql_body = ' ( ' + ','.join(table_field) + ' ) '
        sql_blank = ' , %s ' * (table_field_len - 1)
        sql_tail = ' values ( %s ' + sql_blank + ' ) '
        sql = sql_head + sql_body + sql_tail
        Util.printf(sql)
        self.mysql_executemany(sql, datalist)

    def insert_update_report(self, **args):
        map_store=args.get("map_store")
        update_fields=args.get("update_fields")
        table_name = "video_report_%d" % int(self.monitorReport.mydate.get_now(format='ym'))
        table_field = []
        update_field=[]
        datalist = []
        table_field_len = 0

        for report in map_store.values():
            if table_field_len == 0:
                for k in report.keys():
                    table_field.append(k)
                table_field_len = len(table_field)

            table_value = []
            for v in table_field:
                table_value.append(report[v])
            datalist.append(tuple(table_value))

        for field in update_fields:
            update_field.append( "%s=values(%s)" % (field,field))

        sql_head = ' insert into ' + table_name
        sql_body = ' ( ' + ','.join(table_field) + ' ) '
        sql_blank = ' , %s ' * (table_field_len - 1)
        sql_tail = ' values ( %s ' + sql_blank + ' ) '
        sql_duplicate=' ON DUPLICATE KEY UPDATE '+','.join(update_field)
        sql = sql_head + sql_body + sql_tail + sql_duplicate
        Util.printf(sql)
        self.mysql_executemany(sql, datalist)

    def update_report_patch(self):
        table = "video_report_%d" % int(self.monitorReport.mydate.get_now(format='ym'))
        sql1 = "update %s v set v.js_original_data= v.ad_stock  where v.dtime='%s' and v.play_plat=2"
        sql2 = "update %s v,channel_factor f set v.cate_code_rate=f.factor  where v.dtime='%s' and f.channel_id = v.cate_code and v.play_plat=2"
        sql3 = "update %s v,paly_type_factor t set v.play_type_rate= t.factor where v.dtime='%s' and t.play_type=v.play_type and v.play_plat=2"
        sql4 = "update %s v set v.is_video_share= 2 where (video_title is null or length(trim(video_title))=0 or uid=0) and v.dtime='%s' and v.play_plat=2"
        sql5 = "update %s v set v.price_lv1= 4.00 where v.dtime='%s' and v.play_plat=2"
        sql6 = "update %s v set cate_code_rate=1.0  where v.dtime='%s' and v.play_plat=2 and v.cate_code_rate=0"
        sql = (sql1, sql2, sql3, sql4, sql5, sql6)
        self.mysql_execute(sql, (table, self.monitorReport.mydate.get_now('-')))

    def update_mysql_field(self, *args):
        update_map = args[0]
        update_field = args[1]
        uids = args[2]
        table = "video_report_%d" % int(self.monitorReport.mydate.get_now(format='ym'))
        base_sql = "update %s set %s" % (table, update_field)
        sql = base_sql + "=%s where dtime=%s and play_plat=2 and video_id=%s and uid=%s"
        Util.printf(sql)
        datalist = []
        for k, v in update_map.items():
            datalist.append((v, self.monitorReport.mydate.get_now('-'), k, uids.get(k, 0)))
        self.mysql_executemany(sql, datalist)

    def insert_ad(self, map_store):
        table_name = "ad_mobile_%d" % int(self.monitorReport.mydate.get_now(format='ym'))
        table_field = []
        datalist = []
        table_field_len = 0

        for report in map_store.values():
            if table_field_len == 0:
                for k in report.keys():
                    table_field.append(k)
                table_field_len = len(table_field)

            table_value = []
            for v in table_field:
                table_value.append(report[v])
            datalist.append(tuple(table_value))

        sql_head = ' insert into ' + table_name
        sql_body = ' ( ' + ','.join(table_field) + ' ) '
        sql_blank = ' , %s ' * (table_field_len - 1)
        sql_tail = ' values ( %s ' + sql_blank + ' ) '
        sql = sql_head + sql_body + sql_tail
        Util.printf(sql)
        self.mysql_executemany(sql, datalist)

    def check_create_table(self,dt):
        current_dt = self.monitorReport.mydate.day
        sql = """ CREATE TABLE `ad_mobile_%s` (
                      `id` int(11) NOT NULL AUTO_INCREMENT,
                      `dtime` date NOT NULL,
                      `video_id` int(11) DEFAULT NULL,
                      `all_stock` int(11) DEFAULT NULL COMMENT '全部库存',
                      `stock` bigint(20) DEFAULT NULL COMMENT '库存',
                      `all_occu` int(11) DEFAULT NULL COMMENT '全部消耗',
                      `all_t_occu` int(11) DEFAULT NULL COMMENT '全部真实消耗',
                      `occu` bigint(20) DEFAULT NULL COMMENT '真实消耗',
                       `t_occu` int(11) DEFAULT NULL,
                       `vv` int(11) DEFAULT NULL COMMENT '分成vv',
                       `all_vv` int(11) DEFAULT NULL COMMENT '全部vv',
                       `is56` tinyint(2) DEFAULT '0',
                       PRIMARY KEY (`id`),
                     KEY `dtime_index` (`dtime`)
        ) ENGINE=InnoDB AUTO_INCREMENT=1359 DEFAULT CHARSET=utf8 """
        if int(current_dt) == dt:
            next_date = datetime.date.today() + datetime.timedelta(days=dt)
            self.mysql_execute((sql,),(next_date.strftime('%Y%m'),))



class ReportOracle(OracleWrap):
    conn_oracle_params = {"host": "10.10.34.48", "user": "leilinhai", "db": "videodb",
                          "passwd": "linhailei"}

    def __init__(self, monitorReport,conn_report_params=None):
        OracleWrap.__init__(self, conn_report_params)
        self.monitorReport=monitorReport

    def write_rows_file(self, path):
        #sql = 'select v_id,max_adv,uv,view_all_uv,stock,occu,pos1,pos2,pos3,pos4,pos5,t_occu from dwpdata.core_mobile_pgc where sver is not null and substr(sver,0,1)>=5 and  data_date=%s'
        sql = "select v_id,sum(max_adv),sum(uv),sum(view_all_uv),sum(stock),sum(occu),sum(pos1),sum(pos2),sum(pos3),sum(pos4),sum(pos5),sum(t_occu) from dwpdata.core_mobile_pgc where sver is not null   and sver <> 'null'  and  regexp_like(sver, '^\d')  and substr(sver,0,1)>=5  and  data_date=%s group by v_id "
        Util.printf(sql % self.monitorReport.mydate.get_now() )
        rows = self.oracle_fetchall(sql % self.monitorReport.mydate.get_now())
        self.save_rows(rows, path)

    def save_rows(self, rows, path):
        file_write = open(path, 'w')
        val = []
        for row in rows:
            for i in range(12):
                val.append(str(row[i]))
            file_write.write("  ".join(val) + "\n")
            val = []
        file_write.close()

if __name__ == '__main__':
    monitorReport = ReportMonitor()
    monitorReport.__setattr__("mydate",MyDate(20150726))
    reportMysql=ReportMysql(ReportMysql.conn_space_params,monitorReport)
    reportMysql.check_create_table(26)
