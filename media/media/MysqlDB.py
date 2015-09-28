# -*- coding: utf8 -*-
import os
import multiprocessing
import datetime
from media.ReportEntity import ContantsMappingColumn
from pythonFrame.dbEngine.MysqlWrap import MysqlWrap
from pythonFrame.dbEngine.OracleWrap import OracleWrap
from pythonFrame.myUtils.Util import Util, MyDate

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

    conn_donate_params = {"host": "10.10.57.85", "user": "winter_read", "db": "wminter",
                        "passwd": "4n995i1bsnoy", "charset": "gbk", "port": 3306}


    def __init__(self, conn_params,q=None,mydate=MyDate(Util.get_yesterday())):
        MysqlWrap.__init__(self, conn_params)
        self.q = q
        self.mydate=mydate
        self.table_name = "video_report_%d" % int(self.mydate.get_now(format='ym'))


    def query_all_donate(self,path):
        sql = "SELECT WM_ACCEPT_USER_ID uid,VID vid,IS_FROM_56 is56,sum(WM_GIVE_MONEY)  money  " \
              " FROM wm_sponsor_record  where  WM_GIVE_DAY=%s   group by WM_ACCEPT_USER_ID,VID,IS_FROM_56"
        Util.printf(sql % self.mydate.get_now())
        rows = self.mysql_select(sql % self.mydate.get_now())
        self.save_rows(rows,path,4)
        self.q.put_singal(path[os.path.dirname(path).__len__()+1:])

    def save_rows(self, rows, path,row_len):
        file_write = open(path, 'w')
        val = []
        for row in rows:
            for i in range(row_len):
                val.append(str(row[i]))
            file_write.write("  ".join(val) + "\n")
            val = []
        file_write.close()

    def insert_mysql_duplicate(self,monitor):
        self.insert_to_mysql_default(monitor.union_store)
        self.save_url_vid(monitor.map_store)
        self.save_url_uid(monitor.uid_share_map)

    def insert_mysql_duplicate_field(self,monitor):
        self.insert_to_mysql_on_duplicate(monitor.union_store)
        self.save_url_vid(monitor.map_store)
        self.save_url_uid(monitor.uid_share_map)


    def insert_to_mysql_default(self,columns_data):
        field_exclude = ["uid","video_id","is56","dtime","play_plat"]
        datalist = []
        cmc = ContantsMappingColumn()
        contants = cmc.get_unique_value()

        table_field= [k for k in contants]
        table_field.append("dtime")
        table_field.append("play_plat")
        table_field_len = len(table_field)

        set_diff = contants.difference(set(field_exclude))
        update_field = []
        map(lambda s:update_field.append( "%s=values(%s)" % (s,s)),set_diff)

        for report in columns_data.values():
            report.setdefault("dtime",self.mydate.get_now())
            report.setdefault("play_plat",1)

            table_value = []
            for v in table_field:
                table_value.append(report.get(v,cmc.get_default_value(v)))


            datalist.append(tuple(table_value))

        sql_head = ' insert into ' + self.table_name
        sql_body = ' ( ' + ','.join(table_field) + ' ) '
        sql_blank = ' , %s ' * (table_field_len - 1)
        sql_tail = ' values ( %s ' + sql_blank + ' ) '
        sql_duplicate=' ON DUPLICATE KEY UPDATE '+','.join(update_field)
        sql = sql_head + sql_body + sql_tail + sql_duplicate
        Util.printf(sql)
        Util.printf("will update rows_num:%d" % datalist.__len__())
        self.mysql_executemany(sql, datalist)

    def insert_to_mysql_on_duplicate(self,columns_data):
        field_exclude = ["uid","video_id","is56","dtime","play_plat"]
        cmc = ContantsMappingColumn()
        table_field_len = 0
        update_field = []
        table_field= []
        datalist = []

        for report in columns_data.values():
            report.setdefault("dtime",self.mydate.get_now())
            report.setdefault("play_plat",1)
            if table_field_len == 0:
                for k in report.keys():
                    table_field.append(k)
                    if k not in field_exclude:
                        update_field.append( "%s=values(%s)" % (k,k))

                table_field_len = len(table_field)

            table_value = []
            for v in table_field:
                table_value.append(report.get(v,cmc.get_default_value(v)))

            datalist.append(tuple(table_value))

        sql_head = ' insert into ' + self.table_name
        sql_body = ' ( ' + ','.join(table_field) + ' ) '
        sql_blank = ' , %s ' * (table_field_len - 1)
        sql_tail = ' values ( %s ' + sql_blank + ' ) '
        sql_duplicate=' ON DUPLICATE KEY UPDATE '+','.join(update_field)
        sql = sql_head + sql_body + sql_tail + sql_duplicate
        Util.printf(sql)
        Util.printf("will update rows_num:%d" % datalist.__len__())
        self.mysql_executemany(sql, datalist)

    def save_url_vid(self,map_store):
        sql = 'update '+ self.table_name +' set cate_code=%s,play_type=%s,video_play_time=%s,video_title=%s,is_video_share=%s where video_id=%s and dtime=%s and play_plat=1'
        datalist = []
        for k,v in map_store.items():
            datalist.append((  v.get("cate_code"),
                             v.get("play_type"),
                             v.get("video_play_time"),
                             v.get("video_title"),
                             v.get("is_video_share"),
                             k,
                             self.mydate.get_now()))
        Util.printf("will update rows_num:%d" % datalist.__len__())
        self.mysql_executemany(sql, datalist)

    def save_url_uid(self,map_store):
        sql = 'update '+ self.table_name +' set is_user_share=2 where uid =%s and dtime =%s and play_plat=1 '
        datalist = []
        for k in map_store.keys():
            datalist.append((k,self.mydate.get_now()))
        Util.printf("will update rows_num:%d" % datalist.__len__())
        self.mysql_executemany(sql, datalist)


    def update_report_patch(self):
        sql1 = "update %s v set v.js_original_data= v.ad_stock  where v.dtime='%s' and v.play_plat=1"
        sql2 = "update %s v,channel_factor f set v.cate_code_rate=f.factor  where v.dtime='%s' and f.channel_id = v.cate_code and v.play_plat=1"
        sql3 = "update %s v,paly_type_factor t set v.play_type_rate= t.factor where v.dtime='%s' and t.play_type=v.play_type and v.play_plat=1"
        sql4 = "update %s v set is_video_share= 2 where (video_title is null or length(trim(video_title))=0 or uid=0) and v.dtime='%s' and v.play_plat=1"
        sql5 = "update %s v set v.price_lv1= 4.00 where v.dtime='%s' and v.play_plat=1"
        sql6 = "update %s v set cate_code_rate=1.0  where v.dtime='%s' and v.play_plat=1 and v.cate_code_rate=0"
        sql7 = "update %s v set donate_income=0.92 * donate_order_income  where v.dtime='%s' and v.play_plat=1 "
        sql = (sql1, sql2, sql3, sql4, sql5, sql6,sql7)
        self.mysql_execute(sql, (self.table_name, self.mydate.get_now()))

    def check_parent_son_message(self):
        sql1 = "SELECT  dtime, uid,count(distinct  ifnull(parent_uid,0)) c FROM %s where dtime=%s group by dtime,uid having c>1"
        sql2 = "SELECT  count(*) c FROM %s where dtime=%s and parent_uid is not null and play_plat=1"
        sql3 = "SELECT  count(*) c FROM %s where dtime=%s and parent_uid is not null and play_plat=2"

        conditon1 = self.check_parent_son_message_sql(sql1)
        conditon2 = self.check_parent_son_message_sql(sql2)
        conditon3 = self.check_parent_son_message_sql(sql3)

        if conditon1 is not None and conditon1.__len__()>0:
            return -1
        if conditon2 is not None and conditon2[0][0] == 0:
            return -1
        if conditon3 is not None and conditon3[0][0] == 0:
            return -1
        return 0


    def check_parent_son_message_sql(self,sql):
        sql_format = sql % (self.table_name, self.mydate.get_now())
        conditon =  self.mysql_select(sql_format)
        Util.printf(str(conditon))
        return conditon


    def check_create_table(self,dt):
        current_dt = self.mydate.day
        sql = """  CREATE TABLE `video_report_%s` (
          `id` bigint(20) NOT NULL AUTO_INCREMENT,
          `uid` bigint(20) NOT NULL COMMENT '用户id',
          `video_id` bigint(11) DEFAULT NULL COMMENT '视频id',
          `video_title` varchar(255) DEFAULT '' COMMENT '视频标题',
          `cate_code` bigint(10) DEFAULT NULL COMMENT '视频分类',
          `cate_code_rate` decimal(5,4) DEFAULT '0.0000' COMMENT '视频分类比率',
          `play_type` tinyint(4) DEFAULT '0' COMMENT '0、无设置 1、独播 2、首播',
          `play_type_rate` decimal(5,4) DEFAULT '0.0000' COMMENT '视频属性比率',
          `w_pv` bigint(11) DEFAULT NULL COMMENT '主站的pv:打开页面但没看过视频',
          `w_uv` bigint(11) DEFAULT NULL COMMENT '主站uv',
          `w_vv` bigint(11) DEFAULT NULL COMMENT '主站vv：看过视频的',
          `rate_lv1` decimal(5,4) DEFAULT '0.0000' COMMENT '搜狐对大渠道的扣量比率',
          `price_lv1` decimal(8,4) unsigned zerofill DEFAULT NULL COMMENT '搜狐对大渠道的定价',
          `js_type` tinyint(4) DEFAULT NULL COMMENT '结算方式-暂时无意义',
          `js_original_data` bigint(11) DEFAULT NULL,
          `status` tinyint(4) DEFAULT '0' COMMENT '放数据进度，0为不放，1为给大渠道放，2为小渠道放',
          `is_settled` tinyint(4) DEFAULT '0' COMMENT '是否已结算，0为未结算，1为已结算',
          `create_time` datetime DEFAULT NULL COMMENT '数据生成时间',
          `dtime` date DEFAULT NULL COMMENT '数据时间，格式示例：20140215',
          `w_step1` bigint(11) DEFAULT NULL,
          `w_step2` bigint(11) DEFAULT NULL,
          `w_step3` bigint(11) DEFAULT NULL,
          `w_step4` bigint(11) DEFAULT NULL,
          `w_step5` bigint(11) DEFAULT NULL,
          `w_step6` bigint(11) DEFAULT NULL,
          `w_step_other` bigint(11) DEFAULT NULL,
          `down_load_bytes` bigint(20) DEFAULT '0',
          `w_cdn_uv` bigint(20) DEFAULT NULL COMMENT 'cdn uv',
          `play_time` bigint(20) DEFAULT NULL,
          `w_fuv_playcount` bigint(20) DEFAULT NULL COMMENT '页面加载完成',
          `w_fuv_videostart` bigint(20) DEFAULT NULL,
          `w_fuv_videoends` bigint(20) DEFAULT NULL,
          `ad_max_adv` int(11) DEFAULT NULL COMMENT '最大广告数',
          `ad_uv` int(11) DEFAULT NULL COMMENT '广告总UV',
          `ad_view_all_uv` int(11) DEFAULT NULL COMMENT '看完广告UV',
          `ad_stock` int(11) DEFAULT NULL COMMENT '库存',
          `ad_occu` int(11) DEFAULT NULL COMMENT '占用量',
          `ad_pos1` int(11) DEFAULT NULL COMMENT '前贴1',
          `ad_pos2` int(11) DEFAULT NULL,
          `ad_pos3` int(11) DEFAULT NULL,
          `ad_pos4` int(11) DEFAULT NULL,
          `ad_pos5` int(11) DEFAULT NULL,
          `data_time_stamp` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新或创建的时间戳',
          `w_ip_count` bigint(20) DEFAULT '0' COMMENT '独立ip数',
          `w_ip_count_netbar` bigint(20) DEFAULT '0' COMMENT '网吧独立ip数',
          `suggest_rate_lv1` decimal(5,4) DEFAULT '0.0000' COMMENT '建议扣量比率',
          `ad_t_occu` int(11) DEFAULT NULL COMMENT '广告真实消耗',
          `donate_income` decimal(14,4) DEFAULT '0.0000' COMMENT '打赏金额',
          `w_cdn_single_vv` bigint(20) DEFAULT NULL COMMENT '排重cdn vv',
          `w_dm_single_vv` bigint(20) DEFAULT NULL COMMENT '排重dm vv',
          `is_user_share` tinyint(4) DEFAULT '1' COMMENT ' 1 参与分成；2 不参与分成',
          `is_video_share` tinyint(4) DEFAULT '1' COMMENT ' 1 参与分成；2 不参与分成',
          `donate_order_income` decimal(14,4) DEFAULT '0.0000' COMMENT '原始打赏金额',
          `video_play_time` bigint(20) DEFAULT NULL,
          `is56` tinyint(2) DEFAULT '0' COMMENT '0:sohu 1:wuliu',
          `play_plat` tinyint(2) NOT NULL DEFAULT '1',
          `invalid_vv` bigint(20) DEFAULT NULL,
          `parent_uid` bigint(20) DEFAULT NULL COMMENT '父账号uid',
          `be_income` int(4) DEFAULT NULL COMMENT '是否可见收益：0不可见，1可见',
          `be_extract` int(4) DEFAULT NULL COMMENT '是否可以提现：0不可，1可以',
          `bind_status` int(4) DEFAULT NULL COMMENT '与父账号的绑定状态：0绑定申请中，1已绑定，    2子账号拒绝绑定，3父账号取消申请，4子账号申请解绑，5父账号解除对子账号绑定，6父账号不允许解除绑定',
          PRIMARY KEY (`id`),
          UNIQUE KEY `uq_dtime_uid_vid` (`dtime`,`uid`,`video_id`,`is56`,`play_plat`),
          KEY `idx_uid` (`uid`),
          KEY `idx_puid` (`parent_uid`),
          KEY `idx_vid` (`video_id`,`dtime`,`is56`,`play_plat`)
        ) ENGINE=InnoDB AUTO_INCREMENT=85613767 DEFAULT CHARSET=utf8
         """
        if int(current_dt) == dt:
            next_date = datetime.date.today() + datetime.timedelta(days=dt)
            self.mysql_execute((sql,),(next_date.strftime('%Y%m'),))


class ReportOracle(OracleWrap):
    conn_oracle_params = {"host": "10.10.34.48", "user": "leilinhai", "db": "videodb",
                          "passwd": "linhailei"}

    def __init__(self,q, mydate=MyDate(Util.get_yesterday()),conn_report_params=None):
        OracleWrap.__init__(self, conn_report_params)
        self.mydate=mydate
        self.q = q

    def write_rows_file(self, path):
        sql = "select u_id,v_id,source,max_adv,uv,view_all_uv,stock,occu,pos1,pos2,pos3,pos4,pos5,t_occu from dwpdata.core_UGC_OCCU_RATIO where data_date=%s"
        Util.printf(sql % self.mydate.get_now() )
        rows = self.oracle_fetchall(sql % self.mydate.get_now())
        self.save_rows(rows, path)
        self.q.put_singal(path[os.path.dirname(path).__len__()+1:])

    def save_rows(self, rows, path):
        file_write = open(path, 'w')
        val = []
        for row in rows:
            for i in range(14):
                val.append(str(row[i]))
            file_write.write("  ".join(val) + "\n")
            val = []
        file_write.close()


