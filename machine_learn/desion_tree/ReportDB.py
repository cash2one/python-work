from os.path import dirname, join
from pythonFrame.dbEngine.MysqlWrap import MysqlWrap
from pythonFrame.myUtils.Util import Util, MyDate


__author__ = 'linzhou208438'
__update__ = '2015/5/10'


class ReportMysqlFlask(MysqlWrap):
    conn_test_params = {"host": "10.16.15.178", "user": "root", "db": "test",
                        "passwd": "root", "charset": "gbk", "port": 3306}

    conn_space_params = {"host": "10.10.77.47", "user": "wemedia_space", "db": "wemedia_sharing",
                         "passwd": "20!21@14", "charset": "gbk", "port": 3307}

    conn_formal_params = {"host": "10.10.57.125", "user": "my_sharing", "db": "my_sharing",
                          "passwd": "jdfcbw73Kru", "charset": "gbk", "port": 3306}

    conn_mobile_params = {"host": "10.10.12.48", "user": "ad_mobile_rw", "db": "ad_mobile",
                        "passwd": "7NpabDEoafCMvHH", "charset": "utf8", "port": 3306}

    def __init__(self, conn_params,date_param):
        MysqlWrap.__init__(self, conn_params)
        self.mydate = MyDate(date_param)


    def select_mobile_file(self):
        table_name = "video_report_%d" % int(self.mydate.get_now(format='ym'))
        sql_body = 'select video_id,is56,ifnull(w_vv,0),ifnull(play_time,0), ifnull(w_fuv_playcount,0), ifnull(down_load_bytes,0), ifnull(w_cdn_uv,0) ,ifnull(suggest_rate_lv1,0) '
        sql_where = ' from '+table_name+' where play_plat=2 and is_user_share=1 and is_video_share=1 and dtime='+str(self.mydate.get_now())
        sql = sql_body + sql_where
        rows = self.mysql_select(sql)
        self.save_rows(rows,str(self.mydate.get_now()))


    def save_rows(self, rows,path):
        module_path = dirname(__file__)
        file_write = open(join(module_path,'data',path), 'w')
        val = []
        for row in rows:
            for i in range(8):
                val.append(str(row[i]))
            file_write.write("  ".join(val) + "\n")
            val = []
        file_write.close()

    def update_suggest_rate(self,darray):
        table_name = "video_report_%d" % int(self.mydate.get_now(format='ym'))
        sql = "update "+table_name+" v set v.suggest_rate_lv1= %s  where v.dtime=%s and v.play_plat=2 and video_id=%s and is56=%s"
        datalist = []
        for row in darray:
            datalist.append((row[7]/10000.0, int(self.mydate.get_now()), row[0], row[1] ))
        self.mysql_executemany(sql,datalist)
        self.update_report_patch()

    def update_report_patch(self):
        table = "video_report_%d" % int(self.mydate.get_now(format='ym'))
        sql1 = " update %s set suggest_rate_lv1= (suggest_rate_lv1*100+20)/300 where play_plat=2 and dtime=%s and is_user_share=1 and is_video_share=1 and suggest_rate_lv1>0.1"
        sql2 = "update %s set suggest_rate_lv1= (suggest_rate_lv1*100+10)/200 where play_plat=2 and dtime= %s and is_user_share=1 and is_video_share=1 and suggest_rate_lv1<0.1 "
        sql3 = "update %s set suggest_rate_lv1=1  where play_plat=2 and dtime= %s and is_user_share=1 and is_video_share=1 and (js_original_data =null or js_original_data =0 ) "
        sql = (sql1, sql2,sql3)
        self.mysql_execute(sql, (table, self.mydate.get_now()))



if __name__ == '__main__':
    mysql = ReportMysqlFlask(ReportMysqlFlask.conn_space_params,20150720)
