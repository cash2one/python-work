from os.path import dirname, join
from sklearn.tree import DecisionTreeRegressor
from base import load_data
from pythonFrame.myUtils.Util import Util
import numpy as np
from ReportDB import ReportMysqlFlask


__author__ = 'linzhou208438'
__update__ = '2015/7/8'



from flask import Flask

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def home():
    return '<h1>welcome to sohu machine learn !</h1>'

@app.route('/mobile_suggest/<date_param>', methods=['POST','GET'])
def mobile_suggest(date_param):
    global model
    mysql = ReportMysqlFlask(ReportMysqlFlask.conn_formal_params,date_param)
    #mysql = ReportMysqlFlask(ReportMysqlFlask.conn_space_params,date_param)
    mysql.select_mobile_file()

    digits_test = load_data(date_param)
    x_test = digits_test.data[:,2:]
    y_test = model.predict(x_test)
    y_test.resize(len(y_test),1)
    ret = np.hstack((digits_test.data.astype(np.int64),y_test.astype(np.int64)))
    mysql.update_suggest_rate(ret)
    return 'success'


@app.route('/mobile_suggest_show/<date_param>', methods=['POST','GET'])
def mobile_suggest_show(date_param):
    global model
    digits_test = load_data(date_param)
    x_test = digits_test.data[:,2:]
    y_test = model.predict(x_test)
    y_test.resize(len(y_test),1)
    ret = np.hstack((digits_test.data.astype(np.int64),y_test.astype(np.int64)))
    return print_html(ret)


def print_head(html):
    heads = ['index','video_id','is56','w_vv','play_time','w_fuv_playcount','down_load_bytes','w_cdn_uv','rate_lv1']
    html.append('<tr>')
    for head in heads:
        html.append('<th>'+head+'</th>')
    html.append('</tr>')


def print_html(y_test):
    html = []
    i = 0
    html.append('<table>')
    print_head(html)
    for row in y_test:
        html.append('<tr>')
        html.append('<td>'+str(i)+'</td>')
        for j in range(7):
            html.append('<td>'+str(row[j])+'</td>')
        html.append('<td>'+str(row[7]/10000.0)+'</td>')
        html.append('</tr>')
        i = i+1
    html.append('</table>')
    return "  ".join(html)


def create_tree(offline):
    global model
    module_path = dirname(__file__)
    if offline:
       digits = load_data("video_report_201507_v5.txt")
       X = digits.data[:,1:]
       y = digits.target.astype(np.int64)
       model = DecisionTreeRegressor(max_depth=80)
       model.fit(X, y)
       Util.store_object(model,join(module_path, 'data', 'tree_model'))
    else:
        model = Util.grab_object(join(module_path, 'data', 'tree_model'))
    return model



if __name__ == '__main__':
    global model
    create_tree(False)
    app.run(host='10.10.79.193',port=5000,debug=True)
    #app.run(debug=True)
