# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 16:34:33 2019

@author: jiaxu
"""
from flask import Flask, request, render_template,redirect,url_for

app = Flask(__name__)

# @app.route('/')
# def my_form():
#     return render_template('my-form.html')

# @app.route('/', methods=['POST'])
# def my_form_post():
#     text = request.form['name']
#     processed_text = text.upper()
#     return processed_text

@app.route('/', methods=['POST','GET'])
def index():
    if request.method == 'POST': # 判断是否是 POST 请求
    # 获取表单数据
        period = request.form.get('time')
        print(period)# 传入表单对应输入字段的 name 值
        return redirect(url_for('index'))
        

    
    return render_template('my-form.html')

if __name__ == "__main__":
    # build_pair_trading_model()
    # back_testing(metadata, engine, k, back_testing_start_date, back_testing_end_date)
    app.run()
    
    
