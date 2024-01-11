#! /usr/bin/python
# -*- coding: UTF-8 -*-

import pymysql

host = '47.100.188.120'
port = 53306
db = 'dsai_ai'
user = 'dsai_ai'
password = '1qaz@WSX202312'


# ---- 用pymysql 操作数据库
def get_connection():
    conn = pymysql.connect(host=host, port=port, db=db, user=user, password=password)
    return conn


def check_list():
    conn = get_connection()

    # 使用 cursor() 方法创建一个 dict 格式的游标对象 cursor
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    # 使用 execute()  方法执行 SQL 查询
    cursor.execute(
        "select id,title_cn,title_en,doi,authors_cn,authors_en,keywords_cn,keywords_en,digest_cn,digest_en from t_metadata_html where id = '001202dac8d341a4ae3e8c433744eb5b'")

    # 使用 fetchone() 方法获取单条数据.
    data = cursor.fetchall()

    # 关闭数据库连接
    cursor.close()
    conn.close()
    print("查询")
    return data


def check_chapters_list(metadata_html_id):
    conn = get_connection()

    # 使用 cursor() 方法创建一个 dict 格式的游标对象 cursor
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    # 使用 execute()  方法执行 SQL 查询
    cursor.execute(
        "SELECT * FROM `t_metadata_html_chapters` where metadata_html_id = '" + metadata_html_id + "'")

    # 使用 fetchone() 方法获取单条数据.
    data = cursor.fetchall()

    # 关闭数据库连接
    cursor.close()
    conn.close()
    return data


def check_count():
    conn = get_connection()

    # 使用 cursor() 方法创建一个 dict 格式的游标对象 cursor
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    # 使用 execute()  方法执行 SQL 查询
    cursor.execute(
          "select count(1) as count from t_metadata_html where id = '001202dac8d341a4ae3e8c433744eb5b'")

    # 使用 fetchone() 方法获取单条数据.
    data = cursor.fetchone()
    # 关闭数据库连接
    cursor.close()
    conn.close()
    return data['count']


def check_chapters_count(metadata_html_id):
    conn = get_connection()

    # 使用 cursor() 方法创建一个 dict 格式的游标对象 cursor
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    # 使用 execute()  方法执行 SQL 查询
    cursor.execute(
        "SELECT count(1) as count FROM `t_metadata_html_chapters` where metadata_html_id = '" + metadata_html_id + "'")

    # 使用 fetchone() 方法获取单条数据.
    data = cursor.fetchone()
    # 关闭数据库连接
    cursor.close()
    conn.close()
    return data['count']
