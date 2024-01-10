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


def check_it():

    conn = get_connection()

    # 使用 cursor() 方法创建一个 dict 格式的游标对象 cursor
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    # 使用 execute()  方法执行 SQL 查询
    cursor.execute("SELECT * FROM `t_file` where id = '189acc7b-cd78-4b7d-88b2-bb5923c1b60a' or  id = 'c53f9cbd74f941ebb202ae73191ecc15'")

    # 使用 fetchone() 方法获取单条数据.
    data = cursor.fetchall()
    for data1 in data:
        for value in data1.values():
            print(value)

        for key,value in data1.items():
            print(f"Key: {key}, Value: {value}")


    # 关闭数据库连接
    cursor.close()
    conn.close()


if __name__ == '__main__':
    check_it()