#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : pureoym
# @Contact : pureoym@163.com
# @TIME    : 2018/11/12 16:18
# @File    : rank_parter.py.py
# Copyright 2017 pureoym. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ========================================================================
import numpy as np
import random
import urllib.request  # 爬虫下载引用
import pandas as pd
import pymysql
import sys
from sqlalchemy import create_engine
import sqlalchemy

# 解析xml引用
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

ALEXA_URL = ["http://data.alexa.com/data/TCaX/0+qO000fV?cli=10&url="]  # alexa接口URL列表
# ALEXA_URL = ["http://data.alexa.com/data/TCaX/0+qO000fV?cli=10&url=","http://data.alexa.com/data/ezdy01DOo100QI?cli=10&url=","http://data.alexa.com/data/+wQ411en8000lA?cli=10&url="] # alexa接口URL列表

INTERNAL_WEIGHT = 0.9  # 内部排名占排名的算法权重

# mysql登陆信息
HOST = '10.10.65.232'
USER = 'opendata'
PASSWD = 'opendata_123'
DB = 'opendata'
PORT = 3306

# 更新的合作伙伴表名
TABLE = 't_dp_partner_bak_20170508'
# 新增内部排名的文件地址
NEW_INTERNAL_RANK_PATH = '/application/search/partner_rank/partner_update_internal_rank.txt'

# 建立mysql连接
try:
    conn = pymysql.connect(host='10.10.65.232',
                           user='opendata',
                           password='opendata_123',
                           db='opendata',
                           port=3306,
                           charset='utf8')
except pymysql.err.OperationalError as e:
    print('OperationalError: ' + str(e))
    sys.exit()

try:
    engine = create_engine('mysql+pymysql://opendata:opendata_123@10.10.65.232:3306/opendata')
except sqlalchemy.exc.OperationalError as e:
    print('OperationalError: ' + str(e))
    sys.exit()
except sqlalchemy.exc.InternalError as e:
    print('InternalError: ' + str(e))
    sys.exit()


def get_mysql_conn(conf):
    """
    建立mysql链接
    :param conf:
    :param logger:
    :return:
    """
    try:
        conn = pymysql.connect(host=conf['host'],
                               user=conf['user'],
                               passwd=conf['passwd'],
                               db=conf['db'],
                               port=conf['port'],
                               charset="utf8")
        # logger.debug('mysql connection established')
    except Exception as e:
        # logger.error(e)
        print(e)
        sys.exit()
    return conn


def execute(conn, sql):
    '''
    执行sql语句
    :param conn:
    :param sql:
    :return:
    '''
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
    except pymysql.Error as e:
        conn.rollback()
        # logger.error(e)
        print(e)


def execute_and_get_result(conn, sql):
    '''
    执行sql语句
    :param conn:
    :param sql:
    :return:
    '''
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
        results = cursor.fetchall()
        return results
    except pymysql.Error as e:
        conn.rollback()
        print(e)


def update_to_db(sql):
    execute(conn, sql)


def update_internal_rank(internal_rank_data_path):
    """
    更新合作伙伴内部排名
    :param internal_rank_data_path:
    :return:
    """
    partner_df = pd.read_csv(internal_rank_data_path, header=None, names=['domainName'])

    # 获取原最高内部排名
    sql1 = "SELECT MAX(internalRank) FROM " + TABLE
    internal_rank_max = execute_and_get_result(conn, sql1)[0][0]

    # 计算新内部排名
    partner_df['internalRank'] = internal_rank_max + partner_df.index + 1

    # 更新内部排名
    partner_df['update_sql1'] = "UPDATE " + TABLE + " SET internalRank = " \
                                + partner_df['internalRank'].map(str) \
                                + " WHERE domainName = '" + partner_df['domainName'] + "'"
    partner_df['update_sql1'].map(update_to_db)


def update_external_rank():
    """
    更新合作伙伴外部排名
    参考文档：
    http://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_sql.html
    http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_sql.html
    :return:
    """

    # 查询有域名的合作伙伴
    try:
        sql = 'SELECT partnerId,domainName,internalRank,externalRank FROM ' \
              + TABLE + ' WHERE LENGTH(domainName) > 0'
        df = pd.read_sql(sql, con=engine)
    except pymysql.err.ProgrammingError as e:
        print('ProgrammingError: ' + str(e))
        sys.exit()

    # 通过域名获取外部排名
    update_sql = ''
    df.to_sql(name='t_dp_partner_backup_20181112', con=engine, if_exists='append', index_label='partnerId')

    # 更新这些排名
    print(df.head())
    df.to_sql(name='sum_case_1', con=engine, if_exists='append', index=False)
    conn.close()
    print('ok')


def calculate_rank_and_update():
    """
    根据外部排名和内部排名计算合作伙伴排名
    :return:
    """
    pass


if __name__ == '__main__':
    # 更新合作伙伴内部排名
    # 需要将域名按照排名顺序按行保存成文本文件txt，并输入该文件
    update_internal_rank(NEW_INTERNAL_RANK_PATH)

    # 更新合作伙伴外部排名
    update_external_rank()

    # 计算排名并更新
    calculate_rank_and_update()
