#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : pureoym
# @Contact : pureoym@163.com
# @TIME    : 2018/11/14 15:40
# @File    : test.py
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
import pymysql
import sys
import pandas as pd

TABLE = 't_dp_partner_backup_20181112'

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


def calculate_rank_and_update():
    """
    根据外部排名和内部排名计算合作伙伴排名
    http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_sql.html
    :return:
    """
    # 查询有内部排名或者外部排名的合作伙伴
    try:
        sql = 'SELECT partnerId,internalRank,externalRank FROM ' + TABLE + \
              ' WHERE externalRank != 0 OR internalRank != 0'
        df = pd.read_sql(sql, con=conn)
    except pymysql.err.ProgrammingError as e:
        print('ProgrammingError: ' + str(e))
        sys.exit()

    df['rank'] = 0

    # 更新操作
    df['sql'] = "UPDATE " + TABLE + " SET rank = " + df['rank'].map(str) \
                       + " WHERE partnerId = '" + df['partnerId'].map(str) + "'"
    df['sql'].map(execute_sql)

    return df


def execute_sql(sql):
    """
    执行sql语句
    :param sql:
    :return:
    """
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
    except pymysql.Error as e:
        conn.rollback()
        # logger.error(e)
        print(e)


if __name__ == '__main__':
    calculate_rank_and_update()

    # 关闭mysql连接
    conn.close()
