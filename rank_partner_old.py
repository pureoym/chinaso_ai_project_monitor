#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : pureoym
# @Contact : pureoym@163.com
# @TIME    : 2018/11/8 10:05
# @File    : rank_partner_old.py
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
# -*- coding: UTF-8 -*-

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


def update_internal_rank():
    """
    更新内部排名
    :return:
    """
    partner_df = pd.read_csv(NEW_INTERNAL_RANK_PATH, header=None, names=['domainName'])

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
    pandas与mysql交互测试
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

    def get_partner_list():
        '''获取合作伙伴列表:
            输出：[partnerId,domainName,internalRank,externalRank]'''
        partners = []
        try:
            conn = pymysql.connect(HOST, USER, PASSWD, DB, PORT)
            cursor = conn.cursor()
            sql = 'SELECT partnerId,partnerName,domainName,internalRank,externalRank,rank FROM t_dp_partner_bak_20170508 WHERE resourceTypes LIKE \'%01276%\''
            cursor.execute('SET NAMES utf8')
            cursor.execute(sql)
            rows = cursor.fetchall()
            for row in rows:
                partnerId = int(row[0])
                partnerName = row[1]
                domainName = row[2]
                internalRank = int(row[3])
                externalRank = int(row[4])
                rank = row[5]
                partners.append([partnerId, domainName, internalRank, externalRank, rank])
        finally:
            cursor.close()
            conn.close()

        return partners

    def update_external_rank(partners):
        '''更新外部排名:
            输入：[pid,domainName,internalRank,externalRank]
            输出：[pid,internalRank,externalRank]'''
        return map(lambda x: [x[0], x[2], crawl_alexa(x[1])], partners)

    def crawl_alexa(domain_name):
        '''抓取域名对应的alexa排名'''
        if domain_name is None:  # 如果输入域名为空，则返回0
            return 0
        if domain_name == '':  # 如果输入域名为空串，则返回0
            return 0
        else:  # 否则，返回抓取的alexa排名
            url = generate_url(domain_name)  # 从Alexa接口URL列表中随机挑选一个生成请求URL
            req = urllib.request.Request(url)
            try:
                response = urllib.request.urlopen(req)
            except Exception as e:
                if hasattr(e, 'reason'):
                    print('URLError, reason: ', e.reason)
                elif hasattr(e, 'code'):
                    print('Error code: ', e.code)
            else:
                xml_str = response.read()
                rank = parse_rank_from_xml(xml_str)
                print('rank[%s]=%s' % (domain_name, rank))
                return rank
        return 0

    def generate_url(domain_name):
        '''从Alexa接口URL列表中随机挑选一个生成请求URL'''
        return random.choice(ALEXA_URL) + domain_name

    def parse_rank_from_xml(xml_str):
        '''解析抓取到的XML内容，得到RANK'''
        rank = 0
        try:
            root = ET.fromstring(xml_str)
            for country in root.findall('SD'):
                if country.find('COUNTRY').get('CODE') == 'CN':
                    rank = int(country.find('COUNTRY').get('RANK'))
        except Exception as e:
            print('Parse xml error, error xml:', xml_str)
        return rank

    def compute_rank(partners):
        '''计算排名
            输入:[pid,internal_rank,external_rank]
            输出:[pid,internal_rank,external_rank,norm_in_rank,norm_ex_rank,rank]'''
        pa = np.array(partners)
        in_rank = pa[:, 1]  # 获取内部排名
        norm_in_rank = normalize(in_rank)  # 内部排名归一化
        ex_rank = pa[:, 2]  # 获取外部排名
        norm_ex_rank = normalize(ex_rank)  # 外部排名归一化
        rank = INTERNAL_WEIGHT * norm_in_rank + (1 - INTERNAL_WEIGHT) * norm_ex_rank  # 根据归一化的排名和权重计算最终排名
        pa_ranked = np.c_[pa, norm_in_rank, norm_ex_rank, rank]
        return pa_ranked

    def normalize(a):
        '''归一化，只处理非零元素'''
        max_a = a[np.nonzero(a)].max()
        min_a = a[np.nonzero(a)].min()
        norm_a_with_zero_term = (max_a - a + 1) / (max_a - min_a + 1.0)
        norm_a = norm_a_with_zero_term * (a != 0)  # 去除非零项
        return norm_a

    def save_rank(partners):
        '''保存入库
            输入:[pid,internal_rank,external_rank,norm_in_rank,norm_ex_rank,rank]'''
        status = False
        try:
            conn = pymysql.connect(HOST, USER, PASSWD, DB, PORT)
            cursor = conn.cursor()
            cursor.execute('SET NAMES utf8')
            for partner in partners:
                pid = int(partner[0])
                external_rank = int(partner[2])
                rank = partner[5]
                update_sql = 'UPDATE t_dp_partner_bak_20170508 SET externalRank=%s,rank=%s WHERE partnerId=%s' % (
                    external_rank, rank, pid)
                if rank > 0:
                    print(update_sql)
                cursor.execute(update_sql)
            conn.commit()
            status = True
        except Exception as e:
            conn.rollback()  # 确保批量提交的事务性
        finally:
            cursor.close()
            conn.close()

        return status

    def partner_cmp(x, y):
        if x[5] > y[5]:
            return -1
        if x[5] < y[5]:
            return 1
        return 0


if __name__ == "__main__":

    # 连接数据库
    conf = {'host': '10.10.65.232',
            'user': 'opendata',
            'passwd': 'opendata_123',
            'db': 'opendata',
            'port': 3306}
    conn = get_mysql_conn(conf)
    table_name = "t_dp_partner_backup_20181112"

    # 更新原有内部排名
    input_path = "/application/search/partner_rank/partner_update_internal_rank.txt"
    update_internal_rank(input_path, table_name)
    # 读取文件
    # path_1 = "/application/search/partner_rank/partner_update_internal_rank.txt"
    # partner_df_1 = pd.read_csv(path_1, header=None, names=['domainName'])
    #
    # # 获取原最高内部排名
    # sql1 = "SELECT MAX(internalRank) FROM " + table_name
    # internal_rank_max = execute_and_get_result(conn, sql1)[0][0]
    #
    # # 计算新内部排名
    # partner_df_1['internalRank'] = internal_rank_max + partner_df_1.index + 1
    #
    # # 更新内部排名
    # partner_df_1['update_sql1'] = "UPDATE " + table_name + " SET internalRank = " \
    #                               + partner_df_1['internalRank'].map(str) \
    #                               + " WHERE domainName = '" + partner_df_1['domainName'] + "'"
    # partner_df_1['update_sql1'].map(update_to_db)

    # 更新外部排名
    update_external_rank(table_name)

    # 计算排名

    "UPDATE t_dp_partner SET internalRank = 101 WHERE domainName = 'http://www.gov.cn'"

    conn.close()

    # 从mysqlcp库中读取合作伙伴列表
    partners = get_partner_list()
    print("获取需要处理的新闻合作伙伴，数量:" + str(len(partners)))

    # 根据合作伙伴列表中的域名调用alexa接口查询排名
    print("抓取alexa排名：")
    partners_updated = update_external_rank(partners)

    # 根据外部排名和内部排名，计算排名
    print("计算排名：")
    partners_ranked = compute_rank(partners_updated)
    partners_final = sorted(partners_ranked, partner_cmp)
    for i in range(10):
        print(partners_final[i])

    # 保存入库
    print("保存排名计算结果")
    # result = save_rank(partners_ranked)
    # if result == True:
    #     print("保存成功！")
    # else:
    #     print("保存失败！")
