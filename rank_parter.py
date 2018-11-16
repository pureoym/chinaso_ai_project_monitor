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
import random
import urllib.request
import pandas as pd
import pymysql
import sys

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

ALEXA_URL = ["http://data.alexa.com/data/TCaX/0+qO000fV?cli=10&url="]  # alexa接口URL列表
# ALEXA_URL = ["http://data.alexa.com/data/TCaX/0+qO000fV?cli=10&url=",
#              "http://data.alexa.com/data/ezdy01DOo100QI?cli=10&url=",
#              "http://data.alexa.com/data/+wQ411en8000lA?cli=10&url="] # alexa接口URL列表


# 更新的合作伙伴表名
TABLE = 't_dp_partner_backup_20181112'
# 新增内部排名的文件地址
NEW_INTERNAL_RANK_PATH = '/application/search/partner_rank/partner_internal_rank_20181112.txt'
ALEXA_RANK = '/application/search/partner_rank/alexa_rank.csv'

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


def execute_sql(sql):
    """
    执行sql语句 test
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


def update_internal_rank(internal_rank_data_path):
    """
    更新合作伙伴内部排名
    由原来合作伙伴表中最大内部排名依次继续向下排名，作为新增需要排名的合作伙伴的内部排名。
    :param internal_rank_data_path:
    :return:
    """
    partner_df = pd.read_csv(internal_rank_data_path, header=None, names=['domainName'])

    # 获取原最高内部排名
    sql1 = "SELECT MAX(internalRank) FROM " + TABLE
    df1 = pd.read_sql(sql1, con=conn)
    internal_rank_max = df1['MAX(internalRank)'][0]

    # 计算新内部排名
    partner_df['internalRank'] = internal_rank_max + partner_df.index + 1

    # 更新内部排名
    partner_df['update_sql'] = "UPDATE " + TABLE + " SET internalRank = " \
                               + partner_df['internalRank'].map(str) \
                               + " WHERE domainName = '" + partner_df['domainName'] + "'"
    partner_df['update_sql'].map(execute_sql)


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
        sql = 'SELECT partnerId,domainName FROM ' + TABLE + ' WHERE LENGTH(domainName) > 0'
        df = pd.read_sql(sql, con=conn)
    except pymysql.err.ProgrammingError as e:
        print('ProgrammingError: ' + str(e))
        sys.exit()

    # 通过域名获取外部排名
    df['externalRank'] = df['domainName'].map(download_rank)

    # 保存下载结果
    df.to_csv(ALEXA_RANK, index=False)

    # 更新内部排名
    df['sql'] = "UPDATE " + TABLE + " SET externalRank = " \
                + df['externalRank'].map(str) + " WHERE partnerId = '" \
                + df['partnerId'].map(str) + "'"
    df['sql'].map(execute_sql)

    return df


def download_rank(domain_name):
    """
    通过alex接口下载网站排名
    :param url: 网站的url
    :return: 网站的排名，如果无数据则返回0
    """
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
    """
    从Alexa接口URL列表中随机挑选一个生成请求URL
    :param domain_name:
    :return:
    """
    return random.choice(ALEXA_URL) + domain_name


def parse_rank_from_xml(xml_str):
    """
    解析抓取到的XML内容，得到RANK
    :param xml_str:
    :return:
    """
    rank = 0
    try:
        root = ET.fromstring(xml_str)
        for country in root.findall('SD'):
            if country.find('COUNTRY').get('CODE') == 'CN':
                rank = int(country.find('COUNTRY').get('RANK'))
    except Exception as e:
        print('Parse xml error, error xml:', xml_str)
    return rank


def calculate_rank_and_update():
    """
    根据外部排名和内部排名计算合作伙伴排名
    :return:
    """
    # 查询有内部排名或者外部排名的合作伙伴
    try:
        sql = 'SELECT partnerId,internalRank,externalRank FROM ' + TABLE + ' WHERE externalRank != 0 OR internalRank != 0'
        df = pd.read_sql(sql, con=conn)
    except pymysql.err.ProgrammingError as e:
        print('ProgrammingError: ' + str(e))
        sys.exit()

    imax = df['internalRank'].max()
    imin = df['internalRank'].min()
    emax = df['externalRank'].max()
    emin = df['externalRank'].min()

    # 排名归一化
    df['norm_internal_rank'] = df['internalRank'] \
        .map(lambda x: (imax - x + 1) / (imax - imin + 1.0)) \
        .map(f1)
    df['norm_external_rank'] = df['externalRank'] \
        .map(lambda x: (emax - x + 1) / (emax - emin + 1.0)) \
        .map(f1)

    # 通过内部排名和外部排名计算合作伙伴排名
    external_weight = 1.0 / (imax + 2)
    df['rank'] = df['norm_internal_rank'] * (1.0 - external_weight) + df['norm_external_rank'] * external_weight
    df['rank'] = df['rank'].map(str)

    # 更新内部排名
    df['update_sql'] = "UPDATE " + TABLE + " SET rank = " + df['rank'] \
                       + " WHERE partnerId = '" + df['partnerId'].map(str) + "'"
    df['update_sql'].map(execute_sql)

    return df


def f1(input):
    if input == 1.0:
        return 0.0
    else:
        return input


if __name__ == '__main__':
    # 更新合作伙伴内部排名
    # 需要将域名按照排名顺序按行保存成文本文件txt，并输入该文件
    print('######### update_internal_rank #########')
    # update_internal_rank(NEW_INTERNAL_RANK_PATH)

    # 更新合作伙伴外部排名
    print('######### update_external_rank #########')
    # df1 = update_external_rank()

    # 计算排名并更新
    print('######### calculate_rank_and_update #########')
    calculate_rank_and_update()

    # 关闭mysql连接
    print('######### job done! #########')
    conn.close()
