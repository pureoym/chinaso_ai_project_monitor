#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : pureoym
# @Contact : pureoym@163.com
# @TIME    : 2018/11/9 10:20
# @File    : mysql_utils.py
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
import logging
from logging.handlers import TimedRotatingFileHandler


# LOG_PATH = '/data0/search/weibo-spider/01347/logs/log'  # log保存路径
# logger = logging.getLogger('weibo_spider')
# formatter = logging.Formatter(
#     '[%(asctime)s] [%(threadName)s:%(thread)d] [%(levelname)s]: %(message)s')
# file_handler = TimedRotatingFileHandler(LOG_PATH, when='midnight', encoding='utf-8')
# file_handler.setFormatter(formatter)
# logger.addHandler(file_handler)
# logger.setLevel(logging.DEBUG)


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

