# -*- coding: utf-8 -*-
# author: pureoym
# time: 2018/1/22 17:24
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
# ==============================================================================
import MySQLdb

HOST = '10.10.65.232'
USER = 'opendata'
PASSWD = 'opendata_123'
DB = 'opendata'
PORT = 3306


def update_partner():
    '''
    获取dp_partner表的新闻条目，
    更新这些条目，在该条目的resourceTypes字段内增加资源类型01324
    :return:
    '''
    status = False
    try:
        conn = MySQLdb.connect(HOST, USER, PASSWD, DB, PORT)
        cursor = conn.cursor()

        # search partner
        sql = 'SELECT partnerId,resourceTypes FROM t_dp_partner WHERE ' \
              'resourceTypes LIKE \'%01276%\' AND resourceTypes NOT LIKE \'%01324%\''
        cursor.execute('SET NAMES utf8')
        cursor.execute(sql)
        rows = cursor.fetchall()
        print 'update_partner: sql = %s, len() = %s' % (sql, len(rows))

        # update partner
        count = 0
        for row in rows:
            partnerId = row[0]
            resourceTypes = row[1]
            new_resourceTypes = resourceTypes + ',01324'
            update_sql = 'UPDATE t_dp_partner SET resourceTypes=\'%s\' WHERE partnerId=%s' \
                         % (new_resourceTypes, partnerId)
            # print 'update_partner: update_sql = ' + update_sql
            cursor.execute(update_sql)
            count += 1

        conn.commit()
        print 'update_partner: update count = %s' % count
        status = True
    except Exception, e:
        print e
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def insert_flow_control():
    '''
    获取所有flow_control表的新闻条目，
    新增01324条目，结构如下：
    (resourceType = 01324,
    partnerId = 原01276中的partnerId
    autoVerify = 1
    autoGenerateContent = 1)
    :return:
    '''
    status = False
    try:
        conn = MySQLdb.connect(HOST, USER, PASSWD, DB, PORT)
        cursor = conn.cursor()

        # get l1: list of 01276
        sql = 'SELECT * FROM t_dp_flow_control WHERE resourceType = 01276'

        cursor.execute('SET NAMES utf8')
        cursor.execute(sql)
        rows = cursor.fetchall()
        print 'insert_flow_control: sql = %s, len(list_01276) = %s' % (sql, len(rows))

        # get exist_list: list of 01324
        sql2 = 'SELECT * FROM t_dp_flow_control WHERE resourceType = 01324'
        cursor.execute('SET NAMES utf8')
        cursor.execute(sql2)
        rows2 = cursor.fetchall()
        exist_list = []
        print 'insert_flow_control: sql2 = %s, len(exist_list) = %s' % (sql2, len(rows2))
        for row2 in rows2:
            partnerId = row2[1]
            exist_list.append(partnerId)

        # only insert l1 - exist_list
        count = 0
        for row in rows:
            resourceType = '01324'
            partnerId = row[1]
            autoVerify = '1'
            autoGenerateContent = '1'
            if partnerId in exist_list:
                continue
            insert_sql = 'INSERT INTO t_dp_flow_control VALUES (\'%s\',\'%s\',\'%s\',\'%s\')' \
                         % (resourceType, partnerId, autoVerify, autoGenerateContent)
            # print 'insert_flow_control: insert_sql = ' + insert_sql
            cursor.execute(insert_sql)
            count += 1

        conn.commit()
        print 'insert_flow_control: insert count = %s' % count
        status = True
    except Exception, e:
        print e
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def main():
    update_partner()
    print ('update_partner: job done')

    insert_flow_control()
    print ('insert_flow_control: job done')


if __name__ == "__main__":
    main()
