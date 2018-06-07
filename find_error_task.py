#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import smtplib  
from email.mime.text import MIMEText  

#发送报警邮件的异常url数量门限
THRESHOLD=200
ERROR_RATE=0.1
#邮件配置
#收信人
#mailto_list=['bigdatainterface@chinaso.com'] 
mailto_list=['wuzhihua@chinaso.com','liangxiangyu@chinaso.com']
#mailto_list=['ouyangming@chinaso.com']
mail_host="mail.chinaso.com"  #设置服务器
mail_user="bigdatawarning"    #用户名
mail_pass="Chinaso123"   #口令 
mail_postfix="chinaso.com"  #发件箱的后缀
# mysql登陆信息
host = '10.10.65.231'
user = 'cp'
passwd = 'cp_123'
db = 'cp'
port = 3306

def get_task_and_error_url_list():
    '''通过mysql获取在更新的新闻与图片任务id：task_id,
    根据任务id查询url表获得该任务的异常url数目
    返回元组(task_id,error_url_count)列表'''

    task_id = 0
    error_count = 0
    total_count = 0
    result = []

    try:
        conn = MySQLdb.connect(host,user,passwd,db,port)
        cursor = conn.cursor()
        sql = 'SELECT t.Id,t.creator FROM cp_task t WHERE t.resourceType LIKE \'%01271%\'\
            OR t.resourceType LIKE \'%01276%\' AND t.period != -1'
        cursor.execute('SET NAMES utf8')
        cursor.execute(sql)
        rows = cursor.fetchall()
		
        for row in rows:
            task_id = str(row[0])
            creator = str(row[1])
            
            sql2 = 'SELECT COUNT(*) FROM cp_url_'+task_id+' WHERE state in (12,28,29)'
            sql2 += 'UNION SELECT COUNT(*) FROM cp_url_'+task_id
            
            try:
                cursor.execute(sql2)
                rows2 = cursor.fetchall()
                error_count = int(rows2[0][0])

                # 如果MySQL union语句中两结果相同，则合并。故做如下处理。
                if error_count > 0:
                    if len(rows2) > 1:
                        total_count = int(rows2[1][0])
                    else:
                        total_count = error_count
                    result.append((int(task_id),error_count,total_count,creator))
            except Exception, e: 
                print 'mysql_error:'+sql2
                print str(e)
                continue
        
        return result 

    finally:
        cursor.close()
        conn.close()

def get_error_tasks(list):
    '''通过门限过滤列表，过滤保留列表中异常URL大于门限或者比例大于门限比例的，仅返回taskid'''
    list_add_rate = map(lambda x:(x[0],x[1],x[2],x[1]*1.0/x[2],x[3]),list)
    list_filted = filter(lambda x:x[1]>THRESHOLD or x[3]>ERROR_RATE, list_add_rate)
    #print list_filted
    taskid_list = map(lambda x:(x[0],x[4]), list_filted)
    return taskid_list

def send_email(to_list,sub,content):  
    '''发送邮件'''
    me="大数据部系统监控"+"<"+mail_user+"@"+mail_postfix+">"  
    msg = MIMEText(content,_subtype='plain',_charset='utf-8')  
    msg['Subject'] = sub  
    msg['From'] = me  
    msg['To'] = ";".join(to_list)  
    try:  
        server = smtplib.SMTP()  
        server.connect(mail_host)  
        server.login(mail_user,mail_pass)  
        server.sendmail(me, to_list, msg.as_string())  
        server.close()  
        return True  
    except Exception, e:  
        print 'send_email_error:' + str(e)  
        return False

def send_error_task_email(l1,l2):
    '''发送问题任务的告警邮件'''
    email_title='问题任务id报告'
    content='在任务的url表中，问题url数量超过'+str(THRESHOLD)+'时，或者问题url比例超过'+str(ERROR_RATE)+'的任务，将被判断为异常任务。'
    content+='\n当前新闻与新闻图片任务数：'+str(len(l1))
    content+='。其中问题任务数：'+str(len(l2))
    content+='。具体列表如下：\n'+str(l2)
    if send_email(mailto_list,email_title,content):
        print '发送成功'
    else:
        print '发送失败'

if __name__ == '__main__':
    # 获取异常URL的数列表
    l1 = get_task_and_error_url_list()
    #print l1
    print '所有任务数量：'+str(len(l1)) 

    # 根据阈值获取需要报警的任务列表
    l2 = get_error_tasks(l1)
    print '异常任务数量：'+str(len(l2))

    # 发送报警邮件
    send_error_task_email(l1,l2)
