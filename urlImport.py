#-*- coding: UTF-8 -*-
import datetime
import MySQLdb
from urllib2 import urlopen
import json

URL = 'http://data.cms.mgt.chinaso365.com/srv/search/get.json?fields=TitleCN,SourceUrl,Tag&collectionName=news&collectionId=2&dataType=hot&searchParams={%22QueryId%22:{%22queryType%22:%22in%22,%22value%22:[%223618527456%22]},%22Status%22:{%22queryType%22:%22equal%22,%22value%22:%223%22},pageNo:0,pageSize:30}'
HOST = '10.10.65.231'
USER = 'cp'
PASSWD = 'cp_123'
DB = 'cp'
PORT = 3306

def get_sourceUrls(seedUrls):
	sourceUrls = []
	response = urlopen(URL)
	json_str = response.read()
	data = json.loads(json_str)
	results = data["content"]
	for result in results:
		sourceUrl = result.get('SourceUrl')
		sourceUrl = sourceUrl.encode('utf-8')
		if sourceUrl not in seedUrls:
			sourceUrls.append(sourceUrl)
	return sourceUrls
	
def get_seedUrl_list():
	seedUrls = []
	try:
		conn = MySQLdb.connect(HOST,USER,PASSWD,DB,PORT)
		cursor = conn.cursor()
		sql = 'SELECT seedUrl FROM cp_seed WHERE taskId = 8959'
		cursor.execute('SET NAMES utf8')
		cursor.execute(sql)
		rows = cursor.fetchall()
		for row in rows:
			seedUrl = row[0]
			seedUrls.append(seedUrl)
	finally:
		cursor.close()
		conn.close()

	return seedUrls

def save_seeds(sourceUrls):
	status = False
	try:
		conn = MySQLdb.connect(HOST,USER,PASSWD,DB,PORT)
		cursor = conn.cursor()
		cursor.execute('SET NAMES utf8')
		nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		for sourceUrl in sourceUrls:
			insert_sql = "INSERT INTO cp_seed(taskId,seedUrl,commonFields,deleteState,maxDepth,period,lastUpdateUser,lastUpdateTime) VALUES ('%d','%s','%s','%d','%d','%d','%s','%s')" % (8959,sourceUrl,'newsLabel=照谣镜;newsLabelSecond=社会滚动;ignoreCompare=1',0,1,-1,'urlImport',nowTime)
			cursor.execute(insert_sql)
		conn.commit()
		status = True
	except Exception,e:
		conn.rollback() # 确保批量提交的事务性
	finally:
		cursor.close()
		conn.close()

	return status

if __name__ == "__main__":
	seedUrls = get_seedUrl_list()
	sourceUrls = get_sourceUrls(seedUrls)
	print sourceUrls
	status = save_seeds(sourceUrls)
	print status
	
