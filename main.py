# -*- coding: UTF-8 -*-
#!/usr/bin/python

#public moudle
import mysql.connector
from mysql.connector import errorcode

#internal module
import configparser
import datetime
import os

class DbHandler:
	def __init__(self,user, password, host, database):
		self.connectdb(user, password, host, database)

	def connectdb(self, user, password, host, database):
		'''连接数据库并返回连接实例
		'''
		userinfo = {
			'user':user,
			'password':password,
			'host':host, 
			'database':database,
			'connection_timeout':4,
			'auth_plugin':'mysql_native_password'
		}

		try:
			# 得到cnx和cursor
			self.cnx = mysql.connector.connect(**userinfo)
			self.cursor = self.cnx.cursor()

		except mysql.connector.Error as err:
			
			#用户名密码错误
			if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
				print("DB connect error: Wrong username or password")
				self.cnx = None
			
			#数据库不存在
			elif err.errno == errorcode.ER_BAD_DB_ERROR:
				print("Error: Database does not exist")
				self.cnx = None
			
			#数据库连接失败
			elif err.errno == errorcode.CR_CONN_HOST_ERROR:
				print("Error: Can't connect to MySQL server on host")
				self.cnx = None

			#其它错误
			else:
				print(err)
				self.cnx = None

	def isconnected(self):
		'''验证数据库是否连接
		'''
		if self.cnx != None:
			return True
		else:
			return False

	def readdb(self, sql, args=()):
		'''读取数据库,args是元组
		'''
		try:
			self.cursor.execute(sql,args)
			return self.cursor.fetchall()
	
		except mysql.connector.Error as err:
			print(err)
			return None

	def close(self):
		'''关闭数据库
		'''		
		self.cursor.close()
		self.cnx.close()

class NotFoundFileError(Exception):
	def __init__(self, msg1):
		self.msg1 = msg1

class ConfigureHandler:
	def __init__(self, path):		
		self.config = path

	@property
	def config(self):
		return self._config
	
	@config.setter
	def config(self, path):
		#检查config文件是否存在于path中，如果不存在则抛出错误提示
		if 'config' in os.listdir(path):
			self._config = configparser.ConfigParser()
			self._config.read(os.path.join(path, 'config'))
		else:
			raise NotFoundFileError("Can't find config file at \""+os.getcwd()+'\",config file should be at the same path as the main.py')

	def get_dbinfo(self):
		'''获取数据库登陆信息
		'''
		db_userinfo = {
			'user':self.config['db_info']['user'],
			'password':self.config['db_info']['password'],
			'host':self.config['db_info']['host'], 
			'database':self.config['db_info']['database'],
		}
		return db_userinfo	

	def get_timeinfo(self):
		'''获取时间信息
		'''
		time_info = {
			'start':self.config['time_range']['start'],
			'end':self.config['time_range']['end'],
			'skip_weekend':self.config['time_range']['skip_weekend']
		}
		return time_info

	def get_sqlinfo(self):
		'''获取需要计算的表和相关sql语句信息
		'''
		tables = self.config['sql_info']['tables'].split(',')
		interfaces = self.config['sql_info']['column'].split(',')
		sql_info = {
			'args':list(zip(tables,interfaces,self.get_max_bw(tables,interfaces))),	
			'bw_direction':self.config['sql_info']['bw_direction']
		}
		return sql_info

	def get_max_bw(self,devs,interfaces):
		'''得到线路的最大带宽
		tables：列表
		interfaces：列表
		返回各接口带宽最大值的列表
		'''
		res = []
		db = DbHandler(**self.get_dbinfo())
		if db.isconnected():
			for dev,interface in list(zip(devs,interfaces)):
				sql = "SELECT bw FROM "+'circuit_info'+" where dev=%s and interface=%s"
				args = (dev,interface)
				res.append(db.readdb(sql, args)[0][0])
			db.close()
		return res

def get_timerange(start,end,skip_weekend):
	'''获得一个时间全段
	skip_weekend判断是否跳过周末
	返回一个由日期组成的列表
	'''
	dates = []
	dt = datetime.datetime.strptime(start, "%Y-%m-%d")  
	date = start

	while date <= end:
		#判断是否是周末,只计算非周末
		if skip_weekend == 'True':
			if dt.strftime('%w') != '6' and dt.strftime('%w') != '0':
					dates.append(date)
		else:
			dates.append(date)
		dt = dt + datetime.timedelta(days=1)
		date = dt.strftime("%Y-%m-%d")
	return dates

def get_oneday(db_info,table,interface,max_bw,start_time,end_time,direction):
	'''得到一个接口一天的平均值数据
	'''
	bw_direction = {'in':'inbw','out':'outbw'}.get(direction)
	sql = ("SELECT AVG("+bw_direction+") FROM "+table+
		" WHERE interface=%s"
		" AND time between %s AND %s")
	args = (interface,start_time,end_time)
	db = DbHandler(**db_info)
	if db.isconnected():
		res = db.readdb(sql,args)
		db.close()
		#判断返回的结果是否有效，当查询日期没有数据时结果为空，返回'None'
		if res[0][0]:
			return str(round(res[0][0]/int(max_bw),4))
		else:
			return 'None'
	else:
		return 'None'

def main(path):
	config = ConfigureHandler(path)
	db_info = config.get_dbinfo()
	sql_info = config.get_sqlinfo()
	date_range = get_timerange(**config.get_timeinfo())
	res = []
	#按照日期进行数据收集
	for date in date_range:
		print('calculating data at '+date)
		line = date
		for table,interface,max_bw in sql_info['args']:			
			print(table,interface,max_bw)
			line += ','+get_oneday(
				db_info,
				table,
				interface,
				max_bw,
				date+' 9:00:00',
				date+' 16:00:00',
				sql_info['bw_direction'])		
		res.append(line)
	#写入csv文件
	file_name = date_range[0]+'to'+date_range[-1]+'.csv'
	with open(os.path.join(path,file_name), 'w') as file:
		for i in res:
			file.write(i)
			file.write('\n')
	input('Find report at '+path+'\\'+file_name)

if __name__ == '__main__':
	main(os.getcwd())


		
