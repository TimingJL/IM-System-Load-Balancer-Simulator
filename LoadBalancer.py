#coding=utf-8
import time
import math
import random
import parameter
import numpy as np
import redis
import thread
import threading
import ast
import SchedulerPolicy
from time import strftime
from threading import Thread

redisDB = redis.StrictRedis(host='localhost', port=6379, db=0)
redisMQ = redis.Redis()

redisDB.set("RRcount",0)
number_of_users = parameter.number_of_users

def LogIn():
	while True:
		user_ID = redisMQ.blpop("LogIn")#('user_ID', '1')

		#CM_ID = SchedulerPolicy.myRandom()
		CM_ID = SchedulerPolicy.myRR()

		LBList[int(user_ID[1]) - 1].receiver = CM_ID		
		#print 'user_%r to CM_%r' %(user_ID[1], CM_ID)
		print 'user_%r to CM_%r' %(user_ID[1], LBList[int(user_ID[1]) - 1].receiver)

def LogOut():
	while True:
		user_ID = redisMQ.blpop("LogOut")
		LBList[int(user_ID[1]) - 1].receiver = 0
		print '%r LogOut' %(user_ID[1])

#LB define
class LoadBalancer(threading.Thread):
	is_user_online = False
	receiver = 0
	def __init__(self, user_ID):
		threading.Thread.__init__(self)
		self.user_ID = user_ID
		self.redisMQ = redis.Redis()

	#LB thread start
	def run(self):		
		while True:
			val = self.redisMQ.blpop('is_online['+str(self.user_ID)+']')#type(val) = tuple
			#print val
			request = ast.literal_eval(val[1])#translate tuple to dict
			self.is_user_online = request["is_online"]#request on_line or not
			if self.is_user_online:
				#given a CM address
				self.redisMQ.rpush("LogIn",self.user_ID)
				while(receiver>0):#when user was dispatch to any CM					
					#self.redisMQ.blpop("")#receive message from Client					
					#self.redisMQ.rpush(,)#send message to the CM
					pass
			else:
				#Log Out from CM list
				self.redisMQ.rpush("LogOut",self.user_ID)
				pass

			redisDB.set("is_regist["+str(self.user_ID)+"]",self.is_user_online)#tell user(Client.py) is_regist

#create LB
LBList = []
for i in range(1, number_of_users+1):
	LBList.append(LoadBalancer(i))# ID = 1 ~

#user start
for i in LBList:
	i.start()

th = Thread(target=LogIn)
th2 = Thread(target=LogOut)
th.start()
th2.start()

print 'Waiting...'