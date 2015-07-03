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
import datetime
from time import strftime
from threading import Thread

redisDB = redis.StrictRedis(host='localhost', port=6379, db=0)

number_of_users = parameter.number_of_users
SimulateStart = strftime("%Y%m%d-%H%M%S %P")


#user define
class User(threading.Thread):
	is_online = True
	is_regist = False
	def __init__(self, user_ID):
		threading.Thread.__init__(self)
		self.user_ID = user_ID
		self.redisMQ = redis.Redis()

	#Generate Random Timings for a Poisson Process
	#http://preshing.com/20111007/how-to-generate-random-timings-for-a-poisson-process/
	def nextTime(self, rateParameter):
		return -math.log(1.0 - random.random()) / rateParameter

	#turn on and off is_online by random poisson
	def clock(self):
		while True:
			self.is_online = not self.is_online
			request = {"user_ID":self.user_ID, "is_online":self.is_online}
			self.redisMQ.rpush('is_online['+str(self.user_ID)+']', str(request))

			t = self.nextTime(1.0/5.0)#next online/leave time
			time.sleep(5)			

	#user thread start
	def run(self):
		print '%r run' %(self.user_ID)
		timer = Thread(target=self.clock)
		timer.start()
		while True:
			self.is_regist = redisDB.get("is_regist["+str(self.user_ID)+']')
			if(self.is_online and self.is_regist):
				t = self.nextTime(1.0/0.4)#next sending time
				time.sleep(1)

				print self.user_ID#send message to LB


#create users
userList = []
for i in range(1, number_of_users+1):
	userList.append(User(i))# ID = 1 ~

#user start
for i in userList:
	i.start()