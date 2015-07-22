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
from collections import deque

redisDB = redis.StrictRedis(host='localhost', port=6379, db=0)

number_of_users = parameter.number_of_users
SimulateStart = strftime("%Y%m%d-%H%M%S %P")
does_use_random_seed = False
Log = []


#user define
class User(threading.Thread):
	#generate pareto network delay 
	if does_use_random_seed == False:
		temp = np.random.pareto(1.1, 1) + 0. #(init) x = np.random.pareto(shape, size) + lower
		network_delay = temp[0]
	def __init__(self, user_ID):
		threading.Thread.__init__(self)
		self.user_ID = user_ID
		self.redisMQ = redis.Redis()
		self.is_online = False
		self.is_regist = False		
		#=========use constant random seed============================
		if does_use_random_seed == True:
			np.random.seed(user_ID)#use user ID as random seed temporarily
			self.receiver_List = deque((np.random.poisson(6000, 5000)%number_of_users +1))#np.random.poisson(average, size)
			self.online_time_List = deque((np.random.poisson(30, 5000)))
			#self.message_sending_List = deque((np.random.poisson(5, 5000)))
			self.message_sending_List = deque([])
			for i in range(0,3000):
				random.seed(self.user_ID)
				rs = random.sample(range(3000), 3000)
				random.seed(rs[i])
				self.message_sending_List.append(-math.log(1.0 - random.random()) / (1/0.4))
			self.network_delay_List = deque(np.random.pareto(1.1, 5000) + 0.)#(reset) x = np.random.pareto(shape, size) + lower
			self.delta_List = deque(np.random.normal(0, 0.1, 5000))#np.random.normal(mean , standard deviation, 1), type = array
			self.network_delay = self.network_delay_List.popleft()
			# print self.receiver_List
			# print self.online_time_List
			# print self.message_sending_List
			# print self.network_delay_List
		#=============================================================

	#Generate Random Timings for a Poisson Process
	#http://preshing.com/20111007/how-to-generate-random-timings-for-a-poisson-process/
	def nextTime(self, rateParameter):
		return -math.log(1.0 - random.random()) / rateParameter

	#turn on and off is_online by random poisson
	def clock(self):
		while True:
			self.is_online = not self.is_online
			request = {"user_ID":self.user_ID, "is_online":self.is_online}
			self.redisMQ.rpush('is_online['+str(self.user_ID)+']', str(request))#tell LB to regist/erase
			print "send regist"
			###################################
			LogTemp = {"type":0,"content":request}
			Log.append(LogTemp)
			###################################
			if(self.is_online):
				#when online, generate sudo network delay
				if does_use_random_seed:
					self.network_delay = self.network_delay_List.popleft()
				else:
					 tmp = np.random.pareto(1.1, 1) + 0. #(reset) x = np.random.pareto(shape, size) + lower
					 self.network_delay = tmp[0]
			redisDB.set("user["+str(self.user_ID)+"]_network_delay",self.network_delay)

			#next online/leave time
			if does_use_random_seed:
				t = self.online_time_List.popleft()
			else:	
				#t = self.nextTime(1.0/30.0)
				temp = np.random.poisson(60, 1)
				t = temp[0]
			time.sleep(t)

	#user thread start
	def run(self):
		print '%r run' %(self.user_ID)
		timer = Thread(target=self.clock)
		timer.start()
		while True:
			self.is_regist = redisDB.get("is_regist["+str(self.user_ID)+']')			
			# if user online and regist to CM succeed, than starting to send message
			if(self.is_online and self.is_regist):
				print "yes"
				#next sending time
				if does_use_random_seed:
					t = self.message_sending_List.popleft()
				else:
					t = self.nextTime(1.0/1.)
				time.sleep(t)

				#random generate a receiver
				if does_use_random_seed:
					#routine
					receiver_ID = self.receiver_List.popleft()
				else:
					#real random
					receiver_ID_tuple = random.randint(1,number_of_users),#a <= N <= b', tuple(1,)
					receiver_ID = receiver_ID_tuple[0]

				#assign the sudo network delay 
				network_delay = userList[receiver_ID-1].network_delay#type = array
				if does_use_random_seed:
					delta = self.delta_List.popleft()
				else:
					tmp = np.random.normal(0, 0.1, 1)#np.random.normal(mean , standard deviation, 1), type = array
					delta = tmp[0]
				delayTime = network_delay+delta

				#delayTime must > 0
				if (delayTime < 0.):
					delayTime = 0.00001
				#TCP session timeout = 1800: https://nkongkimo.wordpress.com/2010/09/
				if (delayTime > 1800.):
					delayTime = 1800.
				userList[receiver_ID-1].network_delay = delayTime
				#redisDB.set("user["+str(self.user_ID)+"]_network_delay")

				message = {"test_time_stamp":SimulateStart, 
							"sender_ID":self.user_ID,
							"receiver_ID":receiver_ID,#a <= N <= b
							"message_birth_time":time.time(),
							"network_delay":delayTime,
							"is_VIP":False,
							}
				#print "ID[%r], Delay: %r"%(message["sender_ID"],message["network_delay"])

				# send message to LB								
				self.redisMQ.rpush("senderID[" + str(self.user_ID) + "]", message)
				print message
				###################################
				LogTemp = {"type":1,"content":message}
				Log.append(LogTemp)
				###################################

#create users
userList = []
for i in range(1, number_of_users+1):
	userList.append(User(i))# ID = 1 ~

#user start
for i in userList:
	i.start()
	#time.sleep(3)

def LogBehavior():
	time.sleep(20*60)
	i = 1
	for item in Log:
		redisDB.set("LogBehavior[" + str(i) + "][2]",item)
		redisDB.set("LogItemNumber[2]",i)
		i = i + 1
		
	print "OK============================================================================="
	# while True:
	# 	print Log
	# 	time.sleep(1)

th = Thread(target=LogBehavior)
th.start()