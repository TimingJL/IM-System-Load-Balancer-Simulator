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
LogBehavior = '1'
f = open('./log/' + SimulateStart + '-LogBehavior_'+ LogBehavior +'.txt', 'a+')
does_use_random_seed = False
Log = []


#user define
class User(threading.Thread):
	#generate pareto network delay 		
	def __init__(self, user_ID):
		threading.Thread.__init__(self)
		self.user_ID = user_ID	
		self.redisMQ = redis.Redis()
		self.is_online = False
		self.is_regist = False
		redisDB.set("is_regist["+str(self.user_ID)+']', False)
		request = {"user_ID":self.user_ID, "is_online":self.is_online}
		self.redisMQ.rpush('is_online['+str(self.user_ID)+']', str(request))#tell LB to regist/erase
		###################################
		LogTemp = {"type":0,"content":request}
		Log.append(LogTemp)
		f.write(str(LogTemp) + "\n")
		###################################		
		print "init is regist: %r" %(redisDB.get("is_regist["+str(self.user_ID)+']'))

		###########netowrk delay#################
		temp = (-math.log(1.0 - random.random()) / (1/0.6))
		#temp = np.random.pareto(1.1, 1) + 0. #(init) x = np.random.pareto(shape, size) + lower
		self.network_delay = temp
		#self.network_delay = (-math.log(1.0 - random.random()) / 1.1)

	#Generate Random Timings for a Poisson Process
	#http://preshing.com/20111007/how-to-generate-random-timings-for-a-poisson-process/
	def nextTime(self, rateParameter):
		return -math.log(1.0 - random.random()) / rateParameter

	#turn on and off is_online by random poisson
	def clock(self):
		time.sleep(self.user_ID)
		while True:
			self.is_online = not self.is_online
			request = {"user_ID":self.user_ID, "is_online":self.is_online}
			self.redisMQ.rpush('is_online['+str(self.user_ID)+']', str(request))#tell LB to regist/erase
			print "send regist, is user online: %r" %(self.is_online)
			###################################
			LogTemp = {"type":0,"content":request}
			Log.append(LogTemp)
			f.write(str(LogTemp) + "\n")
			###################################
			if(self.is_online):
				#when online, generate sudo network delay
				tmp = (-math.log(1.0 - random.random()) / (1/0.6))
				#tmp = np.random.pareto(1.1, 1) + 0. #(reset) x = np.random.pareto(shape, size) + lower
				self.network_delay = tmp
				redisDB.set("user["+str(self.user_ID)+"]_last_out_Queue_time",0)#in order to calculate Tau(init when user online)


			redisDB.set("user["+str(self.user_ID)+"]_network_delay",self.network_delay)

			#next online/leave time
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

			if((self.is_online == True) and (self.is_regist == 'True')):
				print "yes"
				#random generate a receiver
				#real random
				receiver_ID_tuple = random.randint(1,number_of_users),#a <= N <= b', tuple(1,)
				receiver_ID = receiver_ID_tuple[0]

				message = {"test_time_stamp":SimulateStart, 
							"sender_ID":self.user_ID,
							"receiver_ID":receiver_ID,#a <= N <= b
							"message_birth_time":time.time(),
							#"network_delay":delayTime,
							"is_VIP":False,
							}

				# send message to LB								
				#self.redisMQ.rpush("senderID[" + str(self.user_ID) + "]", message)

				# send message to CM(not through LB)
				CM_ID = redisDB.get("User["+str(receiver_ID)+"]_in_which_CM")
				self.redisMQ.rpush("receiverCM_ID[" + str(CM_ID) +"]", message)#send message to the CM(to recognize the receiver, not sender)
				#print message				
				###################################
				LogTemp = {"type":1,"content":message}
				Log.append(LogTemp)
				f.write(str(LogTemp) + "\n")
				###################################
				#next sending time
				t = self.nextTime(1.0/30.)
				time.sleep(t)				

#create users
userList = []
for i in range(1, number_of_users+1):
	userList.append(User(i))# ID = 1 ~

#user start
for i in userList:
	i.start()
	#time.sleep(3)

def LogBehavior():
	
	f = open('./log/' + SimulateStart + '-LogBehavior_'+ LogBehavior +'.txt', 'w+')
	time.sleep(60*60)
	i = 1
	for item in Log:
		# redisDB.set("LogBehavior[" + str(i) + "][" +LogBehavior+ "]",item)
		# redisDB.set("LogItemNumber[" +LogBehavior+ "]",i)
		f.write(str(item) + "\n")
		i = i + 1
		
	print "OK============================================================================="
	f.close()
	# while True:
	# 	print Log
	# 	time.sleep(1)

th = Thread(target=LogBehavior)
th.start()