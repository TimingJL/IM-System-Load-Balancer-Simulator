#coding=utf-8
import time
import math
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
for i in range(1,number_of_users+1):
	redisDB.set("is_regist["+str(i)+"]",True)

#LB define
class LoadBalancer(threading.Thread):
	is_user_online = False	
	def __init__(self, user_ID):
		threading.Thread.__init__(self)
		self.user_ID = user_ID
		self.redisMQ = redis.Redis()
		self.in_which_CM = 0
		self.is_user_online = False

	def SendMessage(self):
		#redisDB.set("is_regist["+str(self.user_ID)+"]",True)#tell user(Client.py) regist sucess
		#print "self.in_which_CM:%r" %(self.in_which_CM)
		while(self.in_which_CM > 0):			
			messageTuple = self.redisMQ.blpop("senderID[" + str(self.user_ID) + "]")#receive message from Client
			#print messageTuple
			message = ast.literal_eval(messageTuple[1])
			#print "U[%r] in CM[%r], send to [%r] in [%r]" %(self.user_ID, self.in_which_CM,message["receiverCM_ID"],LBList[message["receiverCM_ID"]-1].get_In_Which_CM())
			d = message['receiver_ID']
			r_ID = LBList[message['receiver_ID']-1].get_In_Which_CM()
			print "U[%r] in CM[%r], send to [%i]in CM[%r]" %(self.user_ID, self.in_which_CM, d,r_ID)
			#message = ast.literal_eval(tempMsg[1])
			# print message
			# print type(message)
			#print message['receiver_ID']
			# print LBList[message['receiver_ID']-1].get_In_Which_CM()
			print "\n"

			self.redisMQ.rpush("receiverCM_ID[" + str(r_ID) +"]", message)#send message to the CM(to recognize the receiver, not sender)	

	def get_In_Which_CM(self):
		return self.in_which_CM

	def LogIn(self):
		#=========Scheduling Policy Choose===========#
		#CM_ID = SchedulerPolicy.myRandom()
		#CM_ID = SchedulerPolicy.myRR()
		#CM_ID = SchedulerPolicy.myLMQ()
		CM_ID = SchedulerPolicy.myMM1()
		#============================================#
		self.in_which_CM = CM_ID
		#print 'user_%r to CM_%r' %(self.user_ID, self.in_which_CM)
		print "U[%r] in CM[%r]===============" %(self.user_ID, self.in_which_CM)
		th = Thread(target=self.SendMessage)
		th.start()

	def LogOut(self):
		#redisDB.set("is_regist["+str(self.user_ID)+"]",False)#tell user(Client.py) erase sucess
		#self.in_which_CM = 0
		print '%r LogOut' %(self.user_ID)
		time.sleep(1)

	#LB thread start
	def run(self):
		#Listen to the login or logout request from the user
		#If the LB receive login/logout request, than regist/erase
		while True:
			val = self.redisMQ.blpop('is_online['+str(self.user_ID)+']')#type(val) = tuple; val = (user_ID, is_online)
			request = ast.literal_eval(val[1])#translate tuple to dict
			self.is_user_online = request["is_online"]#request on_line or not
			print self.is_user_online
			if self.is_user_online:
				#if user online is true, login and give it a CM address
				#self.redisMQ.rpush("LogIn",self.user_ID)
				self.LogIn()
			else:
				#if user online is false, Log Out from CM list
				#self.redisMQ.rpush("LogOut",self.user_ID)
				self.LogOut()
				pass

			redisDB.set("is_regist["+str(self.user_ID)+"]",self.is_user_online)#tell user(Client.py) regist/erase sucess

#create LB
LBList = []
for i in range(1, number_of_users+1):
	LBList.append(LoadBalancer(i))# ID = 1 ~

#user start
for i in LBList:
	i.start()

print 'Waiting...'
#print LBList
#print "R_CM: %r" %(LBList[1].get_In_Which_CM())