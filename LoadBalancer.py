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
redisDB.set("MG1_RRcount",0)
number_of_users = parameter.number_of_users
for i in range(1,number_of_users+1):
	redisDB.set("is_regist["+str(i)+"]",True)

for i in range(0,parameter.number_of_CMs+1):
	user_list_of_CM = []
	redisDB.set("CM["+str(i)+"]_user_list",user_list_of_CM)

#LB define
class LoadBalancer(threading.Thread):
	def __init__(self, user_ID):
		threading.Thread.__init__(self)
		self.user_ID = user_ID
		self.redisMQ = redis.Redis()
		self.in_which_CM = 0
		self.is_user_online = False

	def SendMessage(self):
		#redisDB.set("is_regist["+str(self.user_ID)+"]",True)#tell user(Client.py) regist sucess
		#print "self.in_which_CM:%r" %(self.in_which_CM)
		
		while(True):
			if(self.in_which_CM > 0):
				messageTuple = self.redisMQ.blpop("senderID[" + str(self.user_ID) + "]")#receive message from Client
				#print messageTuple
				message = ast.literal_eval(messageTuple[1])

				d = message['receiver_ID']
				#print "d = %r" %(d)
				r_ID = LBList[message['receiver_ID']-1].get_In_Which_CM()
				print "U[%r] in CM[%r], send to [%i]in CM[%r]" %(self.user_ID, self.in_which_CM, d,r_ID)

				print "\n"

				#self.redisMQ.rpush("receiverCM_ID[" + str(r_ID) +"]", message)#send message to the CM(to recognize the receiver, not sender)	

	def get_In_Which_CM(self):
		return self.in_which_CM

	def LogIn(self):
		#=========Scheduling Policy Choose===========#
		#CM_ID = SchedulerPolicy.myRandom()
		CM_ID = SchedulerPolicy.myRR()
		#CM_ID = SchedulerPolicy.mySQF()
		#CM_ID = SchedulerPolicy.myMM1()
		#CM_ID = SchedulerPolicy.myMG1()
		#CM_ID = SchedulerPolicy.myMG1_RR()
		#CM_ID = SchedulerPolicy.myMG1_RR3()
		#CM_ID = SchedulerPolicy.myLNU()
		#CM_ID = SchedulerPolicy.myMM1_merge_LNU()
		#CM_ID = SchedulerPolicy.mySQF_merge_LNU()
		#============================================#
		user_list = eval(redisDB.get("CM["+str(CM_ID)+"]_user_list"))
		if self.user_ID in user_list:
			pass
		else:
			user_list.append(self.user_ID)
			redisDB.set("CM["+str(CM_ID)+"]_user_list",user_list)

		self.in_which_CM = CM_ID
		redisDB.set("is_regist["+str(self.user_ID)+"]",True)#tell user(Client.py) is regist
		redisDB.set("User["+str(self.user_ID)+"]_in_which_CM",CM_ID)#tell user(Client.py) in which CM
		print "Login  U[%r] in CM[%r]===============" %(self.user_ID, self.in_which_CM)


	def LogOut(self):
		redisDB.set("is_regist["+str(self.user_ID)+"]",False)#tell user(Client.py) erase sucess
		# print "out"
		# print redisDB.get("CM["+str(self.in_which_CM)+"]_user_list")
		# print type(redisDB.get("CM["+str(self.in_which_CM)+"]_user_list"))
		user_list = eval(redisDB.get("CM["+str(self.in_which_CM)+"]_user_list"))
		if self.user_ID in user_list:
			user_list.remove(self.user_ID)
			redisDB.set("CM["+str(self.in_which_CM)+"]_user_list",user_list)

		#self.in_which_CM = 0
		print '%r LogOut' %(self.user_ID)
		time.sleep(1)

	#LB thread start
	def run(self):
		#Listen to the login or logout request from the user
		#If the LB receive login/logout request, than regist/erase
		# th = Thread(target=self.SendMessage)
		# th.start()
		#print '%r run' %(self.user_ID)
		while True:
			#print 'in'			
			val = self.redisMQ.blpop('is_online['+str(self.user_ID)+']')#type(val) = tuple; val = (user_ID, is_online)
			request = ast.literal_eval(val[1])#translate tuple to dict
			#print 'get'
			self.is_user_online = request["is_online"]#request on_line or not
			#print "User[%r] is_online: %r" %(self.user_ID, self.is_user_online)
			if self.is_user_online == True:
				#if user online is true, login and give it a CM address
				#self.redisMQ.rpush("LogIn",self.user_ID)
				self.LogIn()
			else:
				#if user online is false, Log Out from CM list
				#self.redisMQ.rpush("LogOut",self.user_ID)
				self.LogOut()
				pass

			#redisDB.set("is_regist["+str(self.user_ID)+"]",self.is_user_online)#tell user(Client.py) regist/erase sucess

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