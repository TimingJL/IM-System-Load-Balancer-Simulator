#coding=utf-8
import time
import math
import parameter
import numpy as np
import redis
import thread
import threading
import ast
import csv 
import datetime
from time import strftime
from threading import Thread
from collections import deque

redisDB = redis.StrictRedis(host='localhost', port=6379, db=0)
redisMQ = redis.Redis()

number_of_CMs = parameter.number_of_CMs

print "number_of_CMs: %r" %(number_of_CMs)


def Finished():
	item = 1
	while True:
		val = redisMQ.blpop("Finish_List[1]")
		redisDB.set("Finish_List[5.1][" + str(item) + "]",val[1])
		redisDB.set("Finish_List[5.1]_job_number",item)
		item = item + 1


#connection_manager define
class Connection_Manager(threading.Thread):	
	def __init__(self, CM_ID):
		threading.Thread.__init__(self)
		self.CM_ID = CM_ID
		self.redisMQ = redis.Redis()
		self.message_queue = deque([])
		self.window = 1
		#self.delta_List = deque(np.random.normal(0, 0.1, 5000))
		#self.queue_length = 0

		self.temp_income_job = 0.
		self.temp_outcome_job = 0.
		self.temp_response_time = 0.
		redisDB.set("CM[" + str(self.CM_ID) + "]temp_response_time", 0)

	def time_window(self):
		while True:
			time.sleep(60)
			self.window = self.window + 1

	def predict_response_time_window(self):
		while True:
			window_size = 5
			time.sleep(window_size)
			tmp = self.temp_outcome_job - self.temp_income_job
			if tmp == 0.:
				tmp = 0.0001
			self.temp_response_time = window_size/tmp
			redisDB.set("CM[" + str(self.CM_ID) + "]temp_response_time",self.temp_response_time)
			self.temp_income_job = 0.
			self.temp_outcome_job = 0.

	def Serving(self):
		while True:
			if self.message_queue:
				message = ast.literal_eval(self.message_queue.popleft())

				print "U[%r] to r[%r] via CM[%r]" %(message["sender_ID"],message["receiver_ID"],self.CM_ID)
				#print "delay: %r\n" %(message["network_delay"])

				out_Queue_time = time.time()
				redisDB.set("CM[" + str(self.CM_ID) + "]QueueLength",len(self.message_queue))

				last_network_delay = redisDB.get("user["+str(message["receiver_ID"])+"]_network_delay")
				deltaTemp = np.random.normal(0, float(last_network_delay)**0.5, 1)#np.random.normal(mu, sigma, 1000)
				delta = deltaTemp[0]
				#delta = self.delta.popleft()
				#print type(network_delay)
				#print type(delta)
				delayTime = float(last_network_delay)+float(delta)
			
				#delayTime must > 0
				if (delayTime < 0.):
					delayTime = 0.0001
				#TCP session timeout = 1800: https://nkongkimo.wordpress.com/2010/09/
				if (delayTime > 20.):
					delayTime = 20.

				print "delta: %r" %(delta)
				print "delay: %r" %(delayTime)
				print '**'	

				message["network_delay"] = delayTime
				redisDB.set("user["+str(message["receiver_ID"])+"]_network_delay",delayTime)
				#Serving + network delay============================================================
				time.sleep(float(message["network_delay"]))
				#===================================================================================
				out_Server_time = time.time()
				self.temp_outcome_job = self.temp_outcome_job + 1

				queueing_time = out_Queue_time - float(message["in_Queue_time"])
				response_time = out_Server_time - float(message["in_Queue_time"])

				message.setdefault("out_Queue_time",out_Queue_time)
				message.setdefault("out_Server_time",out_Server_time)
				message.setdefault("queueing_time",queueing_time)
				message.setdefault("response_time",response_time)
				message.setdefault("window",self.window)
				print self.window
				# print "hi"
				# print "sender: %r" %(message["sender_ID"])
				# print "\n"
				#than save, but how???
				#Finish_List.append(message)
				redisMQ.rpush("Finish_List[1]", message)

	#connection_manager thread start
	def run(self):
		# print self.CM_ID
		th = Thread(target=self.Serving)
		th.start()
		th2 = Thread(target=self.time_window)
		th2.start()
		th3 = Thread(target=self.predict_response_time_window)
		th3.start()
		while True:#receive message from LB
			messageTuple = self.redisMQ.blpop("receiverCM_ID[" + str(self.CM_ID) +"]")
			#print messageTuple
			# tempMsg = ast.literal_eval(messageTuple[1])
			message = ast.literal_eval(messageTuple[1])

			message.setdefault("serviceby", self.CM_ID)
			in_Queue_time = time.time()
			message.setdefault("in_Queue_time", in_Queue_time)
			#in Queue===========================================================================
			self.message_queue.append(str(message))
			#===================================================================================
			self.temp_income_job = self.temp_income_job + 1


			redisDB.set("CM[" + str(self.CM_ID) + "]QueueLength",len(self.message_queue))




#create CMs
CMList = []
for i in range(1, number_of_CMs+1):
	CMList.append(Connection_Manager(i))# ID = 1 ~
	redisDB.set("CM[" + str(i) + "]QueueLength",0)

#user start
for i in CMList:
	i.start()

# th2 = Thread(target=time_window)
# th2.start()
th3 = Thread(target=Finished)
th3.start()