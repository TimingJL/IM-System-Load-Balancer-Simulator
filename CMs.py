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
import random
from time import strftime
from threading import Thread
from collections import deque

redisDB = redis.StrictRedis(host='localhost', port=6379, db=0)
redisMQ = redis.Redis()

number_of_CMs = parameter.number_of_CMs

print "number_of_CMs: %r" %(number_of_CMs)

redisDB.set("CM_clock",0)


def myAutoCorrelationDistribution(p, first_network_delay):
	if p == 0.:
		p = 0.00000001
	x_value = random.random()
	y_value = 0
	a = 1-(1/((p**5)/(p**4)))
	b = 1-(1/((1**5)/(p**4)))
	if x_value <= p:
		# 0 < x_value <= p
		y_value = x_value**5/p**4
	else:
		# p < x_value <= 1
		y_value_raw = 1-(1/((x_value**5)/(p**4)))
		y_value = ((y_value_raw - a)/(b-a)) * (1-p) + p
	second_network_delay = np.random.pareto(1.1, 1) + 0.
	delayTime = (y_value * first_network_delay) + (1.-y_value)*(second_network_delay[0])

	print "y_value: %r" %(y_value)
	print "delayTime: %r" %(delayTime)


	return delayTime

def Finished():
	item = 1
	while True:
		val = redisMQ.blpop("Finish_List[1]")
		redisDB.set("Finish_List[5.1][" + str(item) + "]",val[1])
		redisDB.set("Finish_List[5.1]_job_number",item)
		item = item + 1

def clock():
	while True:
		time.sleep(1)
		count = int(redisDB.get("CM_clock"))
		count = count + 1
		redisDB.set("CM_clock",count)

def MG1_sliding_response_time():
	window_size = parameter.window_size

	while True:
		if int(redisDB.get("CM_clock")) > window_size:
			time.sleep(1)
			for i in range(1, number_of_CMs+1):
				queueLength = int(redisDB.get("CM[" + str(i) + "]QueueLength"))
				if queueLength == 0:
					redisDB.set("CM[" + str(i) + "]MG1_temp_response_time", 0)				
				else:
					arrival_num = 0.
					service_num = 0.
					total_service_time = 0.
					total_service_time_power2 = 0.
					income_list = eval(redisDB.get("CM[" + str(i) + "]income_list"))
					outcome_list = eval(redisDB.get("CM[" + str(i) + "]outcome_list"))				
					service_time_list = eval(redisDB.get("CM[" + str(i) + "]service_time_list"))
					service_time_power2_list = eval(redisDB.get("CM[" + str(i) + "]service_time_power2_list"))

					for j in range(-1, -1*(window_size+1),-1):
						arrival_num = arrival_num + income_list[j]
						service_num = service_num + outcome_list[j]
						total_service_time = total_service_time + service_time_list[j]
						total_service_time_power2 = total_service_time_power2 + service_time_power2_list[j]

					Lambda = arrival_num/window_size
					Mu = service_num/window_size
					if Mu == 0.:
						Mu = 0.000001				
					rho = Lambda/Mu
					if rho == 1:
						rho = 1. - 0.000001
					if total_service_time == 0.:
						total_service_time = 0.000001
						
					E_Se = 0.5*(total_service_time_power2/total_service_time)
					E_TQ = (rho/(1-rho))*(E_Se)


					if rho > 1.:
						temp = (90000+E_Se)
						redisDB.set("CM[" + str(i) + "]MG1_temp_response_time", temp)
					elif rho == 0.:
						if Mu == 0.000001:
							if queueLength == 0.:
								redisDB.set("CM[" + str(i) + "]MG1_temp_response_time", 0)
							else:
								temp = (90000+E_Se)
								redisDB.set("CM[" + str(i) + "]MG1_temp_response_time", temp)
						else:
							temp = (0.000001*E_Se)
							redisDB.set("CM[" + str(i) + "]MG1_temp_response_time", temp)
					else:# 0<rho<=1
						redisDB.set("CM[" + str(i) + "]MG1_temp_response_time", E_TQ)

def MM1_sliding_response_time():
	window_size = parameter.window_size

	while True:
		if int(redisDB.get("CM_clock")) > window_size:
			time.sleep(1)
			for i in range(1, number_of_CMs+1):
				queueLength = int(redisDB.get("CM[" + str(i) + "]QueueLength"))
				if queueLength == 0:
					redisDB.set("CM[" + str(i) + "]temp_response_time", 0)
				else:
					rho = ((-1*queueLength) + ((queueLength**2) + 4*queueLength)**0.5)/2
					arrival_num = 0.
					service_num = 0.
					income_list = eval(redisDB.get("CM[" + str(i) + "]income_list"))
					outcome_list = eval(redisDB.get("CM[" + str(i) + "]outcome_list"))
					for j in range(-1, -1*(window_size+1),-1):
						arrival_num = arrival_num + income_list[j]
						service_num = service_num + outcome_list[j]
					Lambda = arrival_num/window_size
					#Mu = service_num/window_size
					Mu = Lambda/rho

					if Lambda == 0:
						if Mu == 0:
							temp = (90000+queueLength)
							redisDB.set("CM[" + str(i) + "]temp_response_time", temp)
						else:
							temp = (1./Mu)
							redisDB.set("CM[" + str(i) + "]temp_response_time", temp)
					else:
						if Lambda > Mu:
							temp = (90000+queueLength)
							redisDB.set("CM[" + str(i) + "]temp_response_time", temp)
						else:
							temp = (1./(Mu-Lambda))
							redisDB.set("CM[" + str(i) + "]temp_response_time", temp)


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
		self.income_list = []
		self.outcome_list = []
		self.service_time_list = []
		self.service_time_power2_list = []
		self.service_time = 0.
		self.service_time_power2 = 0.
		self.temp_income_job = 0.
		self.temp_outcome_job = 0.
		self.temp_response_time = 0.
		redisDB.set("CM[" + str(self.CM_ID) + "]temp_response_time", 0)
		redisDB.set("CM[" + str(self.CM_ID) + "]MG1_temp_response_time", 0)

	def time_window(self):
		while True:
			time.sleep(60)
			self.window = self.window + 1

	def sliding_window(self):#to record some parameter per second
		while True:
			time.sleep(1)
			self.income_list.append(self.temp_income_job)
			self.outcome_list.append(self.temp_outcome_job)
			self.service_time_list.append(self.service_time)
			self.service_time_power2_list.append(self.service_time_power2)
			redisDB.set("CM[" + str(self.CM_ID) + "]income_list",self.income_list)
			redisDB.set("CM[" + str(self.CM_ID) + "]outcome_list",self.outcome_list)
			redisDB.set("CM[" + str(self.CM_ID) + "]service_time_list",self.service_time_list)
			redisDB.set("CM[" + str(self.CM_ID) + "]service_time_power2_list",self.service_time_power2_list)			
			self.temp_income_job = 0.
			self.temp_outcome_job = 0.
			self.service_time = 0.
			self.service_time_power2 = 0.

	def Serving(self):
		while True:
			if self.message_queue:
				message = ast.literal_eval(self.message_queue.popleft())

				print "U[%r] to r[%r] via CM[%r]" %(message["sender_ID"],message["receiver_ID"],self.CM_ID)

				out_Queue_time = time.time()
				redisDB.set("CM[" + str(self.CM_ID) + "]QueueLength",len(self.message_queue))

				#calculate Tau (set default in Client.py)
				last_out_Queue_time = redisDB.get("user["+str(message["sender_ID"])+"]_last_out_Queue_time")
				if float(last_out_Queue_time) == 0.:#to init when user online, set Tau = 0
					message.setdefault("Tau",0)
				else:#calculate Tau,starting from receiving second message
					Tau = float(out_Queue_time) - float(last_out_Queue_time)
					message.setdefault("Tau",Tau)
				redisDB.set("user["+str(message["sender_ID"])+"]_last_out_Queue_time",out_Queue_time)

				#decide network delay by using message interval time,Tau
				last_network_delay = redisDB.get("user["+str(message["receiver_ID"])+"]_network_delay")
				Tau = float(message["Tau"])
				most_possible_network_delay = math.exp(-1*Tau)
				delayTime = myAutoCorrelationDistribution(most_possible_network_delay, float(last_network_delay))
			
				#delayTime must > 0
				if (delayTime < 0.):
					delayTime = 0.0001
				#TCP session timeout = 20: https://nkongkimo.wordpress.com/2010/09/
				if (delayTime > 20.):
					delayTime = 20.

				message["network_delay"] = delayTime
				redisDB.set("user["+str(message["receiver_ID"])+"]_network_delay",delayTime)
				#Serving + network delay============================================================
				time.sleep(float(message["network_delay"]))
				#===================================================================================
				out_Server_time = time.time()
				self.temp_outcome_job = self.temp_outcome_job + 1
				self.service_time = self.service_time + delayTime
				self.service_time_power2 = self.service_time_power2 + (delayTime**2)

				queueing_time = out_Queue_time - float(message["in_Queue_time"])
				response_time = out_Server_time - float(message["in_Queue_time"])

				message.setdefault("out_Queue_time",out_Queue_time)
				message.setdefault("out_Server_time",out_Server_time)
				message.setdefault("queueing_time",queueing_time)
				message.setdefault("response_time",response_time)
				message.setdefault("window",self.window)
				print self.window

				redisMQ.rpush("Finish_List[1]", message)

	#connection_manager thread start
	def run(self):
		# print self.CM_ID
		th = Thread(target=self.Serving)
		th.start()
		th2 = Thread(target=self.time_window)
		th2.start()
		th3 = Thread(target=self.sliding_window)
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


th4 = Thread(target=Finished)
th4.start()

th5 = Thread(target=clock)
th5.start()

th6 = Thread(target=MM1_sliding_response_time)
th6.start()

th7 = Thread(target=MG1_sliding_response_time)
th7.start()