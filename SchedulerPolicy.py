#!/usr/bin/env python
#coding=utf-8
import parameter
import random
import redis

number_of_CMs = parameter.number_of_CMs
redisDB = redis.StrictRedis(host='localhost', port=6379, db=0)

#Random
def myRandom():
	print "====Random===="
	return random.randint(1,parameter.number_of_CMs)#a <= N <= b

#Round-Robin
def myRR():
	print "====Round-Robin===="
	CM_ID = int(redisDB.get("RRcount"))
	CM_ID = (CM_ID % parameter.number_of_CMs)+1
	redisDB.set("RRcount",CM_ID)
	return CM_ID

#Lest Message Queue(Join the fewest number of job)
def mySQF():
	print "====SQF===="
	queueLength = []
	queueLength.append(0)#ignore [0], begining from [1]
	for i in range(1, number_of_CMs+1):
		queueLength.append(int(redisDB.get("CM[" + str(i) + "]QueueLength")))

	shortestQ = queueLength[1]#length
	shortestQ_ID = 1
	for i in range(1, number_of_CMs+1):
		if queueLength[i] < shortestQ:
			shortestQ_ID = i
			shortestQ = queueLength[i]
	return shortestQ_ID

#M/M/1, lest predict response time first
def myMM1():
	print "====M/M/1===="
	window_size = parameter.window_size
	#redisDB.set("window_size", window_size)
	if int(redisDB.get("CM_clock")) < (window_size + 10): # warm up
		CM_ID = myRR()
		return CM_ID
	else:
		response_time = []
		response_time.append(0)#ignore [0], begining from [1]
		for i in range(1, number_of_CMs+1):
			response_time.append(float(redisDB.get("CM[" + str(i) + "]temp_response_time")))

		shortestR = response_time[1]#length
		shortestR_ID = 1
		for i in range(1, number_of_CMs+1):
			if response_time[i] < shortestR:
				shortestR_ID = i
				shortestR = response_time[i]
		return shortestR_ID

#M/G/1, lest predict response time first
def myMG1():
	print "====M/G/1===="
	window_size = parameter.window_size
	#redisDB.set("window_size", window_size)
	if int(redisDB.get("CM_clock")) < (window_size + 10): # warm up
		CM_ID = myRR()
		return CM_ID
	else:
		response_time = []
		response_time.append(0)#ignore [0], begining from [1]
		for i in range(1, number_of_CMs+1):
			response_time.append(float(redisDB.get("CM[" + str(i) + "]MG1_temp_response_time")))

		shortestR = response_time[1]#length
		shortestR_ID = 1
		for i in range(1, number_of_CMs+1):
			if response_time[i] < shortestR:
				shortestR_ID = i
				shortestR = response_time[i]
		return shortestR_ID


#M/G/1 merge RR2
def myMG1_RR():
	print "====M/G/1 merge RR===="
	window_size = parameter.window_size
	choosenQueue = []
	#redisDB.set("window_size", window_size)
	if int(redisDB.get("CM_clock")) < (window_size + 10): # warm up
		CM_ID = myRR()
		return CM_ID
	else:
		response_time = []
		response_time.append(0)#ignore [0], begining from [1]
		for i in range(1, number_of_CMs+1):
			response_time.append(float(redisDB.get("CM[" + str(i) + "]MG1_temp_response_time")))

		#First Queue
		shortestR = response_time[1]#length
		shortestR_ID = 1
		for i in range(1, number_of_CMs+1):
			if response_time[i] < shortestR:
				shortestR_ID = i
				shortestR = response_time[i]

		choosenQueue.append(shortestR_ID)
		response_time[shortestR_ID] = 99999

		#Second Queue
		shortestR = response_time[1]#length
		shortestR_ID = 1
		for i in range(1, number_of_CMs+1):
			if response_time[i] < shortestR:
				shortestR_ID = i
				shortestR = response_time[i]

		choosenQueue.append(shortestR_ID)

		flag = int(redisDB.get("MG1_RRcount"))
		if flag == 0:
			redisDB.set("MG1_RRcount",1)
			return choosenQueue[0]
		elif flag == 1:
			redisDB.set("MG1_RRcount",0)
			return choosenQueue[1]

#M/G/1 merge RR
def myMG1_RR3():
	print "====M/G/1 merge RR 3===="
	window_size = parameter.window_size
	choosenQueue = []
	#redisDB.set("window_size", window_size)
	if int(redisDB.get("CM_clock")) < (window_size + 10): # warm up
		CM_ID = myRR()
		return CM_ID
	else:
		response_time = []
		response_time.append(0)#ignore [0], begining from [1]
		for i in range(1, number_of_CMs+1):
			response_time.append(float(redisDB.get("CM[" + str(i) + "]MG1_temp_response_time")))

		#First Queue=====================================
		shortestR = response_time[1]#length
		shortestR_ID = 1
		for i in range(1, number_of_CMs+1):
			if response_time[i] < shortestR:
				shortestR_ID = i
				shortestR = response_time[i]

		choosenQueue.append(shortestR_ID)
		response_time[shortestR_ID] = 99999

		#Second Queue====================================
		shortestR = response_time[1]#length
		shortestR_ID = 1
		for i in range(1, number_of_CMs+1):
			if response_time[i] < shortestR:
				shortestR_ID = i
				shortestR = response_time[i]

		choosenQueue.append(shortestR_ID)
		response_time[shortestR_ID] = 99999

		#Third Queue======================================
		shortestR = response_time[1]#length
		shortestR_ID = 1
		for i in range(1, number_of_CMs+1):
			if response_time[i] < shortestR:
				shortestR_ID = i
				shortestR = response_time[i]

		choosenQueue.append(shortestR_ID)

		# flag = int(redisDB.get("MG1_RRcount"))
		# if flag == 0:
		# 	redisDB.set("MG1_RRcount",1)
		# 	return choosenQueue[0]
		# elif flag == 1:
		# 	redisDB.set("MG1_RRcount",0)
		# 	return choosenQueue[1]

		CM_ID = int(redisDB.get("MG1_RRcount"))
		CM_ID = (CM_ID % 4)
		redisDB.set("MG1_RRcount",CM_ID+1)
		print "RR[%r]--CM[%r]" %(CM_ID, choosenQueue[CM_ID])
		return choosenQueue[CM_ID]

#M/M/1 merge number of user, lest predict response time first
def myMM1_merge_LNU():
	print "==== MM1_merge_LNU ===="
	window_size = parameter.window_size
	#redisDB.set("window_size", window_size)
	if int(redisDB.get("CM_clock")) < (window_size + 10): # warm up
		CM_ID = mySQF_merge_LNU()
		return CM_ID
	else:
		response_time = []
		response_time.append(0)#ignore [0], begining from [1]
		for i in range(1, number_of_CMs+1):
			response_time.append(float(redisDB.get("CM[" + str(i) + "]_pseudo_length_response_time")))

		shortestR = response_time[1]#length
		shortestR_ID = 1
		for i in range(1, number_of_CMs+1):
			if response_time[i] < shortestR:
				shortestR_ID = i
				shortestR = response_time[i]
		return shortestR_ID

#Lest Number of User's Queue
def myLNU():
	print "==== LNU ===="
	queueLength = []
	queueLength.append(0)#ignore [0], begining from [1]
	for i in range(1, number_of_CMs+1):
		user_list = eval(redisDB.get("CM["+str(i)+"]_user_list"))
		queueLength.append(len(user_list))

	shortestQ = queueLength[1]#length
	shortestQ_ID = 1
	for i in range(1, number_of_CMs+1):
		if queueLength[i] < shortestQ:
			shortestQ_ID = i
			shortestQ = queueLength[i]
	return shortestQ_ID

#Lest Message Queue(Join the fewest number of job)
def mySQF_merge_LNU():
	print "==== SQF merge LNU ===="
	queueLength = []
	queueLength.append(0)#ignore [0], begining from [1]
	for i in range(1, number_of_CMs+1):
		user_list = eval(redisDB.get("CM["+str(i)+"]_user_list"))
		queueLength.append(int(redisDB.get("CM[" + str(i) + "]QueueLength")) + len(user_list))

	shortestQ = queueLength[1]#length
	shortestQ_ID = 1
	for i in range(1, number_of_CMs+1):
		if queueLength[i] < shortestQ:
			shortestQ_ID = i
			shortestQ = queueLength[i]
	return shortestQ_ID

