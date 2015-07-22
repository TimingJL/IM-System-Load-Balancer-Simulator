#!/usr/bin/env python
#coding=utf-8
import parameter
import random
import redis

number_of_CMs = parameter.number_of_CMs
redisDB = redis.StrictRedis(host='localhost', port=6379, db=0)

#Random
def myRandom():
	return random.randint(1,parameter.number_of_CMs)#a <= N <= b

#Round-Robin
def myRR():
	CM_ID = int(redisDB.get("RRcount"))
	CM_ID = (CM_ID % parameter.number_of_CMs)+1
	redisDB.set("RRcount",CM_ID)
	return CM_ID

#Lest Message Queue(Join the fewest number of job)
def myLMQ():
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