#!/usr/bin/env python
#coding=utf-8
import parameter
import random
import redis

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
	pass

