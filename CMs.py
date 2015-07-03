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
import csv 
import datetime
from time import strftime
from threading import Thread

redisDB = redis.StrictRedis(host='localhost', port=6379, db=0)
redisMQ = redis.Redis()

number_of_CMs = parameter.number_of_CMs
print "number_of_CMs: %r" %(number_of_CMs)

#connection_manager define
class Connection_Manager(threading.Thread):
	def __init__(self, CM_ID):
		threading.Thread.__init__(self)
		self.CM_ID = CM_ID

	#connection_manager thread start
	def run(self):
		print self.CM_ID
		# while True:#receive message from LB
		# 	self.redisMQ.blpop()
		# 	pass


#create CMs
CMList = []
for i in range(1, number_of_CMs+1):
	CMList.append(Connection_Manager(i))# ID = 1 ~

#user start
for i in CMList:
	i.start()