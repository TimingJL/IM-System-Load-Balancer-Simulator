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

a = int(redisDB.get("Finish_List[4]_job_number"))
b = redisDB.get("Finish_List[4][" + str(a) + "]")
print "a: %r"%(a)
j1 = 0
r_RR = 0.
for i in range(1,a):# from 1 to n
	tmp = redisDB.get("Finish_List[4][" + str(i) + "]")
	tmp_dict = ast.literal_eval(tmp)
	if int(tmp_dict["window"]) < 21:
		j1 = j1 + 1
		r_RR = r_RR + float(tmp_dict["response_time"])

#===================================================================

c = int(redisDB.get("Finish_List[5.1]_job_number"))
print "c: %r"%(c)
j2 = 0
r_Random = 0.
for i in range(1,c):# from 1 to n
	tmp = redisDB.get("Finish_List[5.1][" + str(i) + "]")
	tmp_dict = ast.literal_eval(tmp)
	if int(tmp_dict["window"]) < 21:
		j2 = j2 + 1
		r_Random = r_Random + float(tmp_dict["response_time"])

#===================================================================

d = int(redisDB.get("Finish_List[6]_job_number"))
print "d: %r"%(d)
j3 = 0
r_LMQ = 0.
for i in range(1,d):# from 1 to n
	tmp = redisDB.get("Finish_List[6][" + str(i) + "]")
	tmp_dict = ast.literal_eval(tmp)
	if int(tmp_dict["window"]) < 21:
		j3 = j3 + 1
		r_LMQ = r_LMQ + float(tmp_dict["response_time"])


print "Round-Robin: %r" %(r_RR/j1)
print "Random: %r" %(r_Random/j2)
print "LMQ: %r" %(r_LMQ/j3)