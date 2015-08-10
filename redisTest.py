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
redisDB2 = redis.StrictRedis(host='localhost', port=6379, db=0)


redisDB.set("foo",01)
redisDB2.set("foo",02)

# print "redisDB: %r" %(redisDB.get("foo"))
# print "redisDB2: %r" %(redisDB2.get("foo"))

# f = open('./log/test.txt', 'w+') 
# for i in range(1,10):
# 	f.write("Str" + str(i) + "\n")

f = open('./log/20150809-081535 am-LogBehavior_1.txt','r') 
#print f.read(size)

for i in f:
	print i
#print f.readline()

f.close()
