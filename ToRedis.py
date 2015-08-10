#coding=utf-8
import redis
import random
import math
import time
import ast
import numpy as np
from collections import deque

redisDB = redis.StrictRedis(host='localhost', port=6379, db=0)
redisMQ = redis.Redis()


f = open('./log/20150809-232028 pm-LogBehavior_1.txt','r') 

count = 0
for i in f:
	redisDB.set("LogBehavior[" + str(count) + "][1]", i)
	count = count + 1

redisDB.set("LogItemNumber[1]",count)
print "Done"