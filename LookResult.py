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

a = redisDB.get("Finish_List[5.1]_job_number")
b = redisDB.get("Finish_List[5.1][" + str(a) + "]")

print "LogItemNumber: %r" % (redisDB.get("LogItemNumber[2]"))

print "\nfinishied job: %r" %(a)
print "finishied item: %r" %(b)

