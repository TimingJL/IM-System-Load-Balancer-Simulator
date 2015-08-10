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



def average_function(Finish_List_Name):
	#print "Finish_List_Name: %r" %(Finish_List_Name)
	job_number = int(redisDB.get("Finish_List["+Finish_List_Name+"]_job_number"))
	j = 0
	r = 0.
	d = 0.
	for i in range(1, job_number):
		tmp = redisDB.get("Finish_List["+Finish_List_Name+"][" + str(i) + "]")
		tmp_dict = ast.literal_eval(tmp)
		if int(tmp_dict["window"]) < 31:
			j = j + 1
			r = r + float(tmp_dict["response_time"])
			d = d + float(tmp_dict["network_delay"])
	print "%r response time: %r \t Job Number: %r \t Average Delay: %r" %(Finish_List_Name, r/j, j, d/j)

print "\n"

#average_function("MG1")
#average_function("MM1")
#average_function("SQF")
average_function("RR")
#average_function("MG1_RR")
#average_function("MG1_RR3")
#average_function("SQF_merge_LNU")
#average_function("MM1_merge_LNU")


print "\n"