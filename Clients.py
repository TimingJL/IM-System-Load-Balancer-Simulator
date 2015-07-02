#coding=utf-8
import pika
import time
import sys
import math
import random
import time
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

SimulateStart = strftime("%Y%m%d-%H%M%S %P")

number_of_users = 2

#user define
class User(threading.Thread):
	is_online = True
	def __init__(self, userID):
		threading.Thread.__init__(self)
		self.userID = userID

	#Generate Random Timings for a Poisson Process
	#http://preshing.com/20111007/how-to-generate-random-timings-for-a-poisson-process/
	def nextTime(self, rateParameter):
		return -math.log(1.0 - random.random()) / rateParameter

	#turn on and off is_online by random poisson
	def clock(self):
		while True:
			t = self.nextTime(1.0/5.0)#average is_online time
			time.sleep(t)
			self.is_online = not self.is_online
			print '%r: %r' %(self.userID, self.is_online)

	#user thread start
	def run(self):
		timer = Thread(target=self.clock)
		timer.start()
		while True:
			if(self.is_online):
				print self.userID
				t = self.nextTime(1.0/0.4)#average sending time
				time.sleep(t)	

#create users
userList = []
for i in range(1, number_of_users+1):
	userList.append(User(i))

#user start
for i in userList:
	i.start()
	