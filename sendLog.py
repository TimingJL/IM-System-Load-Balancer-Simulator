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

# np.random.seed(1)
# p = deque((np.random.poisson(0.2, 50000)))
p = deque([])
network_delay_List = []

for i in range(0,120):
	np.random.seed(i)
	network_delay_List.append(deque(np.random.pareto(1.1, 50000) + 0.))
	redisDB.set("user["+str(i)+"]_network_delay",network_delay_List[i].popleft())

# print network_delay_List[0]
# print network_delay_List[1]

for i in range(0,50000):
	random.seed(i)
	p.append(-math.log(1.0 - random.random()) / (1/0.7))
# #print p
# print redisDB.get("LogItemNumber")
# #print redisDB.get("LogBehavior[2][1]")

# for i in range(1,int(redisDB.get("LogItemNumber"))):
# 	temp = redisDB.get("LogBehavior[" + str(i) + "][1]")

for i in range(1,int(redisDB.get("LogItemNumber[3]"))):
	temp = redisDB.get("LogBehavior[" + str(i) + "][3]")
	tempDict = ast.literal_eval(temp)
	if tempDict["type"] == 0:
		redisMQ.rpush('is_online['+str(tempDict["content"]["user_ID"])+']', str(tempDict["content"]))
		redisDB.set("user["+str(tempDict["content"]["user_ID"])+"]_network_delay",network_delay_List[tempDict["content"]["user_ID"]].popleft())
		#time.sleep(0.1)
	if tempDict["type"] == 1:
		t = p.popleft()
		print t
		time.sleep(t)
		#redisMQ.rpush("senderID[" + str(tempDict["content"]["sender_ID"]) + "]", str(tempDict["content"]))

		# send message to CM(not through LB)
		print tempDict["content"]
		print type(tempDict["content"])
		CM_ID = redisDB.get("User["+str(tempDict["content"]["receiver_ID"])+"]_in_which_CM")
		redisMQ.rpush("receiverCM_ID[" + str(CM_ID) +"]", str(tempDict["content"]))#send message to the CM(to recognize the receiver, not sender)
		#print message

print "OK=============================================="