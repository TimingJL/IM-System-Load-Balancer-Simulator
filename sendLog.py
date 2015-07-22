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

for i in range(0,25):
	np.random.seed(i)
	network_delay_List.append(deque(np.random.pareto(1.1, 5000) + 0.))
	redisDB.set("user["+str(i)+"]_network_delay",network_delay_List[i].popleft())

# print network_delay_List[0]
# print network_delay_List[1]

for i in range(0,50000):
	random.seed(i)
	p.append(-math.log(1.0 - random.random()) / (1/0.4))
# #print p
# print redisDB.get("LogItemNumber")
# #print redisDB.get("LogBehavior[2][1]")

# for i in range(1,int(redisDB.get("LogItemNumber"))):
# 	temp = redisDB.get("LogBehavior[" + str(i) + "][1]")

for i in range(1,int(redisDB.get("LogItemNumber[2]"))):
	temp = redisDB.get("LogBehavior[" + str(i) + "][2]")
	tempDict = ast.literal_eval(temp)
	if tempDict["type"] == 0:
		redisMQ.rpush('is_online['+str(tempDict["content"]["user_ID"])+']', str(tempDict["content"]))
		redisDB.set("user["+str(tempDict["content"]["user_ID"])+"]_network_delay",network_delay_List[tempDict["content"]["user_ID"]].popleft())
		time.sleep(0.3)
	if tempDict["type"] == 1:
		t = p.popleft()
		print t
		time.sleep(t)
		redisMQ.rpush("senderID[" + str(tempDict["content"]["sender_ID"]) + "]", str(tempDict["content"]))

	# print "[" + str(i) + "]:"
	# print redisDB.get("LogBehavior[" + str(i) + "][1]")
	# time.sleep(1)

print "OK=============================================="

# [20]:
# {'content': {'user_ID': 20, 'is_online': False}, 'type': 0}
# [21]:
# {'content': {'receiver_ID': 4, 'test_time_stamp': '20150706-010851 am', 'sender_ID': 7, 'network_delay': 0.045781988336826035, 'is_VIP': False, 'message_birth_time': 1436159335.219545}, 'type': 1}
