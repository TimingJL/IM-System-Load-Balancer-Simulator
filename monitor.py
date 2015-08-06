#coding=utf-8
import time
import redis
import parameter

number_of_CMs = parameter.number_of_CMs
redisDB = redis.StrictRedis(host='localhost', port=6379, db=0)

while True:
	for i in range(1, number_of_CMs+1):
		print "CM[%r] Queue Length: %r" %(i, redisDB.get("CM[" + str(i) + "]QueueLength"))
	print "\n"

	for i in range(1, number_of_CMs+1):
		user_list = eval(redisDB.get("CM["+str(i)+"]_user_list"))
		print "CM[%r] Predict Queue Length: %r" %(i, int(redisDB.get("CM[" + str(i) + "]QueueLength")) + len(user_list))
	print "\n"	

	for i in range(1, number_of_CMs+1):
		print "CM[%r] Reponse Time_MM1: %r" %(i, redisDB.get("CM[" + str(i) + "]temp_response_time"))
	print "\n"

	for i in range(1, number_of_CMs+1):
		print "CM[%r] Reponse Time_MM1_LNU: %r" %(i, redisDB.get("CM[" + str(i) + "]_pseudo_length_response_time"))
	print "\n"

	for i in range(1, number_of_CMs+1):
		#print "CM[%r] Reponse Time_MG1: %r" %(i, redisDB.get("CM[" + str(i) + "]MG1_temp_response_time"))
		user_list = eval(redisDB.get("CM["+str(i)+"]_user_list"))
		print "CM[%r] user list {%r}: %r" %(i, len(user_list), user_list)
	print "==================================="			
	time.sleep(1)