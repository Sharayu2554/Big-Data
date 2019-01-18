# Databricks notebook source
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row

def mapSingleFriend(user, friend, friends):
  newfriendlist = friends[:]
  if (user < friend) :
    newfriendlist.remove(friend)
    return  (user+','+friend , ','.join(newfriendlist))
  else:
    newfriendlist.remove(friend)
    return  (friend+','+user , ','.join(newfriendlist))

def friendsMapping(user, friends):
  friendlist = friends[:]
  maps = []
  for friend in friends:
    struct = mapSingleFriend(user, friend, friendlist)
    maps.append(struct)
  return maps

def friendsReducing(x, y):
  xs = x.split(',')
  ys = y.split(',')
  yscount = len(list(set(xs).intersection(ys)))
  print(yscount)
  return (','.join(list(set(xs).intersection(ys))), yscount)

def getSortingKey(tuple):
  if(len(tuple) >= 2):
    return tuple[1]
  return 0

def getUserDataRow(user):
  userData = user.split(",")
  return Row(userid=userData[0],fname=userData[1], lname=userData[2], address=userData[3])

def getOutputCSVLine(x):
  return str(str(x[0]) + '\t' + str(x[1]) + '\t' + str(x[2]) + '\t' + str(x[3]) + '\t' + str(x[4]) + '\t' + str(x[5]) + '\t' + str(x[6]) + '\t' + str(x[7]) + '\t' + str(x[8]))
  
tokenized = sc.textFile("dbfs:/FileStore/tables/soc_LiveJournal1Adj-2d179.txt").map(lambda line: line.split("\t"))
friends = tokenized.map(lambda line : (line[0],line[1].split(',')))
friend = friends.first()
friendMap = friends.flatMap(lambda friend : friendsMapping(friend[0], friend[1]))

dataReduced = friendMap.reduceByKey(lambda x, y:  friendsReducing(x,y)).takeOrdered(10, key = lambda x: -getSortingKey(x[1]))
#dataReduced[0][0].split(',')
#dataReduced[0][1][1]

userdata = sc.textFile("dbfs:/FileStore/tables/userdata.txt")
userdataMap = userdata.map(lambda user : getUserDataRow(user))
userdataMap.first()

dataMap = sc.parallelize(dataReduced)
data = dataMap.map(lambda x : Row(userA=(x[0].split(',')[0]), userB=(x[0].split(',')[1]), friendList=x[1][0], count=x[1][1] ))

mutualFriend = spark.createDataFrame(data)
mutualFriend.createOrReplaceTempView("mutualFriend")

userData = spark.createDataFrame(userdataMap)
userData.createOrReplaceTempView("userData")

finaldata = spark.sql("SELECT m.count as count, m.userA as userA, ua.fname as fnameA, ua.lname as lnameA, ua.address as addressA, m.userB as userB, ub.fname as fnameB, ub.lname as lnameB, ub.address as addressB FROM mutualFriend m , userData ua, userData ub WHERE ua.userid = m.userA and ub.userid = m.userB ")

iterate = finaldata.rdd.collect()
output = sc.parallelize(iterate)
outputCSV = output.map(lambda x : getOutputCSVLine(x))
outputCSV.collect()
outputCSV.first()
outputCSV.saveAsTextFile("dbfs:/FileStore/BigDataAss2/Output/q2/")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/BigDataAss2/Output/q2/

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/BigDataAss2/Output/q2/"))

# COMMAND ----------


