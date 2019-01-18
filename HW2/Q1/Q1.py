# Databricks notebook source
import sys
from pyspark import SparkContext, SparkConf

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
  return ','.join(list(set(xs).intersection(ys)))
  
def getDataCSVLine(x):
  keys = x[0].split(',')
  return str(str(keys[0]) + '\t' + str(keys[1]) + '\t' + str(x[1]))
  
tokenized = sc.textFile("dbfs:/FileStore/tables/soc_LiveJournal1Adj-2d179.txt").map(lambda line: line.split("\t"))
friends = tokenized.map(lambda line : (line[0],line[1].split(',')))
friend = friends.first()
friendMap = friends.flatMap(lambda friend : friendsMapping(friend[0], friend[1]))

dataReduced = friendMap.reduceByKey(lambda x, y:  friendsReducing(x,y))
dataReduced.collect()
#dataReduced.first()

dataCSV = dataReduced.map(lambda x : getDataCSVLine(x))
#dataCSV.first()
#dataCSV.saveAsTextFile("dbfs:/FileStore/BigDataAss2/Output/q1/")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/BigDataAss2/Output/q1
# MAGIC   
