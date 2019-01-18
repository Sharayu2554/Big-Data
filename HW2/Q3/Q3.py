# Databricks notebook source
from decimal import Decimal

def getMovieData(movie):
  return (movie[0], movie[2])

def getRatingsData(rating):
  ratingsData = rating.split(',')
  if ratingsData[2] == 'rating':
    return (0, (0.0, 0))
  return (ratingsData[1], (Decimal(ratingsData[2]), 1))

def getTagsData(tag):
  tagData = tag.split(',')
  if tagData[1] == 'movieId':
    return ('', '')
  return (tagData[1], tagData[2])

def reduceRatingObject(x, y):
  decx = Decimal(x[0])
  decy = Decimal(y[0])
  return (decx + decy, x[1]+y[1])

def averageRatingFunction(rating):
  if rating[1][1] is 0:
    return (0, 0.0)
  sumRating = Decimal(rating[1][0])
  count =  rating[1][1]
  average = sumRating / count
  return (rating[0], average)

def getAverageCSVLine(x):
  return str(str(x[0]) + '\t' + str('%.3f'%(x[1])))
  
def getLowAverageCSVLine(x):
  print( '%.3f'%(x[1]))
  return str(str(x[0]) + '\t' + str('%.3f'%(x[1])))
  
def getActionCSVLine(x):
  return str(str(x[0]) + '\t' + str('%.3f'%(x[1][0])) + '\t' + str(x[1][1]))
  
def getThrillerCSVLine(x):
  return str(str(x[0]) + '\t' +  str('%.3f'%(x[1][0][0])) + '\t' + str(x[1][0][1]) + '\t' + str(x[1][1]))
  
moviesdata = spark.read.csv(
    "dbfs:/FileStore/tables/movies.csv", header=True, mode="DROPMALFORMED"
)
movies = moviesdata.rdd.map(lambda x : getMovieData(x))
  
ratings = sc.textFile("dbfs:/FileStore/tables/ratings.csv").map(lambda x : getRatingsData(x))
ratingReduce = ratings.reduceByKey(lambda x, y : reduceRatingObject(x, y))

average = ratingReduce.map(lambda rating : averageRatingFunction(rating))
average.collect()
averageCSV = average.map(lambda x : getAverageCSVLine(x))
averageCSV.first()
averageCSV.saveAsTextFile("dbfs:/FileStore/BigDataAss2/Output/q3/q3_1_a/")

lowAverage = average.takeOrdered(10, key = lambda x: x[1])
lowAverageOrdered = sc.parallelize(lowAverage)
lowAverageCSV = lowAverageOrdered.filter(lambda x: x != (0, 0.0)).map(lambda x : getLowAverageCSVLine(x))
lowAverageCSV.first()
lowAverageCSV.saveAsTextFile("dbfs:/FileStore/BigDataAss2/Output/q3/q3_1_b/")

tags = sc.textFile("dbfs:/FileStore/tables/tags.csv").map(lambda x : getTagsData(x))
actionData = average.leftOuterJoin(tags).filter(lambda x : x[1][1] == 'action')
actionData.collect() 
actionCSV = actionData.map(lambda x : getActionCSVLine(x))
actionCSV.saveAsTextFile("dbfs:/FileStore/BigDataAss2/Output/q3/q3_2/")


thriller = actionData.leftOuterJoin(movies).filter(lambda x : 'Thriller' in x[1][1])
thriller.collect()

thrillerCSV = thriller.map(lambda x : getThrillerCSVLine(x))
thrillerCSV.saveAsTextFile("dbfs:/FileStore/BigDataAss2/Output/q3/q3_3/")



# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/BigDataAss2/Output/q3/q3_1_a/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/BigDataAss2/Output/q3/q3_1_b/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/BigDataAss2/Output/q3/q3_2/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/BigDataAss2/Output/q3/q3_3/

# COMMAND ----------


