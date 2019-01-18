# Databricks notebook source
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

# Load and parse the data
data = sc.textFile("/FileStore/tables/ratings.dat")
ratings = data.map(lambda l: l.split('::')).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

#rdd = sc.parallelize(ratings.collect())
test, train = ratings.randomSplit(weights=[0.4,0.6],seed=1)

# COMMAND ----------


# Build the recommendation model using Alternating Least Squares
rank = 2
numIterations = 20
model = ALS.train(train, rank, numIterations)

# COMMAND ----------


# Evaluate the model on training data
testdata = test.map(lambda p: (p[0], p[1]))
predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))

# COMMAND ----------

ratesAndPreds = test.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
print("Mean Squared Error = " + str(MSE))

# COMMAND ----------

test.first()

# COMMAND ----------

predictions.first()

# COMMAND ----------

ratesAndPreds.first()

# COMMAND ----------

import math
math.sqrt(MSE)

# COMMAND ----------

model.save(sc, "/FileStore/BigDataAssignment3/myCollaborativeFilter")
sameModel = MatrixFactorizationModel.load(sc, "/FileStore/BigDataAssignment3/myCollaborativeFilter")
