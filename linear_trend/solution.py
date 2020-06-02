#!/usr/bin/env python3

from data import generator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg

data = generator.basic_linear(lines = 1000)

spark = SparkSession.builder.appName("trend").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

## questions one
## assume the underlying data shows a linear trend, using a simplistic model (y = mx + b), predict the null values

rawDF = spark.createDataFrame(data).toDF("x", "y")

trainDF = rawDF.filter(col("y").isNotNull())
testDF = rawDF.filter(col("y").isNull())

aggDF = trainDF.withColumn("x_2", col("x")**2)\
               .withColumn("xy", col("x")*col("y"))\
               .agg(sum("x").alias("x_sum"),
                    sum("y").alias("y_sum"),
                    sum("x_2").alias("x_2_sum"),
                    sum("xy").alias("xy_sum"),
                    count("x").alias("n"),
                    avg("x").alias("x_bar"),
                    avg("y").alias("y_bar"))\
               .collect()

Sxy = aggDF[0][3] - ((aggDF[0][0]*aggDF[0][1])/aggDF[0][4])
Sxx = aggDF[0][2] - ((aggDF[0][0]**2)/aggDF[0][4])
b1 = Sxy/Sxx
b0 = aggDF[0][6] - b1*aggDF[0][5]

print("the value of b1 is {} and b0 is {}".format(b1, b0))

testDF = testDF.withColumn("y", (col("x")*b1) + b0)
testDF.show(10)

spark.stop()
