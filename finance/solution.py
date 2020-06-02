#!/usr/bin/env python3

import argparse
from sys import exit
from data import generator
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StringType, FloatType, DateType
from pyspark.sql.functions import col, udf, sum, round, desc, max
    ## note that it is bad practice in the future to import sum and round by overwriting default behaviour of these functions
    ## is the design of the script below a good way to answer these questions?...probably not

class finance:

    def __init__(self, lines = 1000):
        self.data = generator.finance(lines = lines)

## Q1: put data into dataframe where the schema is string, date, float, string
    def question_one(self, spark, data_only = False):
        substring_udf = udf(lambda x: x[1:], StringType())
        sublist_udf = udf(lambda x: x[0], StringType())
        rawDF = spark.createDataFrame(self.data)\
                     .toDF("customer_id","date","purchase_cost","company")\
                     .withColumn("purchase_cost", substring_udf("purchase_cost").cast("float"))\
                     .withColumn("company", sublist_udf("company"))
        if data_only:
            return(rawDF)
        else:
            rawDF.show(5, False)
            rawDF.printSchema()
        
## Q2: aggregate the purchase_costs column against each company to find which company is the most profitable
    def question_two(self, spark):
        rawDF = self.question_one(spark, data_only = True)
        rawDF.groupBy("company")\
             .agg(round(sum("purchase_cost"), 2).alias("revenue"))\
             .orderBy(desc("revenue"))\
             .show(10)
        
## Q3: which date was the most profitable per company
    def question_three(self, spark):
        rawDF = self.question_one(spark, data_only = True)
        window = Window.partitionBy("company").orderBy(desc("revenue"))
        rawDF.groupBy("date", "company")\
             .agg(round(sum("purchase_cost"), 2).alias("revenue"))\
             .withColumn("max_revenue", max("revenue").over(window))\
             .filter(col("max_revenue") == col("revenue"))\
             .withColumnRenamed("date", "best_day")\
             .drop("revenue")\
             .show(10)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--lines", dest = "lines", action = "store", default = "1000")
    parser.add_argument("-o", "--option", dest = "option", action = "store", choices = ["1","2","3"])
    args = parser.parse_args()

    if not args.option:
        print("script cannot continue without an option selected")
        exit()

    spark = SparkSession.builder.appName("finance_questions").master("local[*]").getOrCreate()
    if args.option == "1":
        finance(lines = args.lines).question_one(spark)
    elif args.option == "2":
        finance(lines = args.lines).question_two(spark)
    elif args.option == "3":
        finance(lines = args.lines).question_three(spark)
    else:
        print("something has gone wrong...")

    spark.stop()

