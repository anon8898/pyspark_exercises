#!/usr/bin/env python3

from data import generator
import argparse
from sys import exit
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, split, explode, count, desc, when, lit, sum
from pyspark.sql.types import IntegerType

class start_spark:

    def __init__(self, lines = 100000):
        data = generator.fruit(lines = int(lines))
        self.spark = SparkSession.builder.appName("fruit_categories").master("local[*]").getOrCreate()
#        self.spark.sparkContext.setLogLevel("WARN")
        self.rawDF = self.spark.createDataFrame(data).toDF("customer_id", "age", "items")
        ##self.rawDF.show(10, False)

    ## Question One: Count how mamy apples are bought per customer, order the results in descending order by the customer that bought the most apples, then by customer id (in any order)
    def question_one(self):
        self.rawDF.withColumn("items_list", explode(split("items", ",")))\
                  .filter(col("items_list") == "apple")\
                  .groupBy("customer_id")\
                  .agg(count("items_list").alias("count_apples"))\
                  .join(self.rawDF.select("customer_id"), on = "customer_id", how = "right_outer")\
                  .na.fill(0)\
                  .dropDuplicates()\
                  .orderBy(desc("count_apples"), "customer_id")\
                  .show(15)
        self.spark.stop()

    ## Qustion Two: Count how many pears are bought by customers, split by consumers above the age of 30, and below (or equal) the age of 30
    def question_two(self):
        self.rawDF.withColumn("items_list", explode(split("items", ",")))\
                  .filter(col("items_list") == "pear")\
                  .withColumn("age_bracket", when(col("age") <= 30, "below 30").otherwise("above 30"))\
                  .groupBy("age_bracket")\
                  .agg(count("items_list").alias("count_pears"))\
                  .show(15)
        self.spark.stop()

    ## Question Three: count how many fruits are consumed per person, each fruit needs to be a new column
    def question_three(self):
        baseDF = self.rawDF.withColumn("items_list", explode(split("items", ",")))
        unique_set = baseDF.select("items_list").dropDuplicates().collect()
        window = Window.partitionBy('customer_id')
        for x in unique_set:
            baseDF = baseDF.withColumn(x[0], when(col("items_list") == x[0], 1).otherwise(0))\
                           .withColumn("sum_{}".format(x[0]), sum(x[0]).over(window))
        baseDF.select(["customer_id"] + ["sum_{}".format(x[0]) for x in unique_set])\
              .dropDuplicates()\
              .show(15)
        self.spark.stop()

    def no_viable_option(self):
        print("no viable option was selected")
        self.spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "pyspark exercises - fruit")
    parser.add_argument("-l", "--lines", dest = "lines", action = "store", help = "number of lines the data generated will be, a minimum of 10 lines is required", default = 100000)
    parser.add_argument("-o", "--option", dest = "option", help = "how the question will be evaluated, options include udf, spark, sql")
    args = parser.parse_args()

    if not args.option:
        print("no option was chosen")
        exit()

    spark_class = start_spark(args.lines)
    if args.option.lower() in ["question_one", "one", "1"]:
        spark_class.question_one()
    elif args.option.lower() in ["question_two", "two", "2"]:
        spark_class.question_two()
    elif args.option.lower() in ["question_three", "three", "3"]:
        spark_class.question_three()
    else:
        spark_class.no_viable_option()



