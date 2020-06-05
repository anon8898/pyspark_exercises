#!/usr/env/bin python3

from pyspark.sql import SparkSession
from data import generator
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType, StringType
import argparse

class run:

    def __init__(self, spark, lines = 1000):
        data = generator.sweets(lines = lines)
        self.money_to_int = f.udf(lambda x: int(x[1:-3]), IntegerType())
        self.rawDF = spark.createDataFrame(data).toDF("customer_id", "sweets", "raised_funds", "location_one", "location_two", "location_three")

## question one: which sweet raised the most money
    def question_one(self):
        self.rawDF.withColumn("raised_funds", self.money_to_int("raised_funds"))\
                  .groupBy("sweets")\
                  .agg(f.sum("raised_funds").alias("total_funds"))\
                  .orderBy(f.desc("total_funds"))\
                  .show(10, False)


## question two: the location columns are how the funds for a sweet and customer is distributed after purchase, find a table on the breakdown of funds for each area vs sweets
    def question_two(self):
        self.rawDF.withColumn("raised_funds", self.money_to_int("raised_funds"))\
                  .withColumn("location_one", f.col("location_one").cast("float")*f.col("raised_funds"))\
                  .withColumn("location_two", f.col("location_two")*f.col("raised_funds"))\
                  .withColumn("location_three", f.col("location_three")*f.col("raised_funds"))\
                  .groupBy("sweets")\
                  .agg(f.round(f.sum("location_one"), 2).alias("location_one"),
                       f.round(f.sum("location_two"), 2).alias("location_two"),
                       f.round(f.sum("location_three"), 2).alias("location_three"))\
                  .show(10, False)

## question three: find all instances of velvet flavoured sweets and count which one has the most instances
    def question_three(self):
        self.rawDF.select("customer_id", "sweets")\
                  .where(f.col("sweets").like("%velvet%"))\
                  .groupBy("sweets")\
                  .agg(f.count("customer_id").alias("counter"))\
                  .orderBy(f.desc("counter"))\
                  .show(10, False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--lines", dest = "lines", action = "store", default = 1000)
    parser.add_argument("-o", "--option", dest = "option", action = "store", choices = ["1","2","3"])
    args = parser.parse_args()

    spark = SparkSession.builder.appName("sweets").master("local[*]").getOrCreate()  
    spark_class = run(spark, lines = args.lines)

    if args.option == "1":
        spark_class.question_one()
    if args.option == "2":
        spark_class.question_two()
    if args.option == "3":
        spark_class.question_three()

    spark.stop()
