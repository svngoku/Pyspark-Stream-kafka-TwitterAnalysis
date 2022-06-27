# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
import pandas as pd
from pathlib import Path

spark = SparkSession.builder.appName("MyApp").getOrCreate()

rdd_context = spark.sparkContext.textFile("./datas/datas.txt")

rdd_context.collect()

"""## Map"""

rdd_file = rdd_context.map(lambda x: x.upper())

print(rdd_file.collect())   

"""## FlatMap"""

flatmap = ["mot1 mot2 ", "mot3 mot4 "," mot5 mot6 "]

rdd_flatmap = spark.sparkContext.parallelize(flatmap)

for flatten in rdd_flatmap.collect():
  print(flatten)

rdd_flatmap2 = rdd_flatmap.flatMap(lambda x: x.split(" "))
for flatten2 in rdd_flatmap2.collect():
  print(flatten2)

def mapplify_mapflatify(datas):
  rdd_context = spark.sparkContext.parallelize(datas)
  rdd_mapped = rdd_context.map(lambda x: x)
  rdd_flatmapped = rdd_mapped.flatMap(lambda x: x)
  for element in rdd_flatmapped.collect():
    print(element)

mapplify_mapflatify(flatmap)

"""## Filter"""

filter_datas = [ "Pierre", "Alpha", "Mehdi", "Jean-Pierre", "Thierry"]

# filter_rdd_context = spark.sparkContext.parallelize(filter_datas)



































