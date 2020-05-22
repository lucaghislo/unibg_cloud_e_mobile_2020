###### TEDindeX-Load-Aggregate-Model
######

import sys
import json
import pyspark
from pyspark.sql.functions import struct, col, collect_list, array_join

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


##### FROM FILES
tedx_dataset_path = "s3://tedindex-data/tedx_dataset.csv"

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#### READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read.option("header","true").option("quote", "\"").option("escape", "\"").csv(tedx_dataset_path)
    
tedx_dataset.printSchema()

#### FILTER ITEMS WITH NULL POSTING KEY
count_items = tedx_dataset.count()
count_items_null = tedx_dataset.filter("idx is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")


## READ TAGS DATASET
tags_dataset_path = "s3://tedindex-data/tags_dataset.csv"
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)

## READ WATCH NEXT DATASET
watch_next_dataset_path = "s3://tedindex-data/watch_next_dataset.csv"
watch_next_dataset = spark.read.option("header","true").csv(watch_next_dataset_path).dropDuplicates()
watch_next_dataset.printSchema()

## READ SPEAKER INFO DATASET
speaker_info_dataset_path = "s3://tedindex-data/info_speaker.csv"
speaker_info_dataset = spark.read.option("header","true").csv(speaker_info_dataset_path)

# CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX_DATASET
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags"))
tags_dataset_agg.printSchema()

tedx_dataset_agg = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left").drop("idx_ref").select(col("idx").alias("_id"), col("*")).drop("idx")
tedx_dataset_agg.printSchema()

# ADD IDX OF WATCH NEXT VIDEOS
watch_next_dataset_agg = watch_next_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("url").alias("watch_next_url"))
watch_next_dataset_agg.printSchema()

tedx_dataset_agg = tedx_dataset.join(watch_next_dataset_agg, tedx_dataset.idx == watch_next_dataset_agg.idx_ref, "left").drop("idx_ref").select(col("idx").alias("_id"), col("*")).drop("idx")
tedx_dataset_agg.printSchema()

# ADD INFO ABOUT SPEAKER
tedx_dataset_agg = tedx_dataset_agg.join(speaker_info_dataset, tedx_dataset_agg.main_speaker == speaker_info_dataset.name_speaker, "left").select(col("*"), struct(col("name_speaker").alias("speaker_name"), col("speaker_link").alias("speaker_url"), col("profession").alias("speaker_profession"), col("info_about").alias("info")).alias("speaker")).drop("name_speaker").drop("speaker_link").drop("profession").drop("info_about")
tedx_dataset_agg.printSchema()

mongo_uri = "mongodb://lucaghislotti-shard-00-00-9vbja.mongodb.net:27017,lucaghislotti-shard-00-01-9vbja.mongodb.net:27017,lucaghislotti-shard-00-02-9vbja.mongodb.net:27017"

write_mongo_options = {
    "uri": mongo_uri,
    "database": "TCM_TEDindeX",
    "collection": "tedx_data",
    "username": "admin",
    "password": "skywalker",
    "ssl": "true",
    "ssl.domain_match": "false"}
    
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_agg, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
