"""Voie froide : Kafka -> HDFS (Parquet) avec Spark Structured Streaming.
Les événements bruts (JSON) sont archivés tels quels, partitionnés par date d'ingestion.
"""
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("cybershield-archive")
         .config("spark.hadoop.dfs.replication", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", "network-flows")
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", 50000)
       .load())

out = (raw.select(F.col("key").cast("string").alias("event_id"),
                  F.col("value").cast("string").alias("json"),
                  F.col("topic"), F.col("partition"), F.col("offset"),
                  F.col("timestamp").alias("kafka_ts"))
       .withColumn("ingest_date", F.to_date("kafka_ts")))

q = (out.writeStream.format("parquet")
     .option("path", "hdfs://namenode:9000/cybershield/raw/flows")
     .option("checkpointLocation", "hdfs://namenode:9000/cybershield/checkpoints/archive")
     .partitionBy("ingest_date")
     .trigger(processingTime="10 seconds")
     .start())
q.awaitTermination()
