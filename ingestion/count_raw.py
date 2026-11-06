"""Vérifie l'archive HDFS : nombre d'événements, doublons et distribution des labels.
Lit les Parquet écrits par archive_to_hdfs.py (colonne json brute).
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("cybershield-count-raw").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://namenode:9000/cybershield/raw/flows")

total = df.count()
distinct_ids = df.select("event_id").distinct().count()
print(f"\n=== Événements archivés : {total} | event_id distincts : {distinct_ids} | doublons : {total - distinct_ids}")

print("=== Répartition par partition Kafka")
df.groupBy("partition").count().orderBy("partition").show()

print("=== Distribution des labels")
(df.withColumn("label", F.get_json_object("json", "$.label"))
   .groupBy("label").count().orderBy(F.desc("count")).show(truncate=False))

spark.stop()