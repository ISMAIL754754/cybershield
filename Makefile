SUBMIT=docker compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy --conf spark.driver.host=spark-master \
  --packages $$(grep SPARK_KAFKA_PACKAGE .env | cut -d= -f2)

up:        ## démarre cluster HDFS + Spark + Kafka
	docker compose up -d
hdfs-init: ## crée les répertoires HDFS
	docker compose exec namenode hdfs dfs -mkdir -p /cybershield/raw /cybershield/checkpoints /cybershield/models
	docker compose exec namenode hdfs dfs -chmod -R 777 /cybershield
check:     ## vérifie réplication 2 et 2 DataNodes
	docker compose exec namenode hdfs dfsadmin -report | grep -E "Live datanodes|Configured Capacity"
	docker compose exec namenode hdfs getconf -confKey dfs.replication
produce:   ## rejoue CICIDS2017 vers Kafka
	docker compose --profile producer run --rm producer
archive:   ## Kafka -> HDFS (Parquet)
	$(SUBMIT) /opt/cybershield/ingestion/archive_to_hdfs.py
topics:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic network-flows
peek:      ## lit 5 messages du topic
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic network-flows --from-beginning --max-messages 5
down:
	docker compose down
reset:     ## supprime aussi les volumes
	docker compose down -v
