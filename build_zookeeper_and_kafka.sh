#!/bin/bash

docker rm -f kafka zookeeper 2>/dev/null

docker network create kafka-net 2>/dev/null || true

# Zookeeper
docker run -d \
  --name zookeeper \
  --network kafka-net \
  -e ZOOKEEPER_CLIENT_PORT=2181 \
  -e ZOOKEEPER_TICK_TIME=2000 \
  confluentinc/cp-zookeeper:7.5.0

echo "Waiting for Zookeeper..."
sleep 5

# Kafka
docker run -d \
  --name kafka \
  --network kafka-net \
  -p 9092:9092 \
  -e KAFKA_BROKER_ID=0 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
  confluentinc/cp-kafka:7.5.0


docker exec -it kafka kafka-topics \
  --delete \
  --topic cppkafka_test1 \
  --bootstrap-server localhost:9092


docker exec -it kafka kafka-topics \
  --create \
  --topic cppkafka_test1 \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists \
  --bootstrap-server localhost:9092


docker exec -it kafka kafka-topics \
  --delete \
  --topic cppkafka_test2 \
  --bootstrap-server localhost:9092


docker exec -it kafka kafka-topics \
  --create \
  --topic cppkafka_test2 \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists \
  --bootstrap-server localhost:9092
