1) Start the Zookeeper:

.\zookeeper-server-start.bat ../../config/zookeeper.properties

2) Start the Kafka broker:

.\kafka-server-start.bat ../../config/server.properties

3) Create a topic named TRANSACTION with 2 partitions:

.\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic TRANSACTION
