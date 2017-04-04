# KafkaSql
Write SQL queries on Kafka Topics. 

Usage
To compile:
cd KafkaSql
mvn package
cd target

java -jar kafka.jar --query "select * from <KafkaTopic>" --start <YYYY-MM-dd> --end <YYYY-MM-dd> --limit 10 --brokers "<ip:port>[,<ip:port>]+"


