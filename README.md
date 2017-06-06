# KafkaSql
Write SQL queries on Kafka Topics. 

Usage
To compile:
cd KafkaSql

mvn package

cd target

java -jar kafka.jar --query "select * from <KafkaTopic>" --start <YYYY-MM-dd> --end <YYYY-MM-dd> --limit 10 --brokers "<ip:port>[,<ip:port>]+"

java -jar kafka --query "select distinct data.vendor_tracking_id, updated_at from from shipment_group_feed_bigfoot where  (data.shipment_type ='approved_rto' or data.shipment_type='unapproved_rto')"  --start 2017-06-01 --end 2017-06-05  --limit 10000000 --brokers "<brokerlist>"

va -jar kafka --query "select updatedAt , data.status from shipment_group_feed_bigfoot" --start 2017-06-01  --end 2017-06-05  --limit 10000000  --brokers "<broker_list>"

java -jar kafka --query "select updatedAt , data.status from shipment_group_feed_bigfoot where entityId='Consignment-21059116'" --start 2017-04-19  --end 2017-04-21  --limit 10000000  --brokers "<broker_list>"

java -jar kafka --query "select distinct data.shipment_type  from shipment_feed_bigfoot"  --startEpoch 1489516200000 --endEpoch 1489537800000 --limit 10000000 --brokers "<broker_list>"
