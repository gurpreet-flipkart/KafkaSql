package org.kafka.grep.kafka;

import com.google.common.util.concurrent.Uninterruptibles;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

//import com.sun.corba.se.pept.broker.Broker;


public class KafkaZkUtils {
    private static final int TIMEOUT_MS = 10000;
    private static final int BUFFER_SIZE = 64 * 1024; // 64KB
    private static final Logger logger = LoggerFactory.getLogger(KafkaZkUtils.class);
    private static final int MAX_ATTEMPTS = 5;
    private static final int LEADER_RETRY_INTERVAL = 1000;

    public static SimpleConsumer createSimpleConsumer(String host, int port, String clientId) {
        return new SimpleConsumer(host, port, TIMEOUT_MS, BUFFER_SIZE, clientId);
    }

    public static List<Broker> deserializeBrokerList(String ser) {
        return Arrays.stream(ser.split(",")).map(x -> {
            String[] hostport = x.split(":");
            return new Broker(0, hostport[0], Integer.parseInt(hostport[1]));
        }).collect(Collectors.toList());
    }


    public static Map<String, TopicMetadata> getTopicInfo(List<Broker> brokerList_, List<String> topicNames) {
        // Shuffle the brokers list to avoid hitting the same broker for every request
        List<Broker> brokerList = new ArrayList<>(brokerList_);
        Collections.shuffle(brokerList, new Random(System.nanoTime()));

        for (Broker broker : brokerList) {
            SimpleConsumer consumer = null;
            try {
                consumer = createSimpleConsumer(broker.host(), broker.port(), "topic_lookup");
                TopicMetadataResponse resp = consumer.send(new TopicMetadataRequest(topicNames));
                Map<String, TopicMetadata> topicInfo = new HashMap<>();
                for (TopicMetadata metadata : resp.topicsMetadata()) {
                    topicInfo.put(metadata.topic(), metadata);
                }
                return topicInfo;
            } catch (Exception e) {
                logger.warn("Exception while looking up topic metadata", e);
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }
        return new HashMap<>();
    }

    public static Optional<PartitionMetadata> findLeader(List<Broker> brokerList, String topic, int partitionId) {
        Map<String, TopicMetadata> metadata = null;
        try {
            metadata = getTopicInfo(brokerList, Collections.singletonList(topic));
        } catch (Exception e) {
            logger.debug("Failed to get topic metadata", e);
            return Optional.empty();
        }
        TopicMetadata md = metadata.get(topic);
        if (md.errorCode() != ErrorMapping.NoError()) {
            return Optional.empty();
        }
        for (PartitionMetadata part : md.partitionsMetadata()) {
            if (part.partitionId() == partitionId) {
                if (part.errorCode() == ErrorMapping.NoError()) {
                    return Optional.of(part);
                }
                break;
            }
        }
        return Optional.empty();
    }

    public static Optional<List<PartitionMetadata>> findLeaders(List<Broker> brokerList, String topic) {
        Map<String, TopicMetadata> metadata = null;
        try {
            metadata = getTopicInfo(brokerList, Collections.singletonList(topic));
        } catch (Exception e) {
            logger.debug("Failed to get topic metadata", e);
            return Optional.empty();
        }
        TopicMetadata md = metadata.get(topic);
        if (md.errorCode() != ErrorMapping.NoError()) {
            return Optional.empty();
        }
        return Optional.of(md.partitionsMetadata().stream().filter(x -> x.errorCode() == ErrorMapping.NoError()).collect(Collectors.toList()));
    }


    public static Optional<PartitionMetadata> findNewLeader(List<Broker> brokerList, Broker oldLeader, String topic, int partition) {
        for (int i = 0; i < MAX_ATTEMPTS; i++) {
            Optional<PartitionMetadata> newLeader = findLeader(brokerList, topic, partition);
            if (!newLeader.isPresent() || newLeader.get().leader() == null) {
                logger.info("Leader not found for topic: {}, partition: {}. Retrying", topic, partition);
                Uninterruptibles.sleepUninterruptibly(LEADER_RETRY_INTERVAL, TimeUnit.MILLISECONDS);
            } else if (newLeader.get().leader() == oldLeader && i == 0) {
                logger.info("Leader unchanged for topic: {}, partition: {}. Retrying", topic, partition);
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                Uninterruptibles.sleepUninterruptibly(LEADER_RETRY_INTERVAL, TimeUnit.MILLISECONDS);
            } else {
                return newLeader;
            }
        }
        return Optional.empty();
    }

    public static Optional<Long> getOffsetByTime(List<Broker> brokerList, String topic, int partition, long time) {
        Optional<PartitionMetadata> partitionMetaDataOptional = KafkaZkUtils.findLeader(brokerList, topic, partition);

        if (!partitionMetaDataOptional.isPresent() || partitionMetaDataOptional.get().leader() == null) {
            System.err.println("Can't find leader for topic and partition");
            return Optional.empty();
        }

        String clientId = "HubLoadPendencyRecorder";
        Broker leader = partitionMetaDataOptional.get().leader();
        SimpleConsumer consumer = KafkaZkUtils.createSimpleConsumer(leader.host(), leader.port(), clientId);
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(time, 1));
        OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
        OffsetResponse response = consumer.getOffsetsBefore(request);
        if (response.hasError()) {
            consumer.close();
            System.err.println("Kafka Exception: " + ErrorMapping.exceptionFor(response.errorCode(topic, partition)));
            return Optional.empty();
        }
        long[] offsets = response.offsets(topic, partition);
        consumer.close();
        return Optional.of(offsets[0]);
    }
}
