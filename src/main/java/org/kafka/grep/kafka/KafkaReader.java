package org.kafka.grep.kafka;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.kafka.grep.utils.Utils;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaReader<T> {
    private final String topic;
    private final List<Broker> brokerList;

    private final Class<T> clazz;
    ObjectMapper mapper = new ObjectMapper();
    final int FETCH_SIZE = 20 * 1024 * 1024;
    private String startTime;
    private String endTime;
    private long endTimeEpoch;
    private int till;
    private long startTimeEpoch;
    private boolean skip_entity = true;

    private ExecutorService executorService = Executors.newFixedThreadPool(20);

    //.Undelivered_OtherCityMisroute, Undelivered_Incomplete_Address, received_by_merchant, InScan_Success, Undelivered_Order_Rejected_OpenDelivery, Undelivered_Order_Rejected_By_Customer, Undelivered_No_Response, Undelivered_OutOfDeliveryArea, Undelivered_Request_For_Reschedule, Undelivered_Corresponding_Pickup_Rejected, pickup_reattempt, Undelivered_NonServiceablePincode, Expected, dispatched_to_vendor, expected, pickup_scheduled, received, Undelivered_Heavy_Traffic, marked_for_merchant_dispatch, Undelivered_Shipment_Damage, undelivered_attempted, dispatched_to_merchant, undelivered_unattempted, dispatched_to_seller, not_received, Undelivered_COD_Not_Ready, returned_to_seller, marked_for_seller_return, Undelivered_Not_Attended, out_for_delivery


    public KafkaReader(String topic, List<Broker> brokerList, String startTime, String endTime, Class<T> clazz, int till, boolean skip_entity) {
        this.topic = topic;
        this.brokerList = brokerList;
        this.startTime = startTime;
        this.endTime = endTime;
        this.till = till;
        System.out.println("Grep from :" + startTime + "-" + endTime);
        this.clazz = clazz;
        if (this.clazz != JsonNode.class) {
            mapper.configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true);
        }
        this.skip_entity = skip_entity;
    }

    public KafkaReader(String topic, List<Broker> brokerList, String startTime, String endTime, Class<T> clazz, boolean skip_entity) {
        this.topic = topic;
        this.brokerList = brokerList;
        this.startTime = startTime;
        this.endTime = endTime;
        this.till = Integer.MAX_VALUE;
        System.out.println("Grep from :" + startTime + "-" + endTime);
        this.clazz = clazz;
        if (this.clazz != JsonNode.class) {
            mapper.configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true);
        }
        this.skip_entity = skip_entity;
    }

    public KafkaReader(String topic, List<Broker> brokerList, long startTime, long endTime, Class<T> clazz, int till, boolean skip_entity) {
        this.topic = topic;
        this.brokerList = brokerList;
        this.startTimeEpoch = startTime;
        this.endTimeEpoch = endTime;
        this.till = till;
        System.out.println("Grep from :" + startTime + "-" + endTime + " till " + till);
        this.clazz = clazz;
        if (this.clazz != JsonNode.class) {
            mapper.configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true);
        }
        this.skip_entity = skip_entity;
    }


    public void start(MessageHandler<T> messageHandler) {
        Optional<List<PartitionMetadata>> leaderOpt = KafkaZkUtils.findLeaders(brokerList, topic);
        if (!leaderOpt.isPresent()) {
            System.err.println("Can't find leaders for topic");
            return;
        }
        for (PartitionMetadata metaData : leaderOpt.get()) {
            BlockingQueue<String> queue = new LinkedBlockingQueue<>();
            AtomicBoolean toContinue = new AtomicBoolean(true);
            executorService.submit(() -> read(metaData, queue, toContinue, messageHandler));
            executorService.submit(() -> {
                int matches = 0;
                try {
                    while (true) {
                        String msg = queue.take();
                        if (msg.equals("POISON")) {
                            break;
                        }
                        T message;
                        if (clazz == String.class) {
                            message = (T) msg;
                        } else if (clazz == JsonNode.class) {
                            JsonNode root = mapper.readTree(msg);
                            if (skip_entity) {
                                root = root.get("entity");
                            }
                            message = (T) root;
                        } else {
                            message = mapper.readValue(msg, clazz);
                        }
                        boolean found = messageHandler.consumeMessage(message);
                        if (found) {
                            matches++;
                            if (matches > till) {
                                toContinue.set(false);
                                break;
                            }
                        }
                    }
                } catch (Throwable ex) {
                    ex.printStackTrace();
                    toContinue.set(false);
                }
            });

        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(100, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        executorService.shutdownNow();
    }

    private void read(PartitionMetadata partitionMetaData, BlockingQueue<String> outputQueue, AtomicBoolean toContinue, MessageHandler<T> messageHandler) {
        String clientId = "HubLoadPendencyfindRecorder";
        if (startTime != null && endTime != null) {
            startTimeEpoch = Utils.getForSimpleTime(startTime);
            endTimeEpoch = Utils.getForSimpleTime(endTime);
        }
        final Optional<Long> startReadOffsetOptional = KafkaZkUtils.getOffsetByTime(brokerList, topic, partitionMetaData.partitionId(), startTimeEpoch);
        final Optional<Long> endReadOffsetOptional = KafkaZkUtils.getOffsetByTime(brokerList, topic, partitionMetaData.partitionId(), endTimeEpoch);
        if (startReadOffsetOptional.isPresent() && endReadOffsetOptional.isPresent()) {
            final long startReadOffset = startReadOffsetOptional.get();
            final long endReadOffset = endReadOffsetOptional.get();
            ThreadPrinter.THREADPRINT.println(partitionMetaData.partitionId() + "\t" + partitionMetaData.leader().getConnectionString() + "\t" + startReadOffset + "->" + endReadOffset);
            // We update the broker list every time we find leaders as broker list may change
            List<Broker> brokerList = partitionMetaData.replicas();
            Broker leader = partitionMetaData.leader();
            SimpleConsumer consumer = KafkaZkUtils.createSimpleConsumer(leader.host(), leader.port(), clientId);
            long numRead = 0;
            long currentOffset = startReadOffset;
            try {
                while (!Thread.currentThread().isInterrupted() && currentOffset < endReadOffset && toContinue.get()) {
                    if (consumer == null) {
                        consumer = KafkaZkUtils.createSimpleConsumer(leader.host(), leader.port(), clientId);
                    }
                    FetchRequest req = new FetchRequestBuilder().clientId(clientId)
                            .addFetch(topic, partitionMetaData.partitionId(), currentOffset, FETCH_SIZE)
                            .build();

                    FetchResponse fetchResponse = null;
                    String reason = "";
                    try {
                        fetchResponse = consumer.fetch(req);
                    } catch (Exception e) {
                        reason = e.toString();
                    }

                    if (fetchResponse == null || fetchResponse.hasError()) {
                        if (fetchResponse != null) {
                            short code = fetchResponse.errorCode(topic, partitionMetaData.partitionId());
                            reason = ErrorMapping.exceptionFor(code).toString();
                        }
                        ThreadPrinter.THREADPRINT.println(String.format("Error fetching data from the broker: %s reason: %s", leader.host(), reason));
                        consumer.close();
                        consumer = null;

                        Optional<PartitionMetadata> newLeader = KafkaZkUtils.findNewLeader(brokerList, leader,
                                topic, partitionMetaData.partitionId());
                        if (!newLeader.isPresent() || newLeader.get().leader() == null) {
                            ThreadPrinter.THREADPRINT.println(String.format("Failed to find new leader for topic: %s, partition: %d", topic, partitionMetaData.partitionId()));
                            break;
                        } else {
                            brokerList = partitionMetaData.replicas();
                            leader = newLeader.get().leader();
                        }
                        continue;
                    }

                    for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partitionMetaData.partitionId())) {
                        long offset = messageAndOffset.offset();
                        if (offset < startReadOffset) {
                            ThreadPrinter.THREADPRINT.println(String.format("Found an old offset: %d, expecting: %d", offset, startReadOffset));
                            continue;
                        }
                        currentOffset = messageAndOffset.nextOffset();
                        Message message = messageAndOffset.message();

                        byte[] payload = new byte[message.payload().limit()];
                        message.payload().get(payload);
                        try {
                            outputQueue.put(new String(payload, StandardCharsets.UTF_8));
                            if (!toContinue.get()) {
                                break;
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            numRead++;
                            break;
                        }
                        numRead++;
                    }
                    ThreadPrinter.THREADPRINT.print(".");
                    messageHandler.partialUpdate();
                    // In case we are not able to read anything we should wait before retrying
                    if (numRead == 0) {
                        break;
                    }
                }
                messageHandler.partialUpdate();
                outputQueue.put("POISON");
                if (Thread.currentThread().isInterrupted()) {
                    System.out.println("Thread was interrupted.");
                }
                System.out.println("Completed");
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        } else {
            System.err.println("Older offsets");
        }
    }
}
