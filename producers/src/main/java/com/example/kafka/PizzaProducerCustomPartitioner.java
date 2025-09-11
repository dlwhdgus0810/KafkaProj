package com.example.kafka;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class PizzaProducerCustomPartitioner {

    private static final Logger log = LoggerFactory.getLogger(PizzaProducerCustomPartitioner.class.getName());

    public static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer,
                                        String topicName, int iterCount, int interIntervalMillis, int intervalMillis, int intervalCount, boolean sync) {

        PizzaMessage pizzaMessage = new PizzaMessage();

        int iterSeq = 0;

        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        while (iterSeq++ != iterCount) {
            HashMap<String, String> pMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, pMessage.get("key"), pMessage.get("message"));
            sendMessage(kafkaProducer, producerRecord, pMessage, sync);

            sleepBetweenIntervalCount(intervalMillis, intervalCount, iterSeq);
            sleepInterIntervalMillis(interIntervalMillis);
        }
    }

    public static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                   ProducerRecord<String, String> producerRecord,
                                   HashMap<String, String> pMessage, boolean sync) {

        if (!sync) {
            kafkaProducer.send(producerRecord, (metadata, e) -> {
                if (e == null) {
                    logMetadata(pMessage, metadata);
                } else {
                    logError(e);
                }
            });
        } else {
            try {
                RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
                logMetadata(pMessage, metadata);
            } catch (InterruptedException | ExecutionException e) {
                logError(e);
            }
        }
    }

    private static void logError(Exception e) {
        log.error(e.getMessage());
    }

    private static void logMetadata(HashMap<String, String> pMessage, RecordMetadata metadata) {
        log.info("async message: {}, partition: {}, offset: {}", pMessage.get("key"), metadata.partition(), metadata.offset());
    }

    public static void main(String[] args) {
        String topicName = "pizza-topic-partitioner";

        // KafkaProducer config setting
        Properties props = new Properties();

        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.example.kafka.CustomPartitioner");
        props.setProperty("custom.specialKey", "P001");

        // KafkaProducer object creation
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        sendPizzaMessage(kafkaProducer, topicName,
                -1, 100, 0, 0, true);

        kafkaProducer.close();
    }

    private static void sleepInterIntervalMillis(int interIntervalMillis) {
        if (interIntervalMillis > 0) {
            try {
                log.info("###### interIntervalMillis: {} ######", interIntervalMillis);
                Thread.sleep(interIntervalMillis);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void sleepBetweenIntervalCount(int intervalMillis, int intervalCount, int iterSeq) {
        if ((intervalCount > 0) && (iterSeq % intervalCount == 0)) {
            try {
                log.info("###### IntervalCount: {}, intervalMills: {} ######", intervalCount, intervalMillis);
                Thread.sleep(intervalMillis);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
