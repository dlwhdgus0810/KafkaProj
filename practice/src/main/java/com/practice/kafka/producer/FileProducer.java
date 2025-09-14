package com.practice.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class FileProducer {

    private static final Logger log = LoggerFactory.getLogger(FileProducer.class.getName());
    private static final String DELIMITER = ",";

    public static void main(String[] args) {
        String topicName = "file-topic";

        // KafkaProducer config setting
        Properties props = new Properties();

        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer object creation
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        String filePath = "practice/src/main/resources/pizza_sample.txt";

        sendFileMessages(kafkaProducer, topicName, filePath);

        kafkaProducer.close();
    }

    private static void sendFileMessages(KafkaProducer<String, String> kafkaProducer, String topicName, String filePath) {
        String line;

        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(DELIMITER);
                String key = tokens[0];
                StringBuilder value = new StringBuilder();

                for (int i = 0; i < tokens.length; i++) {
                    if (i != (tokens.length - 1)) {
                        value.append(tokens[i]).append(",");
                    } else {
                        value.append(tokens[i]);
                    }
                }

                sendMessage(kafkaProducer, topicName, key, value.toString());
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void sendMessage(KafkaProducer<String, String> kafkaProducer, String topicName, String key, String value) {
        kafkaProducer.send(new ProducerRecord<>(topicName, key, value), ((metadata, exception) -> {
            if (exception == null) {
                log.info("[record metadata received]\n partition: {}\n offset: {}\n timestamp: {}",
                        metadata.partition(), metadata.offset(), metadata.timestamp());
            } else {
                log.error("exception error from broker: {}", exception.getMessage());
            }
        }));
    }
}
