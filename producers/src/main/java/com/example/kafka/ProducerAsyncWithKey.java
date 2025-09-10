package com.example.kafka;

import com.example.util.MyLogger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerAsyncWithKey {

    static MyLogger log = new MyLogger();

    public static void main(String[] args) {
        String topicName = "multipart-topic";

        // KafkaProducer config setting
        Properties props = new Properties();

        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer object creation
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);

        for (int seq = 0; seq < 20; seq++) {
            // ProducerRecord object creation
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, seq, "hello world: " + seq);

            // producer send message
            kafkaProducer.send(producerRecord, (metadata, e) -> {
                if (e == null) {
                    log.info(metadata);
                } else {
                    log.error("exception error from broker", e);
                }
            });
        }

        kafkaProducer.close();
    }


}
