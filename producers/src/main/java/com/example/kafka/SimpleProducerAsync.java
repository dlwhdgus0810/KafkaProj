package com.example.kafka;

import com.example.util.MyLogger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducerAsync {

    static MyLogger log = new MyLogger();

    public static void main(String[] args) {
        String topicName = "simple-topic";

        // KafkaProducer config setting
        Properties props = new Properties();

        // bootstrap.servers, key.serializer.class, value.serializer.class
//        props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer object creation
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        // ProducerRecord object creation
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "id-001", "hello world from async 2");

        // producer send message
        kafkaProducer.send(producerRecord, (metadata, e) -> {
            if (e == null) {
                log.info(metadata);
            } else {
                log.error("exception error from broker", e);
            }
        });

        kafkaProducer.flush();
        kafkaProducer.close();
    }


}
