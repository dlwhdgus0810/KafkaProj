package com.practice.kafka.producer;

import com.practice.kafka.event.EventHandler;
import com.practice.kafka.event.FileEventHandler;
import com.practice.kafka.event.FileEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

public class FileAppendProducer {

    private static final Logger log = LoggerFactory.getLogger(FileAppendProducer.class.getName());

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
        File file = new File("practice/src/main/resources/pizza_append.txt");
        EventHandler eventHandler = new FileEventHandler(kafkaProducer, topicName, true);
        FileEventSource fileEventSource = new FileEventSource(0, file, eventHandler);
        Thread fileEventSourceThread = new Thread(fileEventSource);
        fileEventSourceThread.start();

//        try {
//            fileEventSourceThread.join();
//            log.info("shutdown");
//        } catch (InterruptedException e) {
//            log.error(e.getMessage());
//        } finally {
//            log.error("producer is closing");
//            kafkaProducer.close();
//        }
    }
}
