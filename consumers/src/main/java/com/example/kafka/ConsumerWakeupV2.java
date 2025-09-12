package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerWakeupV2 {

    private static final Logger log = LoggerFactory.getLogger(ConsumerWakeupV2.class.getName());

    public static void main(String[] args) {

//        String topicName = "simple-topic";
        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_01");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_02");
        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");

        Thread mainThread = Thread.currentThread();

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown detected. Calling consumer.wakeup()");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                }
            }));

            kafkaConsumer.subscribe(List.of(topicName));

            int loopCnt = 0;

            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

                log.info("[loopCnt: {}, consumerRecords count: {}]", loopCnt++, consumerRecords.count());

                for (ConsumerRecord<String, String> record : consumerRecords) {
                    log.info("record key: {}, partition: {}, record offset: {}, record value: {}",
                            record.key(), record.partition(), record.offset(), record.value());
                }

                sleep(loopCnt, 3000);
            }
        } catch (WakeupException e) {
            log.error(e.getMessage());
        }
    }

    private static void sleep(int loopCnt, long millis) {
        try {
            log.info("main thread is sleeping {} ms during while loop", loopCnt * millis);
            Thread.sleep(loopCnt * millis);
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        }
    }
}
