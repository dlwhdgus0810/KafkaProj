package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerPartitionAssignSeek {

    private static final Logger log = LoggerFactory.getLogger(ConsumerPartitionAssignSeek.class.getName());

    private static KafkaConsumer<String, String> createConsumer(String groudId) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groudId);
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(props);
    }

    public static void main(String[] args) {
        String topicName = "pizza-topic";

        KafkaConsumer<String, String> kafkaConsumer = createConsumer("group_pizza_assign_seek_v001");
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
//        kafkaConsumer.subscribe(List.of(topicName));
        kafkaConsumer.assign(List.of(topicPartition));
        kafkaConsumer.seek(topicPartition, 10L);

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown detected. Calling consumer.wakeup()");
            kafkaConsumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                log.error(e.getMessage());
            }
        }));


//        pollAutoCommit(kafkaConsumer);
        pollCommitSync(kafkaConsumer);
//        pollCommitAsync(kafkaConsumer);
    }

    private static void pollCommitAsync(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCnt = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                log.info("[loopCnt: {}, consumerRecords count: {}]", loopCnt++, consumerRecords.count());

                for (ConsumerRecord<String, String> record : consumerRecords) {
                    log.info("[record offset: {}, record key: {}, partition: {}, record value: {}]",
                            record.offset(), record.key(), record.partition(), record.value());
                }

                kafkaConsumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        log.error("offsets: {} is not completed, error: {}", offsets, exception.getMessage());
                    }
                });
            }
        } catch (WakeupException e) {
            log.error(e.getMessage());
        } finally {
            log.info("[commit sync before closing]");
            kafkaConsumer.commitSync();
            log.info("consumer is closing");
            kafkaConsumer.close();
        }
    }

    private static void pollCommitSync(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCnt = 0;
        try (kafkaConsumer) {
            while (true) {
                int recordCnt = pollRecords(kafkaConsumer, loopCnt);
                loopCnt++;

                try {
                    if (recordCnt > 0) kafkaConsumer.commitSync();
                    log.info("commit sync has been completed.");
                } catch (CommitFailedException e) {
                    log.error(e.getMessage());
                }
            }
        } catch (WakeupException e) {
            log.error(e.getMessage());
        } finally {
            log.info("consumer is closing");
        }
    }

    private static void pollAutoCommit(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCnt = 0;

        try (kafkaConsumer) {
            while (true) {
                int recordCnt = pollRecords(kafkaConsumer, loopCnt);

                loopCnt++;

                sleep(loopCnt, 10000);
            }
        } catch (WakeupException e) {
            log.error(e.getMessage());
        } finally {
            log.info("consumer is closing");
        }
    }

    private static int pollRecords(KafkaConsumer<String, String> kafkaConsumer, int loopCnt) {
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
        log.info("[loopCnt: {}, consumerRecords count: {}]", loopCnt, consumerRecords.count());

        for (ConsumerRecord<String, String> record : consumerRecords) {
            log.info("[record offset: {}, record key: {}, partition: {}, record value: {}]",
                    record.offset(), record.key(), record.partition(), record.value());
        }

        return consumerRecords.count();
    }

    private static void sleep(int loopCnt, long millis) {
        try {
            log.info("main thread is sleeping {} ms during while loop", millis);
            Thread.sleep(loopCnt * millis);
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        }
    }
}
