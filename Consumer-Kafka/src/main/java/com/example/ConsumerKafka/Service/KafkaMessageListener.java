package com.example.ConsumerKafka.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

@Service
public class KafkaMessageListener {

    Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);

    @KafkaListener(topics = "test", groupId = "jt-group-new")
    public void consume1(String message, @Header("kafka_receivedMessageKey") String key) {
        log.info("Consumer1 consumed the message with key=[{}] and value=[{}]", key, message);
    }

    @KafkaListener(topics = "test", groupId = "jt-group-new")
    public void consume2(String message, @Header("kafka_receivedMessageKey") String key) {
        log.info("Consumer2 consumed the message with key=[{}] and value=[{}]", key, message);
    }


    @KafkaListener(topics = "test", groupId = "jt-group-new")
    public void consume3(String message, @Header("kafka_receivedMessageKey") String key) {
        log.info("Consumer3 consumed the message with key=[{}] and value=[{}]", key, message);
    }

//    @KafkaListener(topics = "javatechie-demo1",groupId = "jt-group-new")
//    public void consume4(String message) {
//        log.info("consumer4 consume the message {} ", message);
//    }
}
