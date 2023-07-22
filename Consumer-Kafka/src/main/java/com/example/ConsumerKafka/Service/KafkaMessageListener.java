package com.example.ConsumerKafka.Service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

@Service
public class KafkaMessageListener {

    @Autowired
    private EmailSenderService emailSenderService;

    Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);

    @KafkaListener(topics = "test", groupId = "jt-group-new")
    public void consume1(ConsumerRecord<String, String> record) {
        String key = record.key();
        String message = record.value();
        int partition = record.partition();
        long offset = record.offset();

        log.info("Consumer1 received the message with key=[{}] and value=[{}] from partition=[{}] with offset=[{}]",
                key, message, partition, offset);
        emailSenderService.sendSimpleEmail("alcontinebenezer07@gmail.com",key,message);
    }

    @KafkaListener(topics = "test", groupId = "jt-group-new")
    public void consume2(ConsumerRecord<String, String> record) {
        String key = record.key();
        String message = record.value();
        int partition = record.partition();
        long offset = record.offset();

        log.info("Consumer2 received the message with key=[{}] and value=[{}] from partition=[{}] with offset=[{}]",
                key, message, partition, offset);
        emailSenderService.sendSimpleEmail("alcontinebenezer07@gmail.com",key,message);
    }


    @KafkaListener(topics = "test", groupId = "jt-group-new")
    public void consume3(ConsumerRecord<String, String> record) {
        String key = record.key();
        String message = record.value();
        int partition = record.partition();
        long offset = record.offset();

        log.info("Consumer3 received the message with key=[{}] and value=[{}] from partition=[{}] with offset=[{}]",
                key, message, partition, offset);
        emailSenderService.sendSimpleEmail("alcontinebenezer07@gmail.com","Notification",message);
    }
}


