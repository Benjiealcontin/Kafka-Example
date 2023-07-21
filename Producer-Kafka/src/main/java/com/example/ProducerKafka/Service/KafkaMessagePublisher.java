package com.example.ProducerKafka.Service;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String,Object> template;

    public void sendMessageToTopic(String key, String message) {
        CompletableFuture<SendResult<String, Object>> future = template.send("test", key, message);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                RecordMetadata metadata = result.getRecordMetadata();
                System.out.println("Sent message with key=[" + key +
                        "] and value=[" + message +
                        "] to partition=[" + metadata.partition() +
                        "] with offset=[" + metadata.offset() + "]");
            } else {
                System.out.println("Unable to send message with key=[" + key +
                        "] and value=[" + message +
                        "] due to : " + ex.getMessage());
            }
        });
    }


}
