package org.homework.dataproxyservice.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaMessageService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String topic, String message) {
        kafkaTemplate.send(topic, message);
    }
}

