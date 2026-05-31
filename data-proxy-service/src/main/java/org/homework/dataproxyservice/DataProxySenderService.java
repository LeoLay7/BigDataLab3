package org.homework.dataproxyservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.homework.dataproxyservice.repository.MockDataRepository;
import org.homework.dataproxyservice.service.KafkaMessageService;

import java.util.List;
import java.util.Map;

@Service
public class DataProxySenderService {

    private static final Logger LOG = LoggerFactory.getLogger(DataProxySenderService.class);

    private final MockDataRepository mockDataRepository;
    private final KafkaMessageService kafkaMessageService;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topic:mock-data}")
    private String defaultTopic;

    @Value("${app.kafka.log.every-n-messages:500}")
    private int logEveryNMessages;

    public DataProxySenderService(MockDataRepository mockDataRepository, KafkaMessageService kafkaMessageService) {
        this.mockDataRepository = mockDataRepository;
        this.kafkaMessageService = kafkaMessageService;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Читает всю таблицу mock_data и отправляет каждую строку как JSON в Kafka.
     * Возвращает количество отправленных сообщений.
     */
    public long publishAll(String topic) throws JsonProcessingException {
        String target = (topic == null || topic.isBlank()) ? defaultTopic : topic;
        LOG.info("Starting publish from table mock_data to Kafka topic='{}'", target);

        List<Map<String, Object>> rows = mockDataRepository.findAllRows();
        LOG.info("Loaded {} rows from PostgreSQL table mock_data", rows.size());

        long sent = 0;
        for (Map<String, Object> row : rows) {
            String json = objectMapper.writeValueAsString(row);
            kafkaMessageService.send(target, json);

            sent++;
            if (sent == 1 || sent % Math.max(1, logEveryNMessages) == 0 || sent == rows.size()) {
                LOG.info("Kafka publish progress: {}/{} messages sent to topic='{}'", sent, rows.size(), target);
            }
        }

        LOG.info("Finished publish to topic='{}': {} messages", target, sent);
        return sent;
    }
}

