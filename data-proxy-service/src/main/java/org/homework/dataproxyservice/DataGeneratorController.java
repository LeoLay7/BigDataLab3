package org.homework.dataproxyservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DataGeneratorController {

    private static final Logger LOG = LoggerFactory.getLogger(DataGeneratorController.class);

    private final DataProxySenderService senderService;

    public DataGeneratorController(DataProxySenderService senderService) {
        this.senderService = senderService;
    }

    @PostMapping("/generate-data")
    public ResponseEntity<String> generateData(@RequestParam(name = "topic", required = false) String topic) {
        LOG.info("Received /generate-data request, topic override='{}'", topic);
        try {
            long count = senderService.publishAll(topic);
            String effectiveTopic = (topic == null || topic.isBlank()) ? "mock-data" : topic;
            LOG.info("Successfully published {} messages to topic='{}'", count, effectiveTopic);
            return ResponseEntity.ok("Published " + count + " messages to topic '" + effectiveTopic + "'");
        } catch (JsonProcessingException e) {
            LOG.error("JSON serialization failure while publishing", e);
            return ResponseEntity.status(500).body("Failed to serialize row to JSON: " + e.getMessage());
        } catch (Exception e) {
            LOG.error("Unhandled error while publishing data", e);
            return ResponseEntity.status(500).body("Error while publishing messages: " + e.getMessage());
        }
    }
}

