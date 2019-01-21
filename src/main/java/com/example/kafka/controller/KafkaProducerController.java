package com.example.kafka.controller;



import com.example.kafka.constant.TopicEnum;
import com.example.kafka.producer.TopicProducer;
import com.example.kafka.service.EntityProcessorService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;


// Accessing by http://localhost:8084/swagger-ui.html#

@RestController
public class KafkaProducerController {
    
    @Autowired
    private EntityProcessorService entityProcessor;
    
    @Autowired
    private TopicProducer topicProducer;
    
    @PostMapping(value= "/send-to-kafka", consumes = {MediaType.APPLICATION_JSON_VALUE})
    public String sendAllTopicsToKafka(@RequestBody String json) {
        try {
            return topicProducer.produce(json); 
        } catch(RuntimeException e) {
            return e.getMessage();
        }
    }
    
    @RequestMapping(value = "/get-topics/{codigo}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public String getAllTopics(@PathVariable Integer codigo) {
        return entityProcessor.getJsonCollectionsString(codigo);
    }
    
    @RequestMapping(value = "/get-topic/{codigo}/{topico}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public String getTopic(@PathVariable Integer codigo, TopicEnum topico) {
        return entityProcessor.getJsonCollectionString(codigo, topico.name(), "dev_");
    }
}
