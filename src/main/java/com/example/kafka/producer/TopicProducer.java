package com.example.kafka.producer;

import java.util.LinkedHashMap;
import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import com.example.kafka.service.EntityProcessorService;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

@Component
public class TopicProducer {

    @Autowired
    protected KafkaTemplate<String, String> template;

    @Value("${kafka.topic-delay}")
    private Integer delay;

    @Value("${kafka.topic-prefix}")
    private String topicPrefix;

    @Autowired
    protected EntityProcessorService processor;

    public String produce(String jsonArrayString) throws RuntimeException {

        final Gson gson = new Gson();

        final JsonParser jsonParser = new JsonParser();

        final JsonArray jsonArray = (JsonArray) jsonParser.parse(jsonArrayString);

        jsonArray.forEach(jsonObejct -> {
            LinkedHashMap hashMap = gson.fromJson(jsonObejct, LinkedHashMap.class);
            hashMap.forEach(this::sendMessage);
        });

        return "Topics has sent to Kafka!";
    }

    private void sendMessage(Object topic, Object payload) throws RuntimeException {

        try {

            Thread.sleep(delay);

            final String topicName = topicPrefix.concat(topic.toString().toLowerCase());

            final Message<String> message = MessageBuilder
                    .withPayload(new Gson().toJson(payload))
                    .setHeader(KafkaHeaders.TOPIC, topicName)
                    .build();

            final ListenableFuture<SendResult<String, String>> future = template.send(message);

            try {
                future.get();
            } catch (ExecutionException ex) {
                throw new RuntimeException("Kafka has failed!");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Thread has failed!");
        }
    }
}
