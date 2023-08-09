package com.johanna.producer;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Service
public class WikimediaChangesProducer {

    @Value("${spring.kafka.topic.name}")
    private String topic;

    private final static Logger LOGGER = LoggerFactory.getLogger(WikimediaChangesProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public WikimediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage() throws InterruptedException {

        BackgroundEventHandler backgroundEventHandler = new WikimediaChangesHandler(
                kafkaTemplate,
                topic
        );
        String uri = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder sourceBuilder = new EventSource.Builder(
                URI.create(uri)
        );
        BackgroundEventSource bes = new BackgroundEventSource.Builder(
                backgroundEventHandler,
                sourceBuilder
        ).build();
        bes.start();
        TimeUnit.MINUTES.sleep(10);

    }

}
