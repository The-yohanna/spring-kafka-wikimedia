package com.johanna.producer;

import com.johanna.producer.model.WikimediaData;
import com.johanna.producer.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class WikimediaKafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WikimediaKafkaConsumer.class);
    private final WikimediaDataRepository wikimediaDataRepository;

    public WikimediaKafkaConsumer(WikimediaDataRepository wikimediaDataRepository) {
        this.wikimediaDataRepository = wikimediaDataRepository;
    }

    @KafkaListener(
            topics = "${spring.kafka.topic.name}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consumer(String eventMessage) {

        LOGGER.info(String.format("Message received -> %s",eventMessage));

        WikimediaData wikimediaData = new WikimediaData();
        wikimediaData.setWikiEventData(eventMessage);
        wikimediaDataRepository.save(wikimediaData);

    }

}
