package com.training.kafka.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;

@SpringBootApplication
public class KafkaApplication {

	private static final Logger log = LoggerFactory.getLogger(KafkaApplication.class);

	@KafkaListener(
		topicPartitions= @TopicPartition(topic="clog10-topic", partitions= {"0", "1", "2", "3", "4"}),
		groupId="clog10-group"
	)
	public void listen(String message){
		log.info("Received Messasge in group clog10-group: " + message);
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}

}