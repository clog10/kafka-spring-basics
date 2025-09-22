package com.training.kafka.kafka;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

@SpringBootApplication
public class KafkaApplication implements CommandLineRunner {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	private static final Logger log = LoggerFactory.getLogger(KafkaApplication.class);

	@KafkaListener(topicPartitions = @TopicPartition(topic = "clog10-topic", partitions = { "0", "1", "2", "3",
			"4" }), groupId = "clog10-group")
	public void listen(String message) {
		log.info("Received Messasge in group clog10-group: " + message);
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("clog10-topic",
				"Hello World from Spring Kafka!");

		future.whenComplete((result, ex) -> {
			if (ex == null) {
				log.info("Sent message=[{}] with offset=[{}]",
						"Hello World from Spring Kafka!",
						result.getRecordMetadata().offset());
			} else {
				log.error("Unable to send message=[{}] due to : {}",
						"Hello World from Spring Kafka!",
						ex.getMessage());
			}
		});
	}

}