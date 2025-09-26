package com.training.kafka.kafka;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class KafkaApplication implements CommandLineRunner {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	private static final Logger log = LoggerFactory.getLogger(KafkaApplication.class);

	@KafkaListener(id="clog10", autoStartup="false", topicPartitions = @TopicPartition(topic = "clog10-topic", partitions = { "0", "1", "2", "3",
			"4" }), containerFactory = "kafkaListenerContainerFactory", groupId = "clog10-group", properties = {
					"max.poll.interval.ms:4000",
					"max.poll.records:10" })
	public void listen(List<ConsumerRecord<String, String>> messages) {
		log.info("Starting a new batch of messages");
		for (ConsumerRecord<String, String> message : messages) {
			log.info("Received Messasge, Partition = {}, Offset = {}, Key={}, Value={}", message.partition(),
					message.offset(), message.key(), message.value());
		}
		log.info("Batch of messages completed");
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

		for (int i = 0; i < 100; i++) {
			kafkaTemplate.send("clog10-topic", String.valueOf(i),
					"Hello World from Spring Kafka! " + i);
		}
		log.info("Waiting to start");
		Thread.sleep(5000);
				log.info("Starting");
		registry.getListenerContainer("clog10").start();
		Thread.sleep(5000);
		registry.getListenerContainer("clog10").stop();
	}

}