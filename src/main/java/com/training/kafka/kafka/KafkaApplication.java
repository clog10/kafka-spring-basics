package com.training.kafka.kafka;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;

@SpringBootApplication
public class KafkaApplication {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private MeterRegistry meterRegistry;

	private static final Logger log = LoggerFactory.getLogger(KafkaApplication.class);

	@KafkaListener(id = "clog10", autoStartup = "true", topicPartitions = @TopicPartition(topic = "clog10-topic", partitions = {
			"0", "1", "2", "3",
			"4" }), containerFactory = "kafkaListenerContainerFactory", groupId = "clog10-group", properties = {
					"max.poll.interval.ms:4000",
					"max.poll.records:50" })
	public void listen(List<ConsumerRecord<String, String>> messages) {
		log.info("Starting a new batch of messages");
		for (ConsumerRecord<String, String> message : messages) {
			// log.info("Received Messasge, Partition = {}, Offset = {}, Key={}, Value={}",
			// message.partition(),
			// message.offset(), message.key(), message.value());
		}
		log.info("Batch of messages completed");
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}

	@Scheduled(fixedDelay = 2000, initialDelay = 100)
	public void sendKafkaMessages() {
		log.info("clog10 rules");
		for (int i = 0; i < 200; i++) {
			kafkaTemplate.send("clog10-topic", String.valueOf(i),
					"Hello World from Spring Kafka! " + i);
		}
	}

	@Scheduled(fixedDelay = 2000, initialDelay = 500)
	public void messageCountMetric() {
		List<Meter> metrics = meterRegistry.getMeters();
		for (Meter metric : metrics) {
			log.info("Meter = {}", metric.getId().getName());
		}

		double count = meterRegistry.get("kafka.producer.record.send.total").functionCounter().count();
		log.info("Count {}", count);
	}

}