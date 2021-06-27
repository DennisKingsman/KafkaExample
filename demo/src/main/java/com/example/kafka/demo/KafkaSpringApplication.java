package com.example.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;
import java.util.concurrent.Future;

@SpringBootApplication
public class KafkaSpringApplication {

	public static void main(String[] args) {
//		SpringApplication.run(KafkaSpringApplication.class, args);
//use JVM as a PRODUCER and catch message in console consumer
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "clientId");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		String topic = "spring-demo";
		Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, "Hello world"));
		try {
			future.get();
		}catch (Exception ex) {
			//todo bad handler
			ex.printStackTrace();
		}

		ProducerConfig producerConfig = new ProducerConfig(properties);
	}

}
