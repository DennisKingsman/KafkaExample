package com.example.kafka.demo;

import com.example.kafka.demo.producer.MyProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

@SpringBootApplication
public class KafkaSpringApplication {

	public static void main(String[] args) {
//		SpringApplication.run(KafkaSpringApplication.class, args);
//use JVM as a PRODUCER and catch message in console consumer

		String topic = "spring-demo";
		MyProducer producer = new MyProducer(topic);
		producer.send("key" , "hello with key");
		try {
			producer.close();
		}catch (IOException ex) {
			ex.printStackTrace();
		}
	}

}
