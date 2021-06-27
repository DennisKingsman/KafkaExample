package com.example.kafka.demo;

import com.example.kafka.demo.consumer.KtConsumer;
import com.example.kafka.demo.consumer.MyConsumer;
import com.example.kafka.demo.producer.MyProducer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@SpringBootApplication
public class KafkaSpringApplication {

	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(KafkaSpringApplication.class, args);
//use JVM as a PRODUCER and catch message in console consumer
		String topic = "spring-demo";
		MyProducer producer = new MyProducer(topic);
		for (int i = 0; i < 100; i++) {
			producer.send(i + " ", "MyProducer message " + i);
			System.out.println("here");
			TimeUnit.SECONDS.sleep(3);
		}


		KafkaConsumer<String, String> consumer = new MyConsumer(topic).setConsumer();
		System.out.println("set consumer past");
		final int giveUp = 100;
		int noRecordsCount = 0;
		//todo method
		while (true) {
			final ConsumerRecords<String, String> consumerRecords =
					consumer.poll(Duration.ofSeconds(1));
			if (consumerRecords.count()==0) {
				noRecordsCount++;
				if (noRecordsCount > giveUp) break;
				else continue;
			}
			consumerRecords.forEach(record -> {
				System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
						record.key(), record.value(),
						record.partition(), record.offset());
			});
			consumer.commitAsync();
		}
		TimeUnit.SECONDS.sleep(10);
		try {
			producer.close();
			consumer.close();
		}catch (IOException ex) {
			ex.printStackTrace();
		}
	}

}
