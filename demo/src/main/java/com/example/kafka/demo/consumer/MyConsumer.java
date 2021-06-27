package com.example.kafka.demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class MyConsumer implements Closeable {

    private String topic;
    private KafkaConsumer<String, String> consumer = setConsumer();

    public MyConsumer(String topic) {
        this.topic = topic;
    }

    public KafkaConsumer<String, String> setConsumer() {
        try {
            TimeUnit.SECONDS.sleep(10);
        }catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
        }
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "groupId");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer(properties);
        kafkaConsumer.subscribe(List.of(topic));
        return kafkaConsumer;
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }

}
