package com.example.kafka.demo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

public class MyProducer implements Closeable {

    private String topic;
    private KafkaProducer<String, String> producer = setProducer();

    public MyProducer(String topic) {
        this.topic = topic;
    }

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }

    public KafkaProducer<String, String> setProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "clientId");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return new KafkaProducer(properties);
    }

    public void send(String key, String val) {
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, key, val));
        try {
            future.get();
        }catch (Exception ex) {
            //todo bad handler
            ex.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }

}
