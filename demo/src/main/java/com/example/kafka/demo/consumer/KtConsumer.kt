package com.example.kafka.demo.consumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.boot.context.properties.bind.Bindable.listOf
import java.io.Closeable
import java.time.Duration
import java.util.*
import java.util.function.Consumer
import kotlin.concurrent.thread

class KtConsumer(private val topic: String) :Closeable {

    private val consumer = getConsumer();

    private fun getConsumer() : KafkaConsumer<String, String> {
        val props = Properties()
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "groupId")
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.toString())
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.toString())
        val consumer = KafkaConsumer<String, String>(props)
        val list:List<String> = listOf(topic)
        consumer.subscribe(list)
        return consumer
    }

    fun consume(recordConsumer: Consumer<ConsumerRecord<String, String>>) {
        thread {
            while (true) {
                val records = consumer.poll(Duration.ofSeconds(1))
                records.forEach{record -> recordConsumer.accept(record)}
            }
        }
    }

    override fun close() {
        consumer.close()
    }

}