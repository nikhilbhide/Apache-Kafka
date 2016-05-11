package com.nikapp.kafka.producer.test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import com.nikapp.kafka.producer.KafkaDataProducer;

public class KafkaProducerTest {
	private static KafkaConsumer<String, String> consumer;
	@BeforeClass
	public static void setUp() throws IOException {		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "latest");
		consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("Kafka-Topic"));
		//just for Kafka broker to mark the 
		ConsumerRecords<String,String> records = consumer.poll(1000);
	}
	
	@Test
	public void testProducer(){
		KafkaDataProducer producer = new KafkaDataProducer();
		producer.produceData(1000);
		ConsumerRecords<String,String> records = consumer.poll(1000);
		assertTrue(records.count()==1000);
	}
	
	@AfterClass
	public static void tearDown() {
		consumer.close();
	}
}