package com.nikapp.kafka.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * This class is the Kafka Producer which puts data onto Kafka topic. 
 *
 ** @author nikhil.bhide
 * @since 1.0
 *
 */
public class KafkaDataProducer {
	private Producer<String, String> kafkaProducer;
	/**
	 * Creates Kafka Producer based on the properties set.
	 * The properties should be configured in properties file; however, over here properties are hardcoded since it is a demonstration project. 
	 * 
	 */
	private void init (){
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		List<Future<RecordMetadata>> ackList = new ArrayList();
		kafkaProducer = new KafkaProducer(props);		
	}

	/**
	 * Produces data on kafka topic based on the number of messages.
	 * Take a note that the order in which the messages are put is not fixed as messages are produced in parallel
	 *
	*/	
	public void produceData(int totalMessagesToSend) {
		init();
		IntStream.rangeClosed(1, totalMessagesToSend)
		.parallel()
		.forEach(i-> {
			System.out.println("Sending message on Kafka topic");
			kafkaProducer.send(new ProducerRecord<String, String>("Kafka-Topic", "This is message number ".concat(String.valueOf(i))));
		});
	}
}