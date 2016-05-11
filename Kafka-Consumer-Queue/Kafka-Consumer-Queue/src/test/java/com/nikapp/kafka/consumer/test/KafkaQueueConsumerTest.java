package com.nikapp.kafka.consumer.test;

import static org.junit.Assert.*;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import com.nikapp.kafka.consumer.KafkaQueueConsumer;
import com.nikapp.kafka.producer.KafkaDataProducer;

public class KafkaQueueConsumerTest {
	@Test
	public void testQueueConsumer() throws InterruptedException, ExecutionException {
		KafkaDataProducer producer = new KafkaDataProducer();
		KafkaQueueConsumer consumer = new KafkaQueueConsumer(2,1000);
		consumer.start(10);
		producer.produceData(1);
		assertEquals(consumer.start(10),1);
	}
}