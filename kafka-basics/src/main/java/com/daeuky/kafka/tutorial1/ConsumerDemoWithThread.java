package com.daeuky.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
	private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

	public static void main(String[] args) {
		new ConsumerDemoWithThread().run();
	}

	private ConsumerDemoWithThread() {

	}

	private void run() {
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "my-six-application";
		String topic = "third_topic";

		// create the consumer runnable
		CountDownLatch latch = new CountDownLatch(1);
		Runnable myConsumerThread = new ConsumerThread(latch, topic, bootstrapServers, groupId);

		// start the thread
		Thread myThread = new Thread(myConsumerThread);
		myThread.start();

		// add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Caught shutdown hook");
			((ConsumerThread) myConsumerThread).shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			logger.info("Application has exited");
		}));

		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Application got interrupted", e);
		} finally {
			logger.info("Application is closing");
		}
	}

	public class ConsumerThread implements Runnable {

		private KafkaConsumer<String, String> consumer;
		private CountDownLatch latch;

		public ConsumerThread(CountDownLatch latch, String topic, String bootstrapServers, String groupId) {
			this.latch = latch;

			// create consumer configs
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			// create consumer
			consumer = new KafkaConsumer<String, String>(properties);

			// subscribe consumer to our topic(s)
			consumer.subscribe(Collections.singleton(topic));
		}

		@Override
		public void run() {
			try {
				// poll for new data
				while(true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));// new in Kafka 2.0.0

					for (ConsumerRecord<String, String> record : records) {
						logger.info("Key: " + record.key() + ", Value: " + record.value());
						logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
					}
				}
			} catch (WakeupException e) {
				logger.info("Received shutdown signal!");
			} finally {
				consumer.close();
				// tell our main code we're down with the consumer
				latch.countDown();
			}
		}

		public void shutdown() {
			// the wakeup() method is a special method to interrupt consumer.poll()
			// it will throw the exception WakeUpException
			consumer.wakeup();
		}
	}
}
