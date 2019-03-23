package com.daeuky.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

	public static RestHighLevelClient createClient() {
		RestClientBuilder builder = RestClient.builder(
			new HttpHost("localhost", 9200, "http"));

		return new RestHighLevelClient(builder);
	}

	public static KafkaConsumer<String, String> createConsumer(String topic) {
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "kafka-demo-elasticsearch";

		// create consumer configs
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10"); // disable auto commit of offsets

		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList(topic));

		return consumer;
	}

	public static void main(String[] args) throws IOException {
		Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);
		RestHighLevelClient client = createClient();

		KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));// new in Kafka 2.0.0

			logger.info("Received " + records.count() + " records");
			for (ConsumerRecord<String, String> record : records) {
				// 2 strategies
				// kafka generic id
//				String id = record.topic() + "_" + record.partition() + "_" + record.offset();

				// twitter feed specific id
				String id = extractIdFromTweet(record.value());

				IndexRequest indexRequest = new IndexRequest(
					"twitter",
					"tweets",
					id // this is to make our consumer idempotent
				).source(record.value(), XContentType.JSON);

				IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
				logger.info(indexResponse.getId());
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			logger.info("Committing offsets...");
			consumer.commitSync();
			logger.info("Offsets have benn committed");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

//		client.close();
	}

	private static JsonParser jsonParser = new JsonParser();
	private static String extractIdFromTweet(String tweetJson) {
		// gson library
		return jsonParser.parse(tweetJson)
			.getAsJsonObject()
			.get("id_str")
			.getAsString();
	}
}
