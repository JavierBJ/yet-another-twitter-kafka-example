package main;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class TweetsConsumer {
	
	private static final String TOPIC = "twitter";
	private static final String DB = "raw_tweets";
	private static final String MONGO_COLL = "tweets";
	
	public static void main(String[] args) {
		/* Access MongoDB to store tweets consumed */
		MongoClient mongo = MongoClients.create();
		MongoDatabase db = mongo.getDatabase(DB);
		MongoCollection<Document> collection = db.getCollection(MONGO_COLL);
		
		/* Create Kafka consumer to get tweets downloaded */
		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", "localhost:9092");
		kafkaProps.put("group.id", "twitter");
		kafkaProps.put("enable.auto.commit", "true");
		kafkaProps.put("auto.commit.interval.ms", "1000");
		kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps);
		consumer.subscribe(Arrays.asList(TOPIC));
		
		/* Consume tweets and write to MongoDB */
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				List<Document> documents = new ArrayList<>();
				for (ConsumerRecord<String, String> record : records) {
					System.out.println("Offset: " + record.offset() + ", Key: " + record.key() + ", Value: " + record.value());
					Document bson = Document.parse(record.value());
					documents.add(bson);
				}
				if (!documents.isEmpty()) {
					collection.insertMany(documents);
				}
			}
		} finally {
			consumer.close();
			mongo.close();
		}
	}
}
