package main;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TweetsProducer {
	
	private static final String TOPIC = "twitter";
	
	/**
	 * Read user IDs to follow in Streaming API from file in <path>
	 */
	private static ArrayList<Long> getUserIds(String path) {
		ArrayList<Long> users = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
			String line;
			while ((line = br.readLine()) != null) {
				users.add(Long.parseLong(line.split(" ")[1]));
				line = br.readLine();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
		return users;
	}
	
	public static void main(String[] args) {
		/* Keeps messages in memory between Twitter API and Kafka broker */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);
		
		/* Connection to Twitter API endpoint */
		Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		List<Long> followings = getUserIds("src/main/resources/polit_to_id.txt");
		endpoint.followings(followings);
		
		/* Authentication */
		Properties twProps = new Properties();
		Authentication auth = null;
		try {
			String propsFile = "src/main/resources/config.properties";
			InputStream propsIS = new FileInputStream(propsFile);
			twProps.load(propsIS);
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
			System.exit(1);
		} catch (IOException e) {
			System.out.println(e.getMessage());
			System.exit(1);
		}
		auth = new OAuth1(twProps.getProperty("ConsumerKey"),
				twProps.getProperty("ConsumerSecret"),
				twProps.getProperty("AccessToken"),
				twProps.getProperty("AccessTokenSecret"));
		
		/* Connect to API */
		ClientBuilder builder = new ClientBuilder()
				.name("Tweets Client")
				.hosts(hosts)
				.authentication(auth)
				.endpoint(endpoint)
				.processor(new StringDelimitedProcessor(msgQueue))
				.eventMessageQueue(eventQueue);
		Client client = builder.build();
		client.connect();
		
		/* Create Kafka producer */
		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", "localhost:9092");
		kafkaProps.put("acks", "all");
		kafkaProps.put("retries", 0);
		kafkaProps.put("batch.size", 16384);
		kafkaProps.put("linger.ms", 1);
		kafkaProps.put("buffer.memory", 33554432);
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String,String> producer = new KafkaProducer<>(kafkaProps);
		
		while (!client.isDone()) {
			try {
				/* Pick tweets from queue to Kafka */
				String tweet = msgQueue.take();
				System.out.println(tweet);
				
				ProducerRecord<String, String> kTweet = new ProducerRecord<>(TOPIC, tweet);
				producer.send(kTweet);
			} catch (InterruptedException e) {
				System.out.println("Interrupted blocking read from queue.");
			}
		}
		
		/* Close connectors */
		producer.close();
		client.stop();
		System.out.println("Closed connection.");
	}
}
