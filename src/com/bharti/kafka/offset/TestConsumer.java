/**
 * 
 */
package com.bharti.kafka.offset;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * @author Madhukar
 *
 */
public class TestConsumer {

	
	private static ConsumerConfig createConsumerConfig(String group){
		Properties props = new Properties();
		props.put("zookeeper.connect", "localhost:2181");
		props.put("group.id", group);
		props.put("zookeeper.sessiontimeout.ms", "6000");
		props.put("zookeeper.connection.timeout.ms", "12000");
		props.put("zookeeper.synctime.ms", "200");
	
		props.put("auto.offset.reset", "smallest");
		props.put("auto.commit.enable", "false");
		props.put("offsets.storage", "kafka");
		props.put("dual.commit.enabled", "false");
		return new ConsumerConfig(props);
	}
	
	private static void startConsuming(String topic, String groupId){
		ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(groupId));
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream =  consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		System.out.println("Hi");		
		while(it.hasNext()){
			MessageAndMetadata<byte[], byte[]> md = it.next();
			byte[] arr = (byte[]) md.message();
			OffsetHandler.commitOffset("localhost", 9092, groupId, topic, md.partition(), md.offset());
			System.out.println("Received :::" + new String(arr));		
			
		}
	}
	
	public static void main(String[] args) {
		String topic = "TestQ";
		startConsuming(topic, "TestConsumerGroup1");
	}

}
