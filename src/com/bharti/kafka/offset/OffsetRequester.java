/**
 * 
 */
package com.bharti.kafka.offset;

/**
 * @author Madhukar
 *
 */



import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;


public class OffsetRequester {

		
	private static final Logger LOGGER = Logger.getLogger(OffsetRequester.class.getName());
	
	public static void main(String[] args) {
		SimpleConsumer consumer = new SimpleConsumer("localhost", 9092, 100000, 64 * 1024, "request_fetcher");
		String topic = "TestQ";
		int partition = 0;
		long whichTime = kafka.api.OffsetRequest.LatestTime();
		System.out.println("Max Offset::"+ readLastOffset(consumer, topic, partition, whichTime));
		
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		List<TopicAndPartition> offReq = new ArrayList<TopicAndPartition>();
		offReq.add(topicAndPartition);
		OffsetFetchRequest req = new OffsetFetchRequest("TestGroup5", offReq,(short) 1, 0, "offset_request_fetcher");		
		OffsetFetchResponse resp = consumer.fetchOffsets(req);
		if(resp.offsets().isEmpty()){
			System.out.println("Nothing is there");
		}else{
			System.out.println("Offset Commited(__consumer_offsets)::"+resp.offsets().get(topicAndPartition).offset());
		}
	}
	
	public static long readLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime) {
	        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
	        requestInfo.put(new TopicAndPartition(topic, partition), new PartitionOffsetRequestInfo(whichTime, 1));
	        OffsetResponse response = consumer.getOffsetsBefore(new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId()));
	        if (response.hasError()) {
	            LOGGER.log(Level.INFO, "Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
	            return 0;
	        }
	        long[] offsets = response.offsets(topic, partition);
	        return offsets[0];
    	}
}
