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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import kafka.javaapi.ConsumerMetadataResponse;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.network.BlockingChannel;
import kafka.api.ConsumerMetadataRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;


public class OffsetHandler {

		
	private static final Logger LOGGER = Logger.getLogger(OffsetHandler.class.getName());	
	
	public static long getConsumedOffset(String groupId) {
		long offset = -1;

		BlockingChannel channel = new BlockingChannel("localhost", 9092,
				BlockingChannel.UseDefaultBufferSize(),
				BlockingChannel.UseDefaultBufferSize(), 5000 /*
															 * read timeout in
															 * millis
															 */);
		channel.connect();
		channel.send(new ConsumerMetadataRequest(groupId, (short) 1, 0,
				"cm-requester"));

		ConsumerMetadataResponse cmr = ConsumerMetadataResponse
				.readFrom(channel.receive().buffer());

		Broker broker = null;
		if (cmr.errorCode() == ErrorMapping.NoError()) {
			System.out
					.println("No Error while getting consumer metadata response");
			broker = cmr.coordinator();
		}
		if (broker == null) {
			System.out.println("No coordinator broker available");
			return -1;
		}
		channel.disconnect();
		
		channel = new BlockingChannel(broker.host(), broker.port(), BlockingChannel.UseDefaultBufferSize(), BlockingChannel.UseDefaultBufferSize(), 5000);
		channel.connect();

		TopicAndPartition part0 = new TopicAndPartition("TestQ", 0);
		List<TopicAndPartition> reqInfo = new ArrayList<TopicAndPartition>();
		reqInfo.add(part0);

		OffsetFetchRequest oreq = new OffsetFetchRequest(groupId, reqInfo, (short) 1, 0, "offsetRequester");
		channel.send(oreq.underlying());

		OffsetFetchResponse resp = OffsetFetchResponse.readFrom(channel.receive().buffer());
		OffsetMetadataAndError ome = resp.offsets().get(part0);
		if(ome.error() == ErrorMapping.NoError()){
			offset = ome.offset();
			System.out.println("Offset::" + resp.offsets().get(part0).offset());
		}else{
			System.out.println("Error code is::"+ome.error());
			offset = -1;
		}
		return offset;
	}
	
	public static int commitOffset(String brokerConnect, int port, String groupId, String topic, int partition, long offset) {
		
		TopicAndPartition partitionInfo = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, OffsetAndMetadata> offsetMap = new LinkedHashMap<TopicAndPartition, OffsetAndMetadata>();
        
		BlockingChannel channel = new BlockingChannel(brokerConnect, port,
				BlockingChannel.UseDefaultBufferSize(),
				BlockingChannel.UseDefaultBufferSize(), 5000);
		channel.connect();
		
		channel.send(new ConsumerMetadataRequest(groupId, (short) 1, 0, "cm-requester"));

		ConsumerMetadataResponse cmr = ConsumerMetadataResponse.readFrom(channel.receive().buffer());

		Broker broker = null;
		if (cmr.errorCode() == ErrorMapping.NoError()) {			
			broker = cmr.coordinator();
		}
		if (broker == null) {
			System.out.println("No coordinator broker available");
			
		}
		
		channel.disconnect();
		
		channel = new BlockingChannel(broker.host(), broker.port(), BlockingChannel.UseDefaultBufferSize(), BlockingChannel.UseDefaultBufferSize(), 5000);
		channel.connect();

		long currentTime = System.currentTimeMillis();
        offsetMap.put(partitionInfo, new OffsetAndMetadata(offset, "commiting-"+offset, currentTime));
        
		OffsetCommitRequest commitReq = new OffsetCommitRequest(groupId, offsetMap, 0, "offset-commit-client", (short)1);
		
		channel.send(commitReq.underlying());
		
		OffsetCommitResponse commitResp = OffsetCommitResponse.readFrom(channel.receive().buffer());
		
		if(commitResp.errorCode(partitionInfo) == ErrorMapping.NoError()){			
			System.out.println("Offset Commited successfully, "+offset);			
		}
		channel.disconnect();
		return commitResp.errorCode(partitionInfo);
	}
	
	public static void main(String[] args) {
		/*String brokerConnect = "localhost";
		int port = 9092;
		String topic = "TestQ";
		String groupId = "TestGroup5";*/		
		getConsumedOffset("TestConsumerGroup1");
	}
	
}
