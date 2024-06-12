package org.ebs;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class App 
{
	private static final String PUBLISHER_SPOUT_1_ID = "publisher_spout_1";
	private static final String PUBLISHER_SPOUT_2_ID = "publisher_spout_2";

	private static final String SUBSCRIBER_SPOUT_1_ID = "subscriber_spout_1";
	private static final String SUBSCRIBER_SPOUT_2_ID = "subscriber_spout_2";
	private static final String SUBSCRIBER_SPOUT_3_ID = "subscriber_spout_3";
	public static Set<String> subscriberIdsSet;

	private static final String BROKER_BOLT_1_ID = "broker_bolt_1";
	private static final String BROKER_BOLT_2_ID = "broker_bolt_2";
	private static final String BROKER_BOLT_3_ID = "broker_bolt_3";

	private static final String SUBSCRIBER_BOLT_1_ID = "subscriber_bolt_1";
	private static final String SUBSCRIBER_BOLT_2_ID = "subscriber_bolt_2";
	private static final String SUBSCRIBER_BOLT_3_ID = "subscriber_bolt_3";

	private static final String PUB_SUB_TOPOLOGY_NAME = "pub_sub_topology";
	public static final String NOTIFICATION_STREAM = "notification_stream";
	public static final String PUBLICATION_STREAM = "publication_stream";

	private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main( String[] args ) throws Exception
    {
    	TopologyBuilder builder = new TopologyBuilder();
		AtomicBoolean canStartPublishing = new AtomicBoolean(false);

		PublisherSpout publisherSpout1 = new PublisherSpout();
		//PublisherSpout publisherSpout2 = new PublisherSpout();
		builder.setSpout(PUBLISHER_SPOUT_1_ID, publisherSpout1);
		//builder.setSpout(PUBLISHER_SPOUT_2_ID, publisherSpout2, 2);

		List<String> brokerSubscriberList = new ArrayList<>(List.of(SUBSCRIBER_SPOUT_1_ID, SUBSCRIBER_SPOUT_2_ID, SUBSCRIBER_SPOUT_3_ID));
		App.subscriberIdsSet = new HashSet<>(brokerSubscriberList);
		Collections.shuffle(brokerSubscriberList);

		SubscriberSpout subscriberSpout1 = new SubscriberSpout();
		SubscriberSpout subscriberSpout2 = new SubscriberSpout();
		SubscriberSpout subscriberSpout3 = new SubscriberSpout();
		builder.setSpout(SUBSCRIBER_SPOUT_1_ID, subscriberSpout1);
		builder.setSpout(SUBSCRIBER_SPOUT_2_ID, subscriberSpout2);
		builder.setSpout(SUBSCRIBER_SPOUT_3_ID, subscriberSpout3);

		BrokerBolt brokerBolt1 = new BrokerBolt();
		BrokerBolt brokerBolt2 = new BrokerBolt();
		BrokerBolt brokerBolt3 = new BrokerBolt();
		builder.setBolt(BROKER_BOLT_1_ID, brokerBolt1)
				.setNumTasks(3)
				.customGrouping(brokerSubscriberList.get(0), new SubscriberBalancedGrouping())
				.shuffleGrouping(PUBLISHER_SPOUT_1_ID);
		builder.setBolt(BROKER_BOLT_2_ID, brokerBolt2)
				.setNumTasks(3)
				.customGrouping(brokerSubscriberList.get(1), new SubscriberBalancedGrouping())
				.shuffleGrouping(BROKER_BOLT_1_ID, PUBLICATION_STREAM);
		builder.setBolt(BROKER_BOLT_3_ID, brokerBolt3)
				.setNumTasks(3)
				.customGrouping(brokerSubscriberList.get(2), new SubscriberBalancedGrouping())
				.shuffleGrouping(BROKER_BOLT_2_ID, PUBLICATION_STREAM);

		SubscriberBolt subscriberBolt1 = new SubscriberBolt();
		SubscriberBolt subscriberBolt2 = new SubscriberBolt();
		SubscriberBolt subscriberBolt3 = new SubscriberBolt();
		builder.setBolt(SUBSCRIBER_BOLT_1_ID, subscriberBolt1)
				.shuffleGrouping(BROKER_BOLT_1_ID, NOTIFICATION_STREAM);
		builder.setBolt(SUBSCRIBER_BOLT_2_ID, subscriberBolt2)
				.shuffleGrouping(BROKER_BOLT_2_ID, NOTIFICATION_STREAM);
		builder.setBolt(SUBSCRIBER_BOLT_3_ID, subscriberBolt3)
				.shuffleGrouping(BROKER_BOLT_3_ID, NOTIFICATION_STREAM);

    	Config config = new Config();
		config.setDebug(false);
		config.put("publicationFilePath", "/home/paul/temp/publications_25.txt");
		config.put("subscriptionFilePath", "/home/paul/temp/subscriptions_25.txt");
    	
    	LocalCluster cluster = new LocalCluster();
    	StormTopology topology = builder.createTopology();
    	
    	// fine tuning
    	config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 1024);
		// for Storm v1 use TOPOLOGY_DISRUPTOR_BATCH_SIZE
    	config.put(Config.TOPOLOGY_TRANSFER_BATCH_SIZE, 1);
    	
    	cluster.submitTopology(PUB_SUB_TOPOLOGY_NAME, config, topology);
    	
    	try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		int successCount = SubscriberBolt.getSuccessCount1();
	    long totalLatency = SubscriberBolt.getTotalLatency1();
	    double averageLatency = successCount > 0 ? (double) totalLatency / successCount : 0;

	    try {
	        FileWriter myWriter = new FileWriter("filename.txt");
	        myWriter.write("The number of successfully delivered notifications: " + successCount);
	        myWriter.write("\nAverage delivery latency (ms): " + averageLatency);
	        myWriter.close();
	        System.out.println("Successfully wrote to the file.");
	      } catch (IOException e) {
	        System.out.println("An error occurred.");
	        e.printStackTrace();
	      }

    	cluster.killTopology(PUB_SUB_TOPOLOGY_NAME);
    	cluster.shutdown();
    	
		// comment this for Storm v1
		cluster.close();
    }
}