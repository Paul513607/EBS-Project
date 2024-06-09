package org.ebs;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

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

    public static void main( String[] args ) throws Exception
    {
    	TopologyBuilder builder = new TopologyBuilder();
		AtomicBoolean canStartPublishing = new AtomicBoolean(false);

		PublisherSpout publisherSpout1 = new PublisherSpout();
		//PublisherSpout publisherSpout2 = new PublisherSpout();
		builder.setSpout(PUBLISHER_SPOUT_1_ID, publisherSpout1, 2);
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
		builder.setBolt(BROKER_BOLT_1_ID, brokerBolt1).shuffleGrouping(brokerSubscriberList.get(0))
				.shuffleGrouping(PUBLISHER_SPOUT_1_ID)
				.customGrouping(SUBSCRIBER_SPOUT_1_ID, new SubscriberBalancedGrouping())
				.customGrouping(SUBSCRIBER_SPOUT_2_ID, new SubscriberBalancedGrouping())
				.customGrouping(SUBSCRIBER_SPOUT_3_ID, new SubscriberBalancedGrouping());
		builder.setBolt(BROKER_BOLT_2_ID, brokerBolt2).shuffleGrouping(brokerSubscriberList.get(1))
				.shuffleGrouping(BROKER_BOLT_1_ID, PUBLICATION_STREAM)
				.customGrouping(SUBSCRIBER_SPOUT_1_ID, new SubscriberBalancedGrouping())
				.customGrouping(SUBSCRIBER_SPOUT_2_ID, new SubscriberBalancedGrouping())
				.customGrouping(SUBSCRIBER_SPOUT_3_ID, new SubscriberBalancedGrouping());
		builder.setBolt(BROKER_BOLT_3_ID, brokerBolt3).shuffleGrouping(brokerSubscriberList.get(2))
				.shuffleGrouping(BROKER_BOLT_2_ID, PUBLICATION_STREAM)
				.customGrouping(SUBSCRIBER_SPOUT_1_ID, new SubscriberBalancedGrouping())
				.customGrouping(SUBSCRIBER_SPOUT_2_ID, new SubscriberBalancedGrouping())
				.customGrouping(SUBSCRIBER_SPOUT_3_ID, new SubscriberBalancedGrouping());

		SubscriberBolt subscriberBolt1 = new SubscriberBolt();
		SubscriberBolt subscriberBolt2 = new SubscriberBolt();
		SubscriberBolt subscriberBolt3 = new SubscriberBolt();
		builder.setBolt(SUBSCRIBER_BOLT_1_ID, subscriberBolt1)
				.shuffleGrouping(BROKER_BOLT_1_ID, NOTIFICATION_STREAM)
				.shuffleGrouping(BROKER_BOLT_2_ID, NOTIFICATION_STREAM)
				.shuffleGrouping(BROKER_BOLT_3_ID, NOTIFICATION_STREAM);
		builder.setBolt(SUBSCRIBER_BOLT_2_ID, subscriberBolt2)
				.shuffleGrouping(BROKER_BOLT_1_ID, NOTIFICATION_STREAM)
				.shuffleGrouping(BROKER_BOLT_2_ID, NOTIFICATION_STREAM)
				.shuffleGrouping(BROKER_BOLT_3_ID, NOTIFICATION_STREAM);
		builder.setBolt(SUBSCRIBER_BOLT_3_ID, subscriberBolt3).shuffleGrouping(BROKER_BOLT_3_ID, NOTIFICATION_STREAM)
				.shuffleGrouping(BROKER_BOLT_1_ID, NOTIFICATION_STREAM)
				.shuffleGrouping(BROKER_BOLT_2_ID, NOTIFICATION_STREAM)
				.shuffleGrouping(BROKER_BOLT_3_ID, NOTIFICATION_STREAM);

    	Config config = new Config();
		config.setDebug(true);
		config.put("publicationFilePath", "/home/paul/temp/publications2.txt");
		config.put("subscriptionFilePath", "/home/paul/temp/subscriptions2.txt");
    	
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

    	cluster.killTopology(PUB_SUB_TOPOLOGY_NAME);
    	cluster.shutdown();
    	
		// comment this for Storm v1
		cluster.close();
    }
}