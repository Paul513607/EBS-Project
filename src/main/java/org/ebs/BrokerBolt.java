package org.ebs;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.ebs.util.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BrokerBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, List<Subscription>> subscriptions;
    private String componentId;
    
    private static final AtomicInteger successCount = new AtomicInteger(0);
    private static final AtomicLong totalLatency = new AtomicLong(0);

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.subscriptions = new HashMap<>();

        this.componentId = context.getThisComponentId();
    }

    private static final Logger logger = LoggerFactory.getLogger(BrokerBolt.class);

    @Override
    public void execute(Tuple tuple) {
        if (App.subscriberIdsSet.contains(tuple.getSourceComponent())) {
            String subscriberId = tuple.getStringByField("subscriberId");
            Subscription subscription = (Subscription) tuple.getValueByField("subscription");
            addSubscription(subscriberId, subscription);
            collector.ack(tuple);
        } else {
            String company = tuple.getStringByField("company");
            double value = tuple.getDoubleByField("value");
            double drop = tuple.getDoubleByField("drop");
            double variation = tuple.getDoubleByField("variation");
            String date = tuple.getStringByField("date");
            long timestamp = tuple.getLongByField("timestamp");
            long receiveTimestamp = System.currentTimeMillis();
            long latency = receiveTimestamp - timestamp;

            // Match publication against subscriptions
            for (Map.Entry<String, List<Subscription>> entry : subscriptions.entrySet()) {
                for (Subscription subscription : entry.getValue()) {
                    if (subscription.matches(company, value, drop, variation, date)) {
                    	 logger.info("Broker {} matched publication \n{(company,{});(value,{});(drop,{});(variation,{});(date,{})}\nwith subscription\n{} for subscriberId {}",
                                 this.componentId, company, value, drop, variation, date, subscription, entry.getKey());
                         collector.emit(App.NOTIFICATION_STREAM, new Values(entry.getKey(), company, value, drop, variation, date, timestamp));
                    
                        // Update statistics
//                        successCount.incrementAndGet();
//                        totalLatency.addAndGet(latency);
                    }
                }
            }

            collector.emit(App.PUBLICATION_STREAM, tuple, tuple.getValues());
            collector.ack(tuple);
        }
    }

    public void addSubscription(String subscriberId, Subscription subscription) {
        subscriptions.computeIfAbsent(subscriberId, k -> new ArrayList<>()).add(subscription);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(App.NOTIFICATION_STREAM, new Fields("subscriberId", "company", "value", "drop", "variation", "date", "timestamp"));
        declarer.declareStream(App.PUBLICATION_STREAM, new Fields("company", "value", "drop", "variation", "date", "timestamp"));
    }
    
    public static int getSuccessCount() {
        return successCount.get();
    }

    public static long getTotalLatency() {
        return totalLatency.get();
    }
}
