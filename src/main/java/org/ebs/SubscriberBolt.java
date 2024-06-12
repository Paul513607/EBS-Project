package org.ebs;

import org.apache.storm.executor.bolt.BoltOutputCollectorImpl;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SubscriberBolt extends BaseRichBolt {
    private OutputCollector collector;
    private String componentName;

    private static final AtomicInteger successCount1 = new AtomicInteger(0);
    private static final AtomicLong totalLatency1 = new AtomicLong(0);

    private static final Logger logger = LoggerFactory.getLogger(SubscriberBolt.class);

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.componentName = topologyContext.getThisComponentId();
    }

    @Override
    public void execute(Tuple tuple) {
        // Emit the notification to the subscriber
        String subscriberId = tuple.getStringByField("subscriberId");
        String company = tuple.getStringByField("company");
        double value = tuple.getDoubleByField("value");
        double drop = tuple.getDoubleByField("drop");
        double variation = tuple.getDoubleByField("variation");
        String date = tuple.getStringByField("date");
        long timestamp = tuple.getLongByField("timestamp");
        long currentTimestamp = System.currentTimeMillis();
        long latency = currentTimestamp - timestamp;

        logger.info("Subscriber {} received notification: company: {}, value: {}, drop: {}, variation: {}, date: {} from {}",
                subscriberId, company, value, drop, variation, date, this.componentName);
        collector.ack(tuple);

        // Update statistics
        successCount1.incrementAndGet();
        totalLatency1.addAndGet(latency);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public static int getSuccessCount1() {
        return successCount1.get();
    }

    public static long getTotalLatency1() {
        return totalLatency1.get();
    }
}
