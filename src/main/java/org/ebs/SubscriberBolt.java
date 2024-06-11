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

public class SubscriberBolt extends BaseRichBolt {
    private OutputCollector collector;

    private static final Logger logger = LoggerFactory.getLogger(SubscriberBolt.class);

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
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

        logger.info("Subscriber {} received notification: company: {}, value: {}, drop: {}, variation: {}, date: {}",
                subscriberId, company, value, drop, variation, date);
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
