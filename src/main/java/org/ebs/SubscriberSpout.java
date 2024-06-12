package org.ebs;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.ebs.util.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriberSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private BufferedReader reader;
    private String filePath;
    private Random random;

    private String componentName;

    private static final Logger logger = LoggerFactory.getLogger(SubscriberSpout.class);

    @Override
    public void open(Map<String, Object> map, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.filePath = (String) map.get("subscriptionFilePath");
        this.random = new Random();
        this.componentName = context.getThisComponentId();
        try {
            this.reader = new BufferedReader(new FileReader(filePath));
        } catch (IOException e) {
            throw new RuntimeException("Error reading file", e);
        }
    }

    @Override
    public void nextTuple() {
        try {
            String line = reader.readLine();
            if (line != null) {
                String[] parts = line.split(";");
                Subscription subscription = new Subscription();
                for (String part : parts) {
                    String[] subParts = part.replace("{", "").replace("}", "").split(",");
                    String field = subParts[0].replace("(", "");
                    String operator = subParts[1];
                    Object value = subParts[2].replace(")", "");
                    if (field.equals("value") || field.equals("drop") || field.equals("variation")) {
                        value = Double.parseDouble((String) value);
                    } else if (field.equals("company") || field.equals("date")) {
                        value = ((String) value).replace("\"", "");
                    }

                    subscription.addCriterion(field, operator, value);
                }

                String subscriberId = this.componentName;
                // logger.info("{} {}", subscriberId, subscription);

                collector.emit(new Values(subscriberId, subscription));
            } else {
                Utils.sleep(1000);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading line", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("subscriberId", "subscription"));
    }
}
