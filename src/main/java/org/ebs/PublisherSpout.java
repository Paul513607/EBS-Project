package org.ebs;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PublisherSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private BufferedReader reader;
    private String filePath;

    private static final Logger logger = LoggerFactory.getLogger(PublisherSpout.class);

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        try {
            this.collector = collector;
            this.filePath = (String) map.get("publicationFilePath");
            this.reader = new BufferedReader(new FileReader(this.filePath));
        } catch (IOException e) {
            throw new RuntimeException("Error reading file", e);
        }
    }

    @Override
    public void nextTuple() {
        try {
            String line = reader.readLine();
            if (line != null) {
                line = line.replace("{", "").replace("}", "");
                String[] parts = line.split(";");
                List<String> partsList = List.of(parts);
                partsList = partsList.stream()
                        .map(part -> part.replace("(", "").replace(")", ""))
                        .collect(Collectors.toList());
                String company = partsList.get(0).split(",")[1].replace("\"", "");
                double value = Double.parseDouble(partsList.get(1).split(",")[1]);
                double drop = Double.parseDouble(partsList.get(2).split(",")[1]);
                double variation = Double.parseDouble(partsList.get(3).split(",")[1]);
                String date = partsList.get(4).split(",")[1].replace("}", "").trim();
                logger.info(Arrays.toString(parts));

                this.collector.emit(new Values(company, value, drop, variation, date));
            } else {
                Utils.sleep(1000);  // Wait for a second if there are no new lines
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading line", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("company", "value", "drop", "variation", "date"));
    }
}
