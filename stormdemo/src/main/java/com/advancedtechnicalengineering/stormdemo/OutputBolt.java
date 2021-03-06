package com.advancedtechnicalengineering.stormdemo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import twitter4j.Status;

import java.util.Map;

/**
 * Simple bolt that outputs tuples as a string
 */
public class OutputBolt implements IRichBolt {
    public static final String NAME = "OutputBolt";

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    }

    @Override
    public void execute(Tuple tuple) {
        Status status = (Status)tuple.getValueByField("status");

        System.out.println(status.getText());
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
