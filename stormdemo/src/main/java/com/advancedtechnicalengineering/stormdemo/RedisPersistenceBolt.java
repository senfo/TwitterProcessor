package com.advancedtechnicalengineering.stormdemo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * Persists data to a redis database
 */
public class RedisPersistenceBolt extends BaseRichBolt {
    public static final String NAME = "RedisPersistenceBolt";
    private Jedis jedis;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        jedis = new Jedis("localhost");
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");

        jedis.incr(String.format("words:%s", word));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
