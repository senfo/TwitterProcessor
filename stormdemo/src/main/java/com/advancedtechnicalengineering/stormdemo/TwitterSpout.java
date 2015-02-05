package com.advancedtechnicalengineering.stormdemo;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.advancedtechnicalengineering.sparkdemo.twitter.TwitterDataProvider;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Extends {@link BaseRichSpout} to build a Storm spout for consuming Twitter data
 */
public class TwitterSpout extends BaseRichSpout {
    public static final String NAME = "TwitterSpout";
    private SpoutOutputCollector collector;
    private BlockingQueue<String> queue;
    private TwitterDataProvider dataProvider;
    private List<String> terms;

    /**
     * Initializes a new instance of the {@link TwitterSpout} class. Requires a registered app on http://dev.twitter.com
     * @param consumerKey Twitter consumer key
     * @param consumerSecret Twitter consumer secret
     * @param token Twitter app token
     * @param tokenString Twitter app token string
     * @param terms Twitter Terms
     */
    public TwitterSpout(String consumerKey, String consumerSecret, String token, String tokenString,
                        List<String> terms) {
        this.dataProvider = new TwitterDataProvider(consumerKey, consumerSecret, token, tokenString);
        this.terms = terms;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("status"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.queue = dataProvider.getMsgQueue();
        this.collector = spoutOutputCollector;

        this.dataProvider.connect(this.terms);
    }

    @Override
    public void nextTuple() {
        String status = queue.poll();

        if (status != null) {
            this.collector.emit(new Values(status));
        }
        else {
            Utils.sleep(50);
        }
    }
}

