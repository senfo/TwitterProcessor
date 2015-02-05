package com.advancedtechnicalengineering.stormdemo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.google.common.collect.Lists;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * The primary entry point and topology for the Storm demo
 */
public class TwitterIngestTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        Properties properties = getProperties();
        Config config = new Config();
        String consumerKey = properties.getProperty("consumerKey");
        String consumerSecret = properties.getProperty("consumerSecret");
        String token = properties.getProperty("token");
        String tokenString = properties.getProperty("tokenString");
        List<String> terms = Lists.newArrayList("Twitter Storm", "Spark");

        builder.setSpout(TwitterSpout.NAME, new TwitterSpout(consumerKey, consumerSecret, token, tokenString, terms));
        builder.setBolt(OutputBolt.NAME, new OutputBolt()).fieldsGrouping(TwitterSpout.NAME, new Fields("status"));

        if (args.length > 0 && args[0].equals("local")) {
            LocalCluster cluster = new LocalCluster();

            config.setMaxTaskParallelism(3);
            cluster.submitTopology("TwitterIngestTopology", config, builder.createTopology());
        }
        else {
            StormSubmitter.submitTopology("TwitterIngestTopology", config, builder.createTopology());
        }
    }

    private static Properties getProperties() {
        try {
            Properties properties = new Properties();
            InputStream stream = new FileInputStream("config.xml");

            properties.loadFromXML(stream);
            stream.close();

            return properties;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
