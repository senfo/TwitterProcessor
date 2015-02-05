package com.advancedtechnicalengineering.sparkdemo.spark;

import com.advancedtechnicalengineering.sparkdemo.twitter.TwitterDataProvider;
import com.google.common.collect.Lists;
import com.twitter.hbc.twitter4j.parser.JSONObjectParser;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * A custom Receiver to demonstrate how to stream Twitter data into spark.
 * We could have used the receiver provided by spark-streaming-twitter_2.10;
 * however, since the intent of this application is to gain an understanding
 * of how streams work in Spark, we've created it ourselves.
 */
public class TwitterReceiver extends Receiver<String> {
    private final TwitterDataProvider provider;

    /**
     * Initializes a new instance of the TwitterReceiver class
     * @param consumerKey Twitter consumer key
     * @param consumerSecret Twitter consumer secret
     * @param token Twitter app token
     * @param tokenString Twitter app token string
     */
    public TwitterReceiver(String consumerKey, String consumerSecret, String token, String tokenString) {
        super(StorageLevel.MEMORY_ONLY());

        this.provider = new TwitterDataProvider(consumerKey, consumerSecret, token, tokenString);
    }

    @Override
    public StorageLevel storageLevel() {
        return StorageLevel.MEMORY_ONLY();
    }

    @Override
    public void onStart() {
        List<String> terms = Lists.newArrayList("Twitter Storm", "Spark");
        BlockingQueue<String> msgQueue = provider.getMsgQueue();
        provider.connect(terms);

        new Thread() {
            @Override public void run() {
                receive(msgQueue);
            }
        }.start();
    }

    @Override
    public void onStop() {
        provider.disconnect();
    }

    private void receive(BlockingQueue<String> msgQueue) {
        while (true) {
            if (!msgQueue.isEmpty()) {
                try {
                    String tweet = msgQueue.take();
                    JSONObject json = new JSONObject(tweet);
                    //JSONObjectParser parser = new
                    store(msgQueue.take());
                }
                catch (InterruptedException | JSONException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
