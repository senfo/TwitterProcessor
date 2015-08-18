package com.advancedtechnicalengineering.sparkdemo.spark;

import com.advancedtechnicalengineering.sparkdemo.twitter.TwitterDataProvider;
import com.google.common.collect.Lists;
import com.twitter.hbc.twitter4j.parser.JSONObjectParser;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import twitter4j.JSONException;
import twitter4j.JSONObject;
import twitter4j.Status;

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
        List<String> terms = Lists.newArrayList("WhosGonnaWin", "Super Bowl");
        BlockingQueue<Status> msgQueue = provider.getStatusQueue();
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

    private void receive(BlockingQueue<Status> msgQueue) {
        while (true) {
            if (!msgQueue.isEmpty()) {
                try {
                    Status status = msgQueue.take();

                    store(status.getText());
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
