package com.advancedtechnicalengineering.sparkdemo.twitter;

import com.twitter.hbc.core.event.Event;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Provides a data source for Twitter streams
 */
public class TwitterDataProvider implements Serializable {
    private TwitterStream twitterStream;
    private BlockingQueue<Status> statusQueue = new LinkedBlockingQueue<>(100000);
    private String consumerKey;
    private String consumerSecret;
    private String token;
    private String tokenString;

    /**
     * Initializes a new instance of the {@link TwitterDataProvider} class. Requires a registered app on http://dev.twitter.com
     * @param consumerKey Twitter consumer key
     * @param consumerSecret Twitter consumer secret
     * @param token Twitter app token
     * @param tokenString Twitter app token string
     */
    public TwitterDataProvider(String consumerKey, String consumerSecret, String token, String tokenString) {
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.token = token;
        this.tokenString = tokenString;
    }

    /**
     * Connects to Twitter and starts listening
     */
    public void connect(List<String> terms) {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                statusQueue.add(status);
            }
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
            public void onScrubGeo(long userId, long upToStatusId) {}
            public void onStallWarning(StallWarning warning) {}
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };

        // Authentication
        cb.setOAuthConsumerKey(consumerKey)
                .setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(token)
                .setOAuthAccessTokenSecret(tokenString);

        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        twitterStream.addListener(listener);

        twitterStream.sample();
    }

    /**
     * Disconnects from the Twitter stream
     */
    public void disconnect() {
        twitterStream.shutdown();
    }

    /**
     * Gets the queue containing String messages from Twitter
     * @return The queue containing String messages from Twitter
     */
    public BlockingQueue<Status> getStatusQueue() {
        return statusQueue;
    }
}
