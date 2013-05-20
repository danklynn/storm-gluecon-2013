package com.fullcontact.stormdemo.spout

import backtype.storm.Config
import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Values
import org.apache.log4j.Logger
import twitter4j.FilterQuery
import twitter4j.StallWarning
import twitter4j.Status
import twitter4j.StatusDeletionNotice
import twitter4j.StatusListener
import twitter4j.TwitterStream
import twitter4j.TwitterStreamFactory
import twitter4j.conf.ConfigurationBuilder

import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

/**
 * Created with IntelliJ IDEA.
 * User: danlynn
 * Date: 5/20/13
 * Time: 9:43 AM
 * To change this template use File | Settings | File Templates.
 */
class TwitterStreamingSpout extends BaseRichSpout {
    SpoutOutputCollector spoutOutputCollector
    TwitterStream stream
    BlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000)

    public TwitterStreamingSpout(Config config) {
        // Tell Storm we want to use com.esotericsoftware.kryo.serializers.FieldSerializer
        // to serialize twitter4j Status objects
        config.registerSerialization(Status)
    }

    @Override
    void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("status"))
    }

    @Override
    void open(Map config, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector
        TwitterStreamFactory fact = new TwitterStreamFactory(
                new ConfigurationBuilder()
                        .setUser((String)config[TWITTER_USERNAME_KEY])
                        .setPassword((String)config[TWITTER_PASSWORD_KEY]).build());

        String[] terms = ((String)config[TWITTER_FILTER_KEY]).split(",")

        stream = fact.getInstance()
        stream.addListener(new QueuingStatusListener())
        stream.filter(new FilterQuery().track(terms))
    }

    @Override
    void nextTuple() {
        spoutOutputCollector.emit(new Values(queue.take()))
    }

    class QueuingStatusListener implements StatusListener {
        @Override
        void onStatus(Status status) {
            if (!queue.offer(status)) {
                log.warn("Queue is full, dropping status: ${status.text}")
            }
        }

        @Override
        void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            // do nothing
        }

        @Override
        void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            // do nothing
        }

        @Override
        void onScrubGeo(long userId, long upToStatusId) {
            // do nothing
        }

        @Override
        void onStallWarning(StallWarning warning) {
            // do nothing
        }

        @Override
        void onException(Exception ex) {
            // do nothing
        }
    }

    static final Logger log = Logger.getLogger(this)

    static final String TWITTER_USERNAME_KEY = 'twitter.username'
    static final String TWITTER_PASSWORD_KEY = 'twitter.password'
    static final String TWITTER_FILTER_KEY = 'twitter.filterTerms'
}
