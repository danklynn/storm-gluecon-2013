package com.fullcontact.stormdemo

import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.StormSubmitter
import backtype.storm.generated.StormTopology
import backtype.storm.topology.TopologyBuilder
import com.fullcontact.stormdemo.bolts.PrinterBolt
import com.fullcontact.stormdemo.spout.TwitterStreamingSpout

/**
 * Created with IntelliJ IDEA.
 * User: danlynn
 * Date: 5/20/13
 * Time: 10:11 AM
 * To change this template use File | Settings | File Templates.
 */
class GlueConTopology {
    public static void main(String[] args) {
        Config config = getConfig(args)

        String topologyName = config.get("topology-name", "local") == "local"
        if (topologyName) {
            def cluster = new LocalCluster()
            cluster.submitTopology(topologyName, config, createTopology(config))
            Thread.sleep(3000000)
            cluster.shutdown()
        } else {
            StormSubmitter.submitTopology(topologyName, config, createTopology(config));
        }
    }

    static StormTopology createTopology(Config config) {

        TopologyBuilder builder = new TopologyBuilder()
        builder.setSpout("twitter-spout", new TwitterStreamingSpout(config), 1)
        builder.setBolt("printer-bolt", new PrinterBolt(), 1).shuffleGrouping("twitter-spout")

        return builder.createTopology()
    }

    static Config getConfig(String[] args) {
        Config config = new Config()

        config.put(TwitterStreamingSpout.TWITTER_USERNAME_KEY, System.getenv("TWITTER_USERNAME"))
        config.put(TwitterStreamingSpout.TWITTER_PASSWORD_KEY, System.getenv("TWITTER_PASSWORD"))

        for (int i=0; i<args.length; i++) {
            if ("--name".equals(args[i]) && args.length > i+1)
                config.put("topology-name", args[i+1])
            if ("--filter".equals(args[i]) && args.length > i+1)
                config.put(TwitterStreamingSpout.TWITTER_FILTER_KEY, args[i+1])
        }

        return config
    }
}
