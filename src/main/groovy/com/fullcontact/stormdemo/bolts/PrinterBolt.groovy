package com.fullcontact.stormdemo.bolts

import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt

/**
 * Created with IntelliJ IDEA.
 * User: danlynn
 * Date: 5/20/13
 * Time: 10:10 AM
 * To change this template use File | Settings | File Templates.
 */
class PrinterBolt extends BaseRichBolt {
    OutputCollector outputCollector

    @Override
    void prepare(Map config, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector
    }

    @Override
    void execute(backtype.storm.tuple.Tuple tuple) {
        System.out.println(tuple)
        outputCollector.ack(tuple)
    }

    @Override
    void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
