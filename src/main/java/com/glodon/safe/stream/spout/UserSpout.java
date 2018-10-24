package com.glodon.safe.stream.spout;

import com.google.common.collect.Lists;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class UserSpout implements IRichSpout {
    boolean isDistributed;
    SpoutOutputCollector collector;
    public static final List<Values> rows = Lists.newArrayList(
      new Values(1,"刘忠凯", System.currentTimeMillis()),
      new Values(2,"胡宗南", System.currentTimeMillis()),
      new Values(3,"徐海东", System.currentTimeMillis())
    );

    public UserSpout() {
        this(true);
    }

    public UserSpout(boolean isDistributed) {
        this.isDistributed = isDistributed;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        final Random rand = new Random();
        final Values row = rows.get(rand.nextInt(rows.size() - 1));
        this.collector.emit(row);
        Thread.yield();
    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user_id","user_name","create_date"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
