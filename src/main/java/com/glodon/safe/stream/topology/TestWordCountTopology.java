package com.glodon.safe.stream.topology;

import com.glodon.safe.stream.bolt.TestWordCount;
import com.glodon.safe.stream.spout.TestWordSpout;
import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @author liuzk
 * @create 2018-08-07 9:02.
 */
public class TestWordCountTopology {
    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word", new TestWordSpout(), 10);
        builder.setBolt("wordCount", new TestWordCount(), 3).fieldsGrouping("word", new Fields("word"));
        Config config = new Config();
        config.setDebug(true);

    }
}
