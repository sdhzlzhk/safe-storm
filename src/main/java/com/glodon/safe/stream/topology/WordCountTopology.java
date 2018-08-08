package com.glodon.safe.stream.topology;

import com.glodon.safe.stream.bolt.SplitSentence;
import com.glodon.safe.stream.bolt.WordCount;
import com.glodon.safe.stream.spout.RandomSentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @author liuzk
 * @create 2018-08-06 19:51.
 */
public class WordCountTopology {
    public static void main(String[] args) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSentenceSpout(), 4);
        builder.setBolt("split", new SplitSentence(), 4).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), 4).fieldsGrouping("split", new Fields("word"));

        Config config = new Config();
        config.setMaxTaskParallelism(3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", config, builder.createTopology());
        Thread.sleep(600000);
        cluster.shutdown();
    }
}
