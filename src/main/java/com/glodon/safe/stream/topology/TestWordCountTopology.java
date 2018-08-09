package com.glodon.safe.stream.topology;

import com.glodon.safe.stream.bolt.TestWordCount;
import com.glodon.safe.stream.spout.TestWordSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @author liuzk
 * @create 2018-08-07 9:02.
 */
public class TestWordCountTopology {
    public static void main(String[] args) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word", new TestWordSpout(), 10);
        builder.setBolt("wordCount", new TestWordCount(), 3).fieldsGrouping("word", new Fields("word"));
        Config config = new Config();
        config.setDebug(true);
        if (args != null && args.length > 0) {
            config.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0],config, builder.createTopology());
        } else {
            config.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", config, builder.createTopology());
            Thread.sleep(10000);
            cluster.shutdown();
        }

    }
}
