package com.glodon.safe.stream.topology;

import com.glodon.safe.stream.bolt.AddAlgorithmBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liuzk
 * @create 2018-08-10 19:48.
 */
public class SimpleAddDRPC {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleAddDRPC.class);
    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();
        LocalDRPC drpc =new LocalDRPC();

        DRPCSpout spout = new DRPCSpout("add", drpc);
        builder.setSpout("drpc", spout);
        builder.setBolt("sum", new AddAlgorithmBolt(), 3).shuffleGrouping("drpc");
        builder.setBolt("return", new ReturnResults(), 3).shuffleGrouping("sum");

        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        cluster.submitTopology("add-topology", conf, builder.createTopology());
        String sumRet = drpc.execute("add", "10 5");
        LOG.info("==================================");
        LOG.info("------------{}-----------------", sumRet);
        LOG.info("==================================");

    }
}
