package com.glodon.safe.stream.topology;

import com.glodon.safe.stream.bolt.AddAlgorithmBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
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
        Config conf = new Config();
        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);
            try {
                StormSubmitter.submitTopologyWithProgressBar(args[0],conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                LOG.error("该拓扑已经运行",e);
            } catch (InvalidTopologyException e) {
                LOG.error("该拓扑错误",e);
            } catch (AuthorizationException e) {
                LOG.error("没有授权运行该拓扑", e);
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("add-topology", conf, builder.createTopology());
            String sumRet = drpc.execute("add", "10 5");
            LOG.info("==================================");
            LOG.info("------------{}-----------------", sumRet);
            LOG.info("==================================");
            cluster.shutdown();
            drpc.shutdown();
        }
    }
}
