package com.glodon.safe.stream.topology;

import com.glodon.safe.stream.bolt.AddAlgorithmBolt;
import com.glodon.safe.stream.bolt.BaseAddAlgorithmBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseDRPCTopology {
    private static final Logger LOG = LoggerFactory.getLogger(BaseDRPCTopology.class);
    public static void main(String[] args) {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("add");
        builder.addBolt(new BaseAddAlgorithmBolt(), 3);
        Config conf = new Config();
        if (null == args || args.length == 0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("drpc-add", conf, builder.createLocalTopology(drpc));
            LOG.info("加法结果：{}", drpc.execute("add", "20,3"));
            cluster.shutdown();
            drpc.shutdown();
        } else {
            try {
                StormSubmitter.submitTopologyWithProgressBar("drpc-add", conf, builder.createRemoteTopology());
            } catch (AlreadyAliveException e) {
                LOG.error("该拓扑已经运行",e);
            } catch (InvalidTopologyException e) {
                LOG.error("该拓扑错误",e);
            } catch (AuthorizationException e) {
                LOG.error("没有授权运行该拓扑", e);
            }
        }
    }
}
