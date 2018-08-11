package com.glodon.safe.stream.client;

import org.apache.storm.Config;
import org.apache.storm.utils.DRPCClient;

/**
 * @author liuzk
 * @create 2018-08-11 11:38.
 */
public class SimpleDRPCClient {
    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.put("storm.thrift.transport", "org.apache.storm.security.auth.plain.PlainSaslTransportPlugin");
        conf.put(Config.STORM_NIMBUS_RETRY_TIMES, 3);
        conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 10);
        conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 20);
        DRPCClient client = new DRPCClient(conf, "192.168.1.114", 3772);
        String result = client.execute("add", "10 5");
        System.out.println("结果 = " + result);
    }
}
