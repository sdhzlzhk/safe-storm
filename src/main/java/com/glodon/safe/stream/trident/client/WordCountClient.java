package com.glodon.safe.stream.trident.client;

import org.apache.storm.Config;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.DRPCClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountClient {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountClient.class);
    public static void main(String[] args) {
        Config conf = new Config();
        conf.put("storm.thrift.transport", "org.apache.storm.security.auth.plain.PlainSaslTransportPlugin");
        conf.put(Config.STORM_NIMBUS_RETRY_TIMES, 3);
        conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 10);
        conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 20);
        try {
            DRPCClient client = new DRPCClient(conf,"cluster01", 3772);
            String result = client.execute("words", "泪");
            LOG.info("结果是：{}", result);
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (DRPCExecutionException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
