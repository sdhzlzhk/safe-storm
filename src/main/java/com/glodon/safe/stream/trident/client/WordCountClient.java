package com.glodon.safe.stream.trident.client;

import com.glodon.safe.stream.spout.RandomSentenceSpout;
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
        try {
            DRPCClient client = new DRPCClient(conf,"cluster01", 3772);
            String result = client.execute("words", "黄河之水天上来，奔流到海不复回");
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
