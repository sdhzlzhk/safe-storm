package com.glodon.safe.stream.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * @author liuzk
 * @create 2018-08-10 19:42.
 */
public class AddAlgorithmBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String args = input.getString(0);
        String[] argsArry = args.split(" ");
        int a = Integer.parseInt(argsArry[0]);
        int b = Integer.parseInt(argsArry[1]);
        int sum = a + b;
        Object retInfo = input.getValue(1);
        collector.emit(new Values(String.valueOf(sum), retInfo));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("result", "return-info"));
    }

}
