package com.glodon.safe.stream.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author liuzk
 * @create 2018-08-07 8:56.
 */
public class TestWordCount extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TestWordCount.class);
    private OutputCollector outputCollector;
    private Map<String, Integer> wordMap = new HashMap<>();
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        Integer count = wordMap.get(word);
        if (null == count) {
            count = 0;
        }
        count++;
        wordMap.put(word, count);
        this.outputCollector.emit(input, new Values(word,count));
        this.outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","count"));
    }
}
