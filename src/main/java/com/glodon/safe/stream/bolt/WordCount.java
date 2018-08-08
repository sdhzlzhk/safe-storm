package com.glodon.safe.stream.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author liuzk
 * @create 2018-08-06 19:44.
 */
public class WordCount extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);
    private Map<String, Integer> wordMap = new HashMap<>(16);

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = input.getString(0);
        Integer count = wordMap.get(word);
        if (null == count) {
            count = 0;
        }
        count++;
        wordMap.put(word, count);
        LOG.info("线程[{}]统计：{} = {}", Thread.currentThread().getName(), word, count);
        collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
