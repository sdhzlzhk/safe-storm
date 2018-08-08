package com.glodon.safe.stream.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * @author liuzk
 * @create 2018-08-06 19:29.
 */
public class RandomSentenceSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(RandomSentenceSpout.class);
    private SpoutOutputCollector collector;
    private Random random;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.random = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(10);
        String[] sentences = new String[]{
                "豫 章 故 郡 洪 都 新 府 星 分 翼 轸 地 接 衡 庐",
                "襟 三 江 而 带 五 湖",
                "控 蛮 荆 而 引 瓯 越",
                "物 华 天 宝 龙 光 射 牛 斗 之 墟",
                "人 杰 地 灵 徐 孺 下 陈 蕃 之 榻"};
        String sentence = sentences[random.nextInt(sentences.length)];
        this.collector.emit(new Values(sentence), UUID.randomUUID());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
