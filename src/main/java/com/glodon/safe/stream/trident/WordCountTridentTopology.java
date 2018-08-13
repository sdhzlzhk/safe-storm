package com.glodon.safe.stream.trident;

import com.glodon.safe.stream.trident.function.WordsSplit;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.operation.impl.FilterExecutor;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class WordCountTridentTopology {
    public static void main(String[] args) {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("十 年 生 死 两 茫 茫，不 思 量，自 难 忘"),
                new Values("千 里 孤 坟，无 处 话 凄 凉"),
                new Values("纵 使 相 逢 应 不 识，尘 满 面，鬓 如 霜"),
                new Values("夜 来 幽 梦 忽 还 乡，小 轩 窗，正 梳 妆"),
                new Values("相 顾 无 言，惟 有 泪 千 行"),
                new Values("料 得 年 年 肠 断 处，明 月 夜，短 松 冈")
        );
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        TridentState wordsCount = topology.newStream("spout1", spout)
                .each(new Fields("sentence"), new WordsSplit("，"), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(6);

        topology.newDRPCStream("words")
                .each(new Fields("args"), new WordsSplit(" "), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordsCount, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
    }
}
