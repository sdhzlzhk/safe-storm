package com.glodon.safe.stream.trident.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class WordsSplit extends BaseFunction {
    private String separator;

    public WordsSplit(String separator) {
        this.separator = separator;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String sentence = tuple.getString(0);
        for (String word : sentence.split(separator)) {
            collector.emit(new Values(word));
        }
    }
}
