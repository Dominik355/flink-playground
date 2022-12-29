package com.bilik.common;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.List;

public class TestSink<IN> extends RichSinkFunction<IN> {

    private final String name;

    public TestSink(String name) {
        this.name = name;
    }

    public TestSink() {
        this("results");
    }

    @Override
    public void open(Configuration parameters) {
        getRuntimeContext().addAccumulator(name, new ListAccumulator<IN>());
    }

    @Override
    public void invoke(IN value, Context context) {
        getRuntimeContext().getAccumulator(name).add(value);
    }

    public List<IN> getResults(JobExecutionResult jobResult) {
        return jobResult.getAccumulatorResult(name);
    }

}
