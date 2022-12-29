package com.bilik;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TestCase1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        env.execute();
    }

    public static class StatefulFlatMap implements FlatMapFunction<String, String> {

        String lastValue;

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            out.collect(lastValue + "-" + value);
            lastValue = value;
        }
    }

}
