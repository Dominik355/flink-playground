package com.bilik.SimpleState;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * How does connect() compare to union() ?
 *     - Union merges n streams of the same type
 *
 *     - Connect works with 2 streams, of different types, and you can tell from which input channel an element was sourced
 */
public class StatefulConnectedStreams {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> control = env.fromElements("DROP", "IGNORE").keyBy(x -> x);
        DataStream<String> data = env
                .fromElements("Flink", "DROP", "Forward", "IGNORE")
                .keyBy(x -> x);

        control.connect(data)
                .process(new ControlFunction())
                .print();

        env.execute();
    }


    /**
     * A KeyedCoProcessFunction, is analogous to a KeyedProcessFunction, but operates on two connected input streams.
     */
    public static class ControlFunction extends KeyedCoProcessFunction<String, String, String, String> {
        private ValueState<Boolean> blocked;

        @Override
        public void open(Configuration config) {
            blocked = getRuntimeContext().getState(new ValueStateDescriptor<>("blocked", Boolean.class));
        }

        @Override
        public void processElement1(String controlValue, Context context, Collector<String> out) throws Exception {
            blocked.update(Boolean.TRUE);
        }

        @Override
        public void processElement2(String dataValue, Context context, Collector<String> out) throws Exception {
            if (blocked.value() == null) {
                out.collect(dataValue);
            }
        }
    }



}