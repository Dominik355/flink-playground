package com.bilik.process;

import com.bilik.joining.TumblingWindowJoin;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.api.java.tuple.Tuple2.of;

/**
 * In the following example a KeyedProcessFunction maintains counts per key,
 * and emits a key/count pair whenever a minute passes (in event time) without an update for that key:
 *  - The count, key, and last-modification-timestamp are stored in a ValueState, which is implicitly scoped by key.
 *  - For each record, the KeyedProcessFunction increments the counter and sets the last-modification timestamp
 *  - The function also schedules a callback one minute into the future (in event time)
 *  - Upon each callback, it checks the callback’s event time timestamp against the last-modification time of
 *    the stored count and emits the key/count if they match (i.e., no further update occurred during that minute)
 *
 *    This simple example could have been implemented with session windows. We use KeyedProcessFunction here to illustrate the basic pattern it provides.
 */
public class KeyCountPair {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<Tuple2<String, String>> stream = env.addSource(new IntegerEventSource());

// apply the process function onto a keyed stream
        DataStream<Tuple2<String, Long>> result = stream
                //.assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks().withTimestampAssigner())
                .keyBy(value -> value.f0)
                .process(new CountWithTimeoutFunction());

        result.print();

        env.execute();
    }

    /**
     * The data type stored in the state
     */
    public static class CountWithTimestamp {
        public String key;
        public long count;
        public long lastModified;
    }

    /**
     * The implementation of the ProcessFunction that maintains the count and timeouts
     */
    public static class CountWithTimeoutFunction extends KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, Long>> {

        /** The state that is maintained by this process function */
        private ValueState<CountWithTimestamp> state;
        private int count;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
        }

        @Override
        public void processElement(
                Tuple2<String, String> value,
                Context ctx,
                Collector<Tuple2<String, Long>> out) throws Exception {

            // retrieve the current count
            CountWithTimestamp current = state.value();
            if (current == null) {
                current = new CountWithTimestamp();
                current.key = value.f0;
            }

            // update the state's count
            current.count++;

            // set the state's timestamp to the record's assigned event time timestamp
            current.lastModified = ctx.timerService().currentProcessingTime();

            // write the state back
            state.update(current);

            // schedule the next timer 60 seconds from the current event time
            count++;

            // we are registering timer from every single element, so it wil be called a lot, but will emit element only every 500 milis
            ctx.timerService().registerEventTimeTimer(current.lastModified + 500);
        }

        @Override
        public void onTimer(
                long timestamp,
                KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, Long>>.OnTimerContext ctx,
                Collector<Tuple2<String, Long>> out) throws Exception {

            // get the state for the key that scheduled the timer
            CountWithTimestamp result = state.value();
            // check if this is an outdated timer or the latest timer
            if (timestamp == result.lastModified + 500) {
                // emit the state on timeout
                out.collect(new Tuple2(result.key, result.count));
            }
        }
    }

    public static class IntegerEventSource implements SourceFunction<Tuple2<String, String>> {

        private static final List<String> strings = List.of(
                "emil", "nike", "okno", "obal", "car", "doors"
        );


        @Override
        public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
            long end = System.currentTimeMillis() + 10000;
            while(System.currentTimeMillis() < end) {
                String word = strings.get(ThreadLocalRandom.current().nextInt(strings.size()));
                ctx.collect(new Tuple2<>(word, word));
                Thread.sleep(10);
            }
        }

        @Override
        public void cancel() {
            throw new IllegalStateException();
        }
    }


}
