package com.bilik.joining;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static com.bilik.joining.TumblingWindowJoin.IntegerEvent.of;

/**
 * Result :
 * 9> [0, 1671450076048] || [0, 1671450076048]
 * 9> [0, 1671450076048] || [0, 1671450076049]
 * 9> [0, 1671450076049] || [0, 1671450076048]
 * 9> [0, 1671450076049] || [0, 1671450076049]
 * 9> [0, 1671450076051] || [0, 1671450076050]
 * 9> [0, 1671450076051] || [0, 1671450076051]
 * 9> [0, 1671450076052] || [0, 1671450076052]
 * 9> [0, 1671450076052] || [0, 1671450076053]
 *
 * What is point of this example ?
 * We are creating stream of 4 windows of length 2 miliseconds,
 * Stream1: |0 1||  3||4  ||   |
 * Stream2: |0 1||2 3||4 5||6 7|
 * We join them by Key, which is same for every element, so we acreate 1 common stream and window it by time.
 * What we get as a result ? All possible combinations of the elements in 1 window
 * so for 0 and 1 in first stream, its : 0-0, 0-1, 1-0, 1-1
 *
 * What happens if you do not window them ? ... you have to
 */
public class TumblingWindowJoin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<IntegerEvent> orangeStream = env.addSource(new IntegerEventSource(
                of(0, 1671450076048l), of(0, 1671450076048l + 1), of(0, 1671450076048l + 3), of(0, 1671450076048l + 4)
        ))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<IntegerEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp));

        DataStream<IntegerEvent> greenStream = env.addSource(new IntegerEventSource(
                        of(0, 1671450076048l + 0), of(0, 1671450076048l + 1), of(0, 1671450076048l + 2), of(0, 1671450076048l + 3), of(0, 1671450076048l + 4), of(0, 1671450076048l + 5), of(0, 1671450076048l + 6), of(0, 1671450076048l + 7)
                ))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<IntegerEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp));

        DataStream<String> result = orangeStream.join(greenStream)
                .where(event -> event.key)
                .equalTo(event -> event.key)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(2)))
                .apply((first, second) -> first.toString() + " || " + second.toString());

        result.print();

        env.execute();
    }

    public static class IntegerEvent implements Serializable {
        public Integer key;
        public long timestamp;

        public static IntegerEvent of(Integer key, long timestamp) {
            IntegerEvent event = new IntegerEvent();
            event.key = key;
            event.timestamp = timestamp;
            return event;
        }

        @Override
        public String toString() {
            return "[" + key + ", " + timestamp +']';
        }
    }

    public static class IntegerEventSource implements SourceFunction<IntegerEvent> {

        final Collection<IntegerEvent> events;

        public IntegerEventSource (IntegerEvent... events) {
            this.events = List.of(events);
        }

        @Override
        public void run(SourceContext<IntegerEvent> ctx) throws Exception {
            for(IntegerEvent event : events) {
                ctx.collect(event);
            }
        }

        @Override
        public void cancel() {
            // no need to cancel infinite stream
        }
    }

}
