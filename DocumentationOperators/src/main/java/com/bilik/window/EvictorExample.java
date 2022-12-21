package com.bilik.window;

import com.bilik.window.AllowedLateness.StringEvent;
import com.bilik.common.utils.Logger;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Iterator;

/**
 * In this example, we can see, that EVICT is called before Window is created, or after Functions, like Process() has been called on window
 */
public class EvictorExample {

    private static final Logger log = new Logger(EvictorExample.class);
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StringEvent> stringEvents = env.addSource(new AllowedLateness.StringEventGenerator())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<StringEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp()))
                .keyBy(StringEvent::getName)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .evictor(new PrinterEvictor())
                //.allowedLateness(Time.minutes(1)) // if we enable this, all the events will be processed, because ther is no bigger gap than a minute
                .process(new MarkerProcess())
                .filter(event -> event.getName().equals("2"));

        env.execute();
    }

    public static class MarkerProcess extends ProcessWindowFunction<StringEvent, StringEvent, String, TimeWindow> {

        @Override
        public void process(String key, ProcessWindowFunction<StringEvent, StringEvent, String, TimeWindow>.Context context, Iterable<StringEvent> elements, Collector<StringEvent> out) throws Exception {
            elements.forEach(element -> {
                element.setName("new name"); // so we can see, if process() is called before or after evict's 'evictAfter()'
                out.collect(element);
            });
        }
    }

    public static class PrinterEvictor implements Evictor<StringEvent, TimeWindow> {

        @Override
        public void evictBefore(Iterable<TimestampedValue<StringEvent>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
            log.info("Evicting BEFORE for window: {} - {}", new Date(window.getStart()), new Date(window.getEnd()));
            for (Iterator<TimestampedValue<StringEvent>> iterator = elements.iterator(); iterator.hasNext(); ) {
                TimestampedValue<StringEvent> event = iterator.next();
                log.info("BEFORE event: " + event.getValue());
            }
            log.info("End of BEFORE eviction");
        }

        @Override
        public void evictAfter(Iterable<TimestampedValue<StringEvent>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
            log.info("Evicting AFTER for window: {} - {}", new Date(window.getStart()), new Date(window.getEnd()));
            for (Iterator<TimestampedValue<StringEvent>> iterator = elements.iterator(); iterator.hasNext(); ) {
                TimestampedValue<StringEvent> event = iterator.next();
                log.info("AFTER event: " + event.getValue());
            }
            log.info("End of AFTER eviction");
        }
    }

}
