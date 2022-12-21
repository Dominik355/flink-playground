package com.bilik.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import static com.bilik.window.AllowedLateness.StringEvent.of;


public class AllowedLateness {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StringEvent> stringEvents = env.addSource(new StringEventGenerator())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<StringEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp()));

        DataStream<StringEvent> keyed = stringEvents
                .keyBy(StringEvent::getName)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .allowedLateness(Time.minutes(1)) // if we enable this, all the events will be processed, because ther is no bigger gap than a minute
                .process(new PrinterProcess());

        DataStream<String> stringified = keyed
                .filter(event -> event.name.equals("2"))
                .map(StringEvent::toString);

        stringified.print();

        env.execute();
    }

    public static class StringEvent implements Serializable {
        private String name;
        private long timestamp;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public static StringEvent of(String name, Date date) {
            StringEvent event = new StringEvent();
            event.name = name;
            event.timestamp = date.getTime();
            return event;
        }

        @Override
        public String toString() {
            return "StringEvent{" +
                    "name='" + name + '\'' +
                    ", timestamp=" + new Date(timestamp) +
                    '}';
        }
    }

    public static class PrinterProcess extends ProcessWindowFunction<StringEvent, StringEvent, String, TimeWindow> {

        @Override
        public void process(String key, ProcessWindowFunction<StringEvent, StringEvent, String, TimeWindow>.Context context, Iterable<StringEvent> elements, Collector<StringEvent> out) throws Exception {
            if (key.equals("2")) System.out.println(key + " ---- START ----- " + new Date(context.window().getStart()) + " - " + new Date(context.window().getEnd()));
            elements.forEach(element -> {
                if (key.equals("2")) System.out.println(String.format("Key %s, Window %s. Element: %s", key, new Date(context.window().getStart()) + " - " + new Date(context.window().getEnd()), element));
                out.collect(element);
            });
            // with allowedLateness - in this process function we get again the whole window with all the events, not just elements which were late
            // WHAT IS WORSE - it emits into output stream every time, so events are douplicating - but we can do nothing baout that, it's also in documentation.
            // for key2, we get here 3 times, because watermarks emits after every new event (if we use Thread.sleep() lower than watermark value (200 milis), we can lower this
            if (key.equals("2")) System.out.println(key + " ---- END ----- " + new Date(context.window().getStart()) + " - " + new Date(context.window().getEnd()));
        }
    }

    public static class StringEventGenerator implements SourceFunction<StringEvent> {

        @Override
        public void run(SourceFunction.SourceContext<StringEvent> ctx) throws Exception {

            for (StringEvent event : EVENTS) {
                //System.out.println("emitting: " + event);
                ctx.collect(event);
                Thread.sleep(250);
                // default watermark emit time is 200 miliseconds, so this should be much enough
            }
        }

        @Override
        public void cancel() {}

        private static final List<StringEvent> EVENTS = List.of(
                of("1", new Date(122, 11, 15, 14, 1, 11)),
                of("2", new Date(122, 11, 15, 14, 1, 02)),
                of("1", new Date(122, 11, 15, 14, 1, 57)),
                of("1", new Date(122, 11, 15, 14, 2, 06)),
                of("1", new Date(122, 11, 15, 14, 2, 11)),
                of("1", new Date(122, 11, 15, 14, 2, 34)),
                //of("1", new Date(122, 11, 15, 14, 3, 43)),  // - if we use this, than with allowedLateness we again drop elements under, because allowLateness depends on time of watermarks, and our lateness is 1 minute
                                                            // so with this event, we forward time about more than 1 minute from window 1-2 (only key2 2:55 will be processed, because it is still in 1 minute lateness for
                                                            // window 2-3, we would have to change this to 4:xx to not process that 2:55 event)
                of("1", new Date(122, 11, 15, 14, 1, 44)), // without allowedLateness - will be dropped
                of("2", new Date(122, 11, 15, 14, 1, 43)), // without allowedLateness - will be dropped
                of("2", new Date(122, 11, 15, 14, 1, 55)), // without allowedLateness - will be dropped
                of("2", new Date(122, 11, 15, 14, 2, 55)), // without allowedLateness - will be dropped
                of("2", new Date(122, 11, 15, 14, 1, 57))
                // why will events with key2 be dropped, even if they fulfill the ascending timestamap for key2?
                // because in the meantime, events arrived from key1, which, for example, went to the second
                // window for minute 2-3. And therefore, even when DataStream is participating in streams
                // according to the key, these streams share watermark information. That is, if an event with
                // a time component of 2 minutes arrives in the stream for key1, then for key2 the events
                // within 1 minute are already ignored, even if no event has yet arrived in that stream that
                // would end the given window
        );
    }

}