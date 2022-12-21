package com.bilik.window;

import com.bilik.common.utils.Logger;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Date;

/**
 * We can see, tht for allowedLateness we can get multiple FIRING for every late element
 */
public class HowTriggerFireForAllowedLateness {

    private static final Logger log = new Logger(HowTriggerFireForAllowedLateness.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<AllowedLateness.StringEvent> stringEvents = env.addSource(new AllowedLateness.StringEventGenerator())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<AllowedLateness.StringEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp()));

        DataStream<AllowedLateness.StringEvent> keyed = stringEvents
                .filter(event -> event.getName().equals("1")) // there is only 1 late arrival som key 1, so easier debug
                .keyBy(AllowedLateness.StringEvent::getName)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .trigger(CopiedEventTimeTrigger.create())
                .allowedLateness(Time.minutes(1))
                .process(new AllowedLateness.PrinterProcess());

        DataStream<String> stringified = keyed.map(AllowedLateness.StringEvent::toString);

        stringified.print();

        env.execute();
    }

    /**
     * This is just copied EventTimeTrigger with added logs
     */
    public static class CopiedEventTimeTrigger extends Trigger<Object, TimeWindow> {
        private static final long serialVersionUID = 1L;

        private CopiedEventTimeTrigger() {}

        @Override
        public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            log.info("onElement for object [{}]. Window max: {} || current watermark: {}", element, new Date(window.maxTimestamp()), new Date(ctx.getCurrentWatermark()));
            if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
                log.info("FIRING");
                // if the watermark is already past the window fire immediately
                return TriggerResult.FIRE;
            } else {
                log.info("CONTINUE");
                ctx.registerEventTimeTimer(window.maxTimestamp());
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
            TriggerResult result = time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
            log.info("onEventTime {} - {}. RESULT: {}", new Date(window.getStart()), new Date(window.getEnd()), result);
            return result;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            log.info("onProcessingTime {} - {}", new Date(window.getStart()), new Date(window.getEnd()));
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            log.info("clear() for window: {} - {}", new Date(window.getStart()), new Date(window.getEnd()));
            ctx.deleteEventTimeTimer(window.maxTimestamp());
        }

        @Override
        public boolean canMerge() {
            System.out.println("can merge ?");
            return true;
        }

        @Override
        public void onMerge(TimeWindow window, OnMergeContext ctx) {
            System.out.println("on merge");
            // only register a timer if the watermark is not yet past the end of the merged window
            // this is in line with the logic in onElement(). If the watermark is past the end of
            // the window onElement() will fire and setting a timer here would fire the window twice.
            long windowMaxTimestamp = window.maxTimestamp();
            if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
                ctx.registerEventTimeTimer(windowMaxTimestamp);
            }
        }

        @Override
        public String toString() {
            return "EventTimeTrigger()";
        }

        /**
         * Creates an event-time trigger that fires once the watermark passes the end of the window.
         *
         * <p>Once the trigger fires all elements are discarded. Elements that arrive late immediately
         * trigger window evaluation with just this one element.
         */
        public static CopiedEventTimeTrigger create() {
            System.out.println("creating new trigger");
            return new CopiedEventTimeTrigger();
        }
    }

}