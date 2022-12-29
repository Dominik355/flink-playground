package com.bilik.window;

import com.bilik.window.AllowedLateness.StringEvent;
import com.bilik.common.utils.Logger;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;


public class AllowedLatenessWithSideoutput {

    private static final Logger log = new Logger(AllowedLatenessWithSideoutput.class);


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<StringEvent> stringEvents = env.addSource(new AllowedLateness.StringEventGenerator())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<StringEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp()))
                .keyBy(StringEvent::getName)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                //.allowedLateness(Time.minutes(1)) // if we enable this, all the events will be processed, because there is no bigger gap than a minute
                .sideOutputLateData(new OutputTag<>("side-output-tag"){}) // must always be an anonymous inner class so that Flink can derive a TypeInformation for the generic type parameter
                .process(new AllowedLateness.PrinterProcess());

        DataStream<StringEvent> lateStream = stringEvents
                .getSideOutput(new OutputTag<StringEvent>("side-output-tag"){})
                .map(element -> log.println("Late element: ", element));

        env.execute();
    }

}
