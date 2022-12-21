package com.bilik.window;

import com.bilik.common.datatypes.TaxiFare;
import com.bilik.common.sources.TaxiFareGenerator;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * The Hourly Tips exercise from the Flink training.
 *
 * The task of the exercise is to first calculate the total tips collected by each driver, hour
 * by hour, and then from that stream, find the highest tip total in each hour.
 */
public class HourlyTips_WindowFunctionsShowcase {

    private final SourceFunction<TaxiFare> source;

    public HourlyTips_WindowFunctionsShowcase(SourceFunction<TaxiFare> source) {
        this.source = source;
    }

    public static void main(String[] args) throws Exception {
        HourlyTips_WindowFunctionsShowcase job = new HourlyTips_WindowFunctionsShowcase(new TaxiFareGenerator());
        //job.executeProcess(); // to see process
        job.executeProcessAggregation(); // to see aggregation with process
    }

    /**
     * keyBy(driverId) -> windows(hourly) -> aggregate tips in window -> max
     */
    public JobExecutionResult executeProcess() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiFare> fares = env.addSource(source)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TaxiFare>forMonotonousTimestamps()
                            .withTimestampAssigner((element, recordTimestamp) -> element.getEventTimeMillis()));

        DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
                .keyBy(fare -> fare.driverId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new AddTips()); // THIS IS SPECIFIC PART


        DataStream<String> stringed = hourlyTips
                .map(tuple3 -> String.format("time: %s, driverId: %s, sumOfTips: %s", new Date(tuple3.f0), tuple3.f1, tuple3.f2));
        stringed.addSink(new PrintSinkFunction<>());

        // gives max tip of overall drivers by an hour
        DataStream<String> hourlyMax = hourlyTips.windowAll(TumblingEventTimeWindows.of(Time.hours(1))).maxBy(2)
                .map(tuple3 -> String.format("Max is - [time: %s, driverId: %s, sumOfTips: %s]", new Date(tuple3.f0), tuple3.f1, tuple3.f2));
        hourlyMax.addSink(new PrintSinkFunction<>());

//        // gives max tip by an our for every driverId (key)
//        DataStream<String> hourlyMaxKeyed = hourlyTips.keyBy(t -> t.f0).maxBy(2)
//                .map(tuple3 -> String.format("Keyed Max is - [time: %s, driverId: %s, sumOfTips: %s]", new Date(tuple3.f0), tuple3.f1, tuple3.f2));
//        hourlyMaxKeyed.addSink(new PrintSinkFunction<>());


        return env.execute("Hourly Tips");
    }

    /**
     * What was wrong with process ? It has to buffer all the data in the window.
     * So better would be to use aggregation on reduce, yea, we can do that, just create aggregator, which would have Tuple<long,float>,
     * where long is driverId and float is sumOfTips. But we are missing timestamp of that particular window, that's why we used process.
     *
     * But there is a way to combine these 2, to get fast approach of aggregation with context information of process().
     *
     * A ProcessWindowFunction can be combined with either a ReduceFunction, or an AggregateFunction to incrementally
     * aggregate elements as they arrive in the window. When the window is closed, the ProcessWindowFunction will be
     * provided with the aggregated result. This allows it to incrementally compute windows while having access to the
     * additional window meta information of the ProcessWindowFunction.
     */
    public JobExecutionResult executeProcessAggregation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiFare> fares = env.addSource(source)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TaxiFare>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.getEventTimeMillis()));

        DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
                .keyBy(fare -> fare.driverId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new AggregateTips(), new ProcessAggregation()); // THIS IS SPECIFIC PART

        hourlyTips.addSink(new PrintSinkFunction<>());

        return env.execute();
        /*
        So what happened here ? how did we combine aggregation and process
        Well, for every element, the aggregation funciton has been used, and after all the elements has been proccessed,
        process function has been called also. But instead of buffering all the elements, the iterator of process function,
        has only 1 element, which is final aggregated element from aggregation function. That is why output of aggregation
        has to be the same as input of process function. So we got speed of aggregation and information of process function. TADAAA
        Sequence would be like this:
        createAccumulator() -> add() -> add() -> ... -> getResult() -> process()
        |__________________________________________________________|  |_________|
                   aggregation                                          process
         */
    }

    // PROCESS FUNCTIONS
    public static class AddTips extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {

        @Override
        public void process(
                Long key,
                Context context,
                Iterable<TaxiFare> fares,
                Collector<Tuple3<Long, Long, Float>> out) {

            float sumOfTips = 0F;
            for (TaxiFare f : fares) {
                sumOfTips += f.tip;
            }
            out.collect(Tuple3.of(context.window().getEnd(), key, sumOfTips));
        }
    }


    // AGGREGATION + PROCESS FUNCTIONS
    public static class AggregateTips implements AggregateFunction<TaxiFare, Float, Float> {

        @Override
        public Float createAccumulator() {
            System.out.println("Creating accumulator");
            return 0f;
        }

        @Override
        public Float add(TaxiFare value, Float accumulator) {
            System.out.println("Adding : " + value.tip + " to: " + accumulator);
            return accumulator + value.tip;
        }

        @Override
        public Float getResult(Float accumulator) {
            System.out.println("returning result: " + accumulator);
            return accumulator;
        }

        @Override
        public Float merge(Float a, Float b) {
            System.out.println("merging " + a + " + " + b);
            return a + b;
        }
    }

    public static class ProcessAggregation extends ProcessWindowFunction<Float, Tuple3<Long, Long, Float>, Long, TimeWindow> {

        @Override
        public void process(
                Long key,
                Context context,
                Iterable<Float> taxiFares,
                Collector<Tuple3<Long, Long, Float>> out) throws Exception {

            System.out.println("HERE comes fares for key " + key + " in window: " + new Date(context.window().getEnd()));
            for (Float f : taxiFares) {
                System.out.println(key + ": " + f);
            }
            System.out.println("aaad we are done here");
        }
    }
}