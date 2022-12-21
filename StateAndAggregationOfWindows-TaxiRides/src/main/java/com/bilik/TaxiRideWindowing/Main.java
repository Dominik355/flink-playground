package com.bilik.TaxiRideWindowing;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import static com.bilik.TaxiRideWindowing.Ride.createRides;

public class Main {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Ride> rides = env.fromCollection(createRides())
                .assignTimestampsAndWatermarks(new MyWatermarkStrategy());

        // 100 rides generated with 1 second difference, so windowing should create max 6 windows for each ID
        rides.broadcast()
                .filter(Ride::isStarted)
                .keyBy(Ride::getTaxiId)
                .window(TumblingEventTimeWindows.of(Time.seconds(20)))
                .aggregate(new CountTaxiRides(), new WrapWindowResult())
                .print();

        env.execute();
    }

    private static class CountTaxiRides implements AggregateFunction<Ride, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Ride ride, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    /**
     * For each element from the particular window, this assigns window end timestamp and put it these elements into stream again.
     * So it's kinda like map() specific for every window
     */
    private static class WrapWindowResult extends ProcessWindowFunction<Integer, Tuple4<String, Long, Long, Integer>, Long, TimeWindow> {

        @Override
        public void process(
                Long taxiId,
                Context context,
                Iterable<Integer> iterableContainingPreaggregatedCount,
                Collector<Tuple4<String, Long, Long, Integer>> out) throws Exception {

            Tuple4<String, Long, Long, Integer> result = new Tuple4<>(
                    "TOTO JE KONIEC OKNA",
                    context.window().getEnd(),
                    taxiId,
                    iterableContainingPreaggregatedCount.iterator().next());

            out.collect(result);
        }
    }

    public static class MyWatermarkStrategy implements WatermarkStrategy<Ride> {

        @Override
        public WatermarkGenerator<Ride> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new NoWatermarksGenerator<>();
        }

        @Override
        public TimestampAssigner<Ride> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new RideTimestmapExtractor(Time.seconds(10));
        }
    }

    /**
     * This is needed in order to assignt timesstamp/exract for every element, so we can do windows and stuff
     */
    public static class RideTimestmapExtractor extends BoundedOutOfOrdernessTimestampExtractor<Ride> {

        public RideTimestmapExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Ride element) {
            return element.getRideTime().getTime();
        }
    }

}
