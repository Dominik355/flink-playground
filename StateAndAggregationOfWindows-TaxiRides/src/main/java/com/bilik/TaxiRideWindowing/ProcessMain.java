package com.bilik.TaxiRideWindowing;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import static com.bilik.TaxiRideWindowing.Ride.createRides;

/**
 * This is same as Main, just instead of : keyby, window, aggregate, we do just keyby and process.
 * Proces takes care of windowing and aggregating.
 */
public class ProcessMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Ride> rides = env.fromCollection(createRides());

        rides
                .filter(Ride::isStarted)
                .keyBy(Ride::getTaxiId)
                .process(new CountingWindow(Time.minutes(1)))
                .print();

        env.execute();

    }

    public static class CountingWindow extends KeyedProcessFunction<Long, Ride, Tuple3<Long, Long, Integer>> {

        private final long durationMsec;

        public CountingWindow(Time duration) {
            durationMsec = duration.toMilliseconds();
        }

        // The central data structure used here is this map from window timestamps to counters,
        // where we will keep track of how many events we have seen for each window
        private transient MapState<Long, Integer> counter;

        // here we setup the state we will use
        @Override
        public void open(Configuration conf) {
            MapStateDescriptor<Long, Integer> desc =
                    new MapStateDescriptor<>("hourlyRides", Long.class, Integer.class);
            counter = getRuntimeContext().getMapState(desc);
        }

        //  for handling each incoming event
        @Override
        public void processElement(Ride ride,
                                   Context ctx,
                                   Collector<Tuple3<Long, Long, Integer>> out) throws Exception {
            long eventTime = ride.getRideTime().getTime();
            TimerService timerService = ctx.timerService();

            if (eventTime > timerService.currentWatermark()) {

                // Round up eventTime to the end of the window containing this event.
                long endOfWindow = (eventTime - (eventTime % durationMsec) + durationMsec - 1);

                // Count this ride.
                Integer count = counter.get(endOfWindow);
                count = (count == null) ? 1 : count + 1;
                counter.put(endOfWindow, count);

                // Schedule a callback for when the window has been completed.
                timerService.registerEventTimeTimer(endOfWindow);
            } else {
                // Process late events?
            }

        }

        // called when timers fire
        @Override
        public void onTimer(long timestamp,
                            OnTimerContext context,
                            Collector<Tuple3<Long, Long, Integer>> out) throws Exception {

            // Look up the result for the hour that just ended.
            Integer count = counter.get(timestamp);

            out.collect(new Tuple3<>(timestamp, context.getCurrentKey(), count));

            // Clear the state for this window.
            counter.remove(timestamp);
        }

    }

}
