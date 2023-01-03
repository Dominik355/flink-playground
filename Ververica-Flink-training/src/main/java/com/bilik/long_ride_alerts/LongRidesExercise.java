package com.bilik.long_ride_alerts;

import com.bilik.common.datatypes.TaxiRide;
import com.bilik.common.sources.LongRidesGenerator;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.time.*;
import java.util.List;

/**
 * The "Long Ride Alerts" exercise.
 *
 * LEAKS:
 * 1. The START event is missing. Then END event will sit in state indefinitely
 * 2. The END event arrives after the timer has fired and cleared the state. In this case the END event will be stored in state indefinitely
 */
public class LongRidesExercise {
    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<Long> sink;

    /** Creates a job using the source and sink provided. */
    public LongRidesExercise(SourceFunction<TaxiRide> source, SinkFunction<Long> sink) {
        this.source = source;
        this.sink = sink;
    }

    /**
     * Creates and executes the long rides pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiRide> rides = env.addSource(source);

        WatermarkStrategy<TaxiRide> watermarkStrategy =
                WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner(
                                (ride, streamRecordTimestamp) -> ride.getEventTimeMillis());

        // pipeline itself
        rides.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ride -> ride.rideId)
                .process(new AlertFunction())
                .addSink(sink);

        return env.execute("Long Taxi Rides");
    }

    public static void main(String[] args) throws Exception {
        LongRidesExercise job =
                new LongRidesExercise(new LongRidesGenerator(TEST_RIDES), new PrintSinkFunction<>());

        job.execute();
    }

    /**
     * We want to give alert as soon as possible, so timer is set exactly for 2 hours.
     */
    public static class AlertFunction extends KeyedProcessFunction<Long, TaxiRide, Long> {

        private ValueState<TaxiRide> rideState;

        private static final int RIDE_LENGTH_MILIS = 1000 * 60 * 60 * 2;
        private static final int TIMER_TOLERANCE_MILIS = 1000;

        @Override
        public void open(Configuration config) throws Exception {
            rideState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("last ride", TaxiRide.class));
        }

        @Override
        public void processElement(TaxiRide ride, Context context, Collector<Long> out) throws Exception {
            TaxiRide savedRide = rideState.value();
            if (savedRide == null) {
                rideState.update(ride);
                 if (ride.isStart) {
                     context.timerService().registerEventTimeTimer(getTimerTime(ride));
                 }
            } else {
                if (ride.isStart) {
                    if (rideTooLong(ride, savedRide)) {
                        out.collect(ride.rideId);
                    }
                } else {
                    context.timerService().deleteEventTimeTimer(getTimerTime(savedRide));
                    if (rideTooLong(savedRide, ride)) {
                        out.collect(ride.rideId);
                    }
                }
                rideState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Long> out) throws Exception {
            if (rideState.value() != null) {
                out.collect(context.getCurrentKey());
            }
        }

        private boolean rideTooLong(TaxiRide startEvent, TaxiRide endEvent) {
            return Duration.between(startEvent.eventTime, endEvent.eventTime)
                    .compareTo(Duration.ofHours(2)) > 0;
        }

        private long getTimerTime(TaxiRide ride) throws RuntimeException {
            if (ride.isStart) {
                return ride.getEventTimeMillis() + RIDE_LENGTH_MILIS + TIMER_TOLERANCE_MILIS;
            } else {
                throw new RuntimeException("Can not get start time from END event.");
            }
        }
    }

    static final LocalDate DEFAULT_DATE = LocalDate.now();

    /** S - start , E - End
     * RideId             14:00             15:00             16:00             17:00
     *    1                  S                                  E                       - almost exactly 2 hours (before timer fires)
     *    2                  S                                                          - no end
     *    3               E  S                                                          - ends before start
     *    4                  S                                                    E     - ends too late
     *    5                  S                          E                               - ends earlier than after 2 hours
     *    6                  S                                      E                   - ends little after 2 hours (after timer fires)
     */
    static final List<TaxiRide> TEST_RIDES = List.of(
            new TaxiRide(1, true, LocalDateTime.of(DEFAULT_DATE, LocalTime.of(14, 0, 0)).toInstant(ZoneOffset.UTC), 0F, 0F, 0F, 0F, (short) 1, 0, 1),
            new TaxiRide(2, true, LocalDateTime.of(DEFAULT_DATE, LocalTime.of(14, 0, 0)).toInstant(ZoneOffset.UTC), 0F, 0F, 0F, 0F, (short) 1, 0, 1),
            new TaxiRide(3, false, LocalDateTime.of(DEFAULT_DATE, LocalTime.of(13, 51, 54)).toInstant(ZoneOffset.UTC), 0F, 0F, 0F, 0F, (short) 1, 0, 1),
            new TaxiRide(4, true, LocalDateTime.of(DEFAULT_DATE, LocalTime.of(14, 0, 34)).toInstant(ZoneOffset.UTC), 0F, 0F, 0F, 0F, (short) 1, 0, 1),
            new TaxiRide(5, true, LocalDateTime.of(DEFAULT_DATE, LocalTime.of(14, 0, 44)).toInstant(ZoneOffset.UTC), 0F, 0F, 0F, 0F, (short) 1, 0, 1),
            new TaxiRide(6, true, LocalDateTime.of(DEFAULT_DATE, LocalTime.of(14, 0, 44)).toInstant(ZoneOffset.UTC), 0F, 0F, 0F, 0F, (short) 1, 0, 1),
            new TaxiRide(3, true, LocalDateTime.of(DEFAULT_DATE, LocalTime.of(14, 0, 45)).toInstant(ZoneOffset.UTC), 0F, 0F, 0F, 0F, (short) 1, 0, 1),
            new TaxiRide(5, false, LocalDateTime.of(DEFAULT_DATE, LocalTime.of(15, 43, 51)).toInstant(ZoneOffset.UTC), 0F, 0F, 0F, 0F, (short) 1, 0, 1),
            new TaxiRide(1, false, LocalDateTime.of(DEFAULT_DATE, LocalTime.of(16, 0, 0)).toInstant(ZoneOffset.UTC), 0F, 0F, 0F, 0F, (short) 1, 0, 1),
            new TaxiRide(6, false, LocalDateTime.of(DEFAULT_DATE, LocalTime.of(16, 7, 21)).toInstant(ZoneOffset.UTC), 0F, 0F, 0F, 0F, (short) 1, 0, 1),
            new TaxiRide(4, false, LocalDateTime.of(DEFAULT_DATE, LocalTime.of(17, 12)).toInstant(ZoneOffset.UTC), 0F, 0F, 0F, 0F, (short) 1, 0, 1)
    ); // so expected output from this collection is : 2,4,6 (with 4 probably doubled)
}