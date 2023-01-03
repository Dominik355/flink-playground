package com.bilik.long_ride_alerts;

import com.bilik.common.datatypes.TaxiRide;
import com.bilik.long_ride_alerts.LongRidesExercise.AlertFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;

public class LongRidesExerciseTest {

    private KeyedOneInputStreamOperatorTestHarness<Long, TaxiRide, Long> harness;

    //@RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build());

    @BeforeEach
    public void setupTestHarness() throws Exception {
        this.harness = setupHarness(new AlertFunction());
    }

    @AfterEach
    public void cleanup() throws Exception {
        this.harness.close();
    }

    private KeyedOneInputStreamOperatorTestHarness<Long, TaxiRide, Long> setupHarness(AlertFunction function) throws Exception {
        KeyedProcessOperator<Long, TaxiRide, Long> operator = new KeyedProcessOperator<>(function);

        KeyedOneInputStreamOperatorTestHarness<Long, TaxiRide, Long> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(operator, r -> r.rideId, Types.LONG);

        testHarness.setup();
        testHarness.open();

        return testHarness;
    }

    @Test
    public void alertFunctionTest() throws Exception {
        AlertFunction alertFunction = new AlertFunction();

        KeyedOneInputStreamOperatorTestHarness<Long, TaxiRide, Long> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(alertFunction),
                        ride -> ride.rideId,
                        Types.LONG
                );

        System.out.println("ENTRIES: " + harness.numKeyedStateEntries());
        harness.setup();
        harness.open();

        harness.processElement(TEST_RIDES.get(1), TEST_RIDES.get(1).getEventTimeMillis());

        List<StreamRecord<? extends Long>> longRides = harness.extractOutputStreamRecords();
        System.out.println("LONG RIDES: " + longRides);
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
    ); // so expected output from this collection is : 2,4,6, (999)

}
