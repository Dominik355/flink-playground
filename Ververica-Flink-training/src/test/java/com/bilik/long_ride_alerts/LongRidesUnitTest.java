package com.bilik.long_ride_alerts;

import com.bilik.common.ComposedKeyedProcessFunction;
import com.bilik.common.datatypes.TaxiRide;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

// needed for the Scala tests to use scala.Long with this Java test
@SuppressWarnings({"rawtypes", "unchecked"})
public class LongRidesUnitTest {

    public static final Instant BEGINNING = Instant.parse("2020-01-01T12:00:00.00Z");
    public static final Instant ONE_MINUTE_LATER = BEGINNING.plusSeconds(60);
    public static final Instant ONE_HOUR_LATER = BEGINNING.plusSeconds(60 * 60);
    public static final Instant TWO_HOURS_LATER = BEGINNING.plusSeconds(120 * 60);
    public static final Instant THREE_HOURS_LATER = BEGINNING.plusSeconds(180 * 60);

    public static TaxiRide startRide(long rideId, Instant startTime) {
        return testRide(rideId, true, startTime);
    }

    public static TaxiRide endRide(TaxiRide started, Instant endTime) {
        return testRide(started.rideId, false, endTime);
    }

    private static TaxiRide testRide(long rideId, Boolean isStart, Instant eventTime) {

        return new TaxiRide(
                rideId,
                isStart,
                eventTime,
                -73.9947F,
                40.750626F,
                -73.9947F,
                40.750626F,
                (short) 1,
                0,
                0);
    }

    private KeyedOneInputStreamOperatorTestHarness<Long, TaxiRide, Long> harness;

    private final KeyedProcessFunction<Long, TaxiRide, Long> javaExercise =
            new LongRidesExercise.AlertFunction();

    private final KeyedProcessFunction<Long, TaxiRide, Long> javaSolution =
            new LongRidesExercise.AlertFunction();

    protected ComposedKeyedProcessFunction composedAlertFunction() {
        return new ComposedKeyedProcessFunction<>(javaExercise, javaSolution);
    }

    @Before
    public void setupTestHarness() throws Exception {
        this.harness = setupHarness(composedAlertFunction());
    }

    @Test
    public void shouldUseTimersAndState() throws Exception {

        TaxiRide rideStarted = startRide(1, BEGINNING);
        harness.processElement(rideStarted.asStreamRecord());

        // check for state and timers
        assertThat(harness.numEventTimeTimers()).isGreaterThan(0);
        assertThat(harness.numKeyedStateEntries()).isGreaterThan(0);

        TaxiRide endedOneMinuteLater = endRide(rideStarted, ONE_MINUTE_LATER);
        harness.processElement(endedOneMinuteLater.asStreamRecord());

        // in this case, state and timers should be gone now
        assertThat(harness.numEventTimeTimers()).isZero();
        assertThat(harness.numKeyedStateEntries()).isZero();
    }

    @Test
    public void shouldNotAlertWithStartFirst() throws Exception {

        TaxiRide rideStarted = startRide(1, BEGINNING);
        TaxiRide endedOneMinuteLater = endRide(rideStarted, ONE_MINUTE_LATER);

        harness.processElement(rideStarted.asStreamRecord());
        harness.processElement(endedOneMinuteLater.asStreamRecord());

        assertThat(harness.getOutput()).isEmpty();
    }

    @Test
    public void shouldNotAlertWithEndFirst() throws Exception {

        TaxiRide rideStarted = startRide(1, BEGINNING);
        TaxiRide endedOneMinuteLater = endRide(rideStarted, ONE_MINUTE_LATER);

        harness.processElement(endedOneMinuteLater.asStreamRecord());
        harness.processElement(rideStarted.asStreamRecord());

        assertThat(harness.getOutput()).isEmpty();
    }

    @Test
    public void shouldAlertWithStartFirst() throws Exception {

        TaxiRide rideStarted = startRide(1, BEGINNING);
        TaxiRide endedThreeHoursLater = endRide(rideStarted, THREE_HOURS_LATER);

        harness.processElement(rideStarted.asStreamRecord());
        harness.processElement(endedThreeHoursLater.asStreamRecord());

        assertThat(resultingRideId()).isEqualTo(rideStarted.rideId);
    }

    @Test
    public void shouldAlertWithEndFirst() throws Exception {

        TaxiRide rideStarted = startRide(1, BEGINNING);
        TaxiRide endedThreeHoursLater = endRide(rideStarted, THREE_HOURS_LATER);

        harness.processElement(endedThreeHoursLater.asStreamRecord());
        harness.processElement(rideStarted.asStreamRecord());

        assertThat(resultingRideId()).isEqualTo(rideStarted.rideId);
    }

    @Test
    public void shouldNotAlertWithoutWatermarkOrEndEvent() throws Exception {

        TaxiRide rideStarted = startRide(1, BEGINNING);
        TaxiRide otherRideStartsThreeHoursLater = startRide(2, THREE_HOURS_LATER);

        harness.processElement(rideStarted.asStreamRecord());
        harness.processElement(otherRideStartsThreeHoursLater.asStreamRecord());
        assertThat(harness.getOutput()).isEmpty();

        Watermark mark2HoursLater =
                new Watermark(BEGINNING.plusSeconds(2 * 60 * 60).toEpochMilli());
        harness.processWatermark(mark2HoursLater);

        StreamRecord<Long> rideIdAtTimeOfWatermark =
                new StreamRecord<>(rideStarted.rideId, mark2HoursLater.getTimestamp());
        assertThat(harness.getOutput()).containsExactly(rideIdAtTimeOfWatermark, mark2HoursLater);
    }

    @Test
    public void shouldAlertOnWatermark() throws Exception {

        TaxiRide startOfLongRide = startRide(1, BEGINNING);
        harness.processElement(startOfLongRide.asStreamRecord());

        // Can't be done properly without some managed keyed state
        assertThat(harness.numKeyedStateEntries()).isGreaterThan(0);

        // At this point there should be no output
        ConcurrentLinkedQueue<Object> initialOutput = harness.getOutput();
        assertThat(initialOutput).isEmpty();

        Watermark mark2HoursLater =
                new Watermark(BEGINNING.plusSeconds(2 * 60 * 60).toEpochMilli());
        harness.processWatermark(mark2HoursLater);

        // Check that the result is correct
        StreamRecord<Long> rideIdAtTimeOfWatermark =
                new StreamRecord<>(startOfLongRide.rideId, mark2HoursLater.getTimestamp());
        assertThat(harness.getOutput()).containsExactly(rideIdAtTimeOfWatermark, mark2HoursLater);
    }

    private Long resultingRideId() {
        ConcurrentLinkedQueue<Object> results = harness.getOutput();
        assertThat(results.size())
                .withFailMessage("Expecting test to have exactly one result")
                .isEqualTo(1);
        StreamRecord<Long> resultingRecord = (StreamRecord<Long>) results.element();
        return resultingRecord.getValue();
    }

    private KeyedOneInputStreamOperatorTestHarness<Long, TaxiRide, Long> setupHarness(
            KeyedProcessFunction<Long, TaxiRide, Long> function) throws Exception {

        KeyedProcessOperator<Long, TaxiRide, Long> operator = new KeyedProcessOperator<>(function);

        KeyedOneInputStreamOperatorTestHarness<Long, TaxiRide, Long> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(operator, r -> r.rideId, Types.LONG);

        testHarness.setup();
        testHarness.open();

        return testHarness;
    }
}