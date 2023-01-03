package com.bilik.hourly_tips;

import com.bilik.common.FlinkMiniClusterExtension;
import com.bilik.common.ParallelTestSource;
import com.bilik.common.TestSink;
import com.bilik.common.datatypes.TaxiFare;
import com.bilik.common.utils.DataGenerator;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/**
 * Up to Date with v1.16
 *
 * This is kinda tricky, because flink has no full support for Junit5
 *
 * Main port is replacing ClassRules with Extensions. As we can see, com.bilik.common.MiniClusterWithClientResource
 * extends ExternalResource, which is a base class for Rules that set up an external resource before
 * a test.
 *
 * We got 2 options ot implement it into Junit5 :
 * https://stackoverflow.com/questions/73297418/how-is-flink-integration-testing-working-without-the-documented-flink-test-utils
 *
 * 1. Create own Extension (See {@link com.bilik.common.FlinkMiniClusterExtension})
 * Use like this: "@ExtendWith(FlinkMiniClusterExtension.class)"
 *
 *
 * 2. Use Experimental Extension from Flink
 * See class {@link org.apache.flink.test.junit5.MiniClusterExtension}
 * This class is still just Experimental, but what it does ?
 * It internally uses InternalMiniClusterExtension, which internally uses MiniClusterResource,
 * which is again Junit4 classRule Resource. So we do the same with extra stuff of clearing
 * the environment after every test and setting it up again before every test
 *
 * How fast are they ? both probably same
 */
//@ExtendWith(FlinkMiniClusterExtension.class) //comment to use Flink Extension
public class HourlyTipsTestJunit5 {

    // Comment to use Own Extension
    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build());

    @Test
    public void testOneDriverOneTip() throws Exception {
        TaxiFare one = testFare(1, t(0), 1.0F);

        ParallelTestSource<TaxiFare> source = new ParallelTestSource<>(one);

        Tuple3<Long, Long, Float> expected = Tuple3.of(t(60).toEpochMilli(), 1L, 1.0F);

        assertThat(results(source)).containsExactly(expected);
    }

    @Test
    public void testTipsAreSummedByHour() throws Exception {
        TaxiFare oneIn1 = testFare(1, t(0), 1.0F);
        TaxiFare fiveIn1 = testFare(1, t(15), 5.0F);
        TaxiFare tenIn2 = testFare(1, t(90), 10.0F);

        ParallelTestSource<TaxiFare> source = new ParallelTestSource<>(oneIn1, fiveIn1, tenIn2);

        Tuple3<Long, Long, Float> hour1 = Tuple3.of(t(60).toEpochMilli(), 1L, 6.0F);
        Tuple3<Long, Long, Float> hour2 = Tuple3.of(t(120).toEpochMilli(), 1L, 10.0F);

        assertThat(results(source)).containsExactlyInAnyOrder(hour1, hour2);
    }

    @Test
    public void testMaxAcrossDrivers() throws Exception {
        TaxiFare oneFor1In1 = testFare(1, t(0), 1.0F);
        TaxiFare fiveFor1In1 = testFare(1, t(15), 5.0F);
        TaxiFare tenFor1In2 = testFare(1, t(90), 10.0F);
        TaxiFare twentyFor2In2 = testFare(2, t(90), 20.0F);
        TaxiFare zeroFor3In2 = testFare(3, t(70), 0.0F);
        TaxiFare zeroFor4In2 = testFare(4, t(70), 0.0F);
        TaxiFare oneFor4In2 = testFare(4, t(80), 1.0F);
        TaxiFare tenFor5In2 = testFare(5, t(100), 10.0F);

        ParallelTestSource<TaxiFare> source =
                new ParallelTestSource<>(
                        oneFor1In1,
                        fiveFor1In1,
                        tenFor1In2,
                        twentyFor2In2,
                        zeroFor3In2,
                        zeroFor4In2,
                        oneFor4In2,
                        tenFor5In2);

        Tuple3<Long, Long, Float> hour1 = Tuple3.of(t(60).toEpochMilli(), 1L, 6.0F);
        Tuple3<Long, Long, Float> hour2 = Tuple3.of(t(120).toEpochMilli(), 2L, 20.0F);

        assertThat(results(source))
                .containsExactlyInAnyOrder(hour1, hour2);
    }

    //**************************** HELPER METHODS ****************************

    public Instant t(int minutes) {
        return DataGenerator.BEGINNING.plus(Duration.ofMinutes(minutes));
    }

    private TaxiFare testFare(long driverId, Instant startTime, float tip) {
        return new TaxiFare(0, 0, driverId, startTime, "", tip, 0F, 0F);
    }

    protected List<Tuple3<Long, Long, Float>> results(SourceFunction<TaxiFare> source) throws Exception {
        TestSink<Tuple3<Long, Long, Float>> sink = new TestSink<>();
        JobExecutionResult jobResult = new HourlyTipsSolution(source, sink).execute();
        return sink.getResults(jobResult);
    }

}