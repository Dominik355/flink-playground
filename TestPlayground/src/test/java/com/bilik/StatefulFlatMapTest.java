package com.bilik;

import com.bilik.TestCase1.StatefulFlatMap;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Before;
import org.junit.Test;

public class StatefulFlatMapTest {

    private OneInputStreamOperatorTestHarness<String, String> testHarness;
    private StatefulFlatMap statefulFlatMapFunction;

    @Before
    public void setupTestHarness() throws Exception {

        //instantiate user-defined function
        statefulFlatMapFunction = new StatefulFlatMap();

        // wrap user defined function into the corresponding operator
        testHarness = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunction));

        // optionally configured the execution environment
        testHarness.getExecutionConfig().setAutoWatermarkInterval(50);

        // open the test harness (will also call open() on RichFunctions)
        testHarness.open();
    }

    @Test
    public void testingStatefulFlatMapFunction() throws Exception {

        //push (timestamped) elements into the operator (and hence user defined function)
        testHarness.processElement("one", 50L);
        testHarness.processElement("two", 100L);

        //trigger event time timers by advancing the event time of the operator with a watermark
        testHarness.processWatermark(100L);

        //trigger processing time timers by advancing the processing time of the operator directly
        testHarness.setProcessingTime(100L);

        //retrieve list of emitted records for assertions
        System.out.println(testHarness.getOutput());
    }


}

