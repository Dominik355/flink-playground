Testing of Stateless and timeless operators is easy, they can be tested just by themselves.\
Such a test might look like this:
    
    public class IncrementFlatMapFunctionTest {

        @Test
        public void testIncrement() throws Exception {
            // instantiate your function
            IncrementFlatMapFunction incrementer = new IncrementFlatMapFunction();
    
            Collector<Integer> collector = mock(Collector.class);
    
            // call the methods that you have implemented
            incrementer.flatMap(2L, collector);
    
            //verify collector was called with the right output
            Mockito.verify(collector, times(1)).collect(3L);
        }
    }

But it gets interesting with adding state and time. For this Flink comes with a collection of so called test harnesses, \
which can be used to test such user-defined functions as well as custom operators:
- **OneInputStreamOperatorTestHarness** (for operators on DataStreams)
- **KeyedOneInputStreamOperatorTestHarness** (for operators on KeyedStreams)
- **TwoInputStreamOperatorTestHarness** (for operators of ConnectedStreams of two DataStreams)
- **KeyedTwoInputStreamOperatorTestHarness** (for operators on ConnectedStreams of two KeyedStreams)

To use the test harnesses a set of additional dependencies is needed. (https://nightlies.apache.org/flink/flink-docs-master/docs/dev/configuration/testing/)

The test harnesses can be used to push records and watermarks into your user-defined functions or custom operators, \
control processing time and finally assert on the output of the operator (including side outputs)

---
To see more tests with Harness, see class like : org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorTest

For more examples on how to use the ProcessFunctionTestHarnesses in order to test the different flavours of the ProcessFunction, \
e.g. KeyedProcessFunction, KeyedCoProcessFunction, BroadcastProcessFunction, etc, the user is encouraged to look at the \
ProcessFunctionTestHarnessesTest.

---
**JUnit Rule MiniClusterWithClientResource**
Apache Flink provides a JUnit rule called MiniClusterWithClientResource for testing complete jobs against a local, \
embedded mini cluster. called MiniClusterWithClientResource.\
To use MiniClusterWithClientResource one additional dependency (test scoped) is needed.

    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-test-utils</artifactId>
        <version>1.16.0</version>    
        <scope>test</scope>
    </dependency>

This cluster can be creeated like this:

    @ClassRule
     public static MiniClusterWithClientResource flinkCluster =
         new MiniClusterWithClientResource(
             new MiniClusterResourceConfiguration.Builder()
                 .setNumberSlotsPerTaskManager(2)
                 .setNumberTaskManagers(1)
                 .build());

And then, it is automatically used as environment in:

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



- In order not to copy your whole pipeline code from production to test, make sources and sinks pluggable in your \
production code and inject special test sources and test sinks in your tests.
- The static variable in CollectSink is used here because Flink serializes all operators before distributing them across \
a cluster. Communicating with operators instantiated by a local Flink mini cluster via static variables is one way \
around this issue. Alternatively, you could write the data to files in a temporary directory with your test sink.
- You can implement a custom parallel source function for emitting watermarks if your job uses event time timers.
- It is recommended to always test your pipelines locally with a parallelism > 1 to identify bugs which only surface \
for the pipelines executed in parallel.
- Prefer @ClassRule over @Rule so that multiple tests can share the same Flink cluster. Doing so saves a significant \
amount of time since the startup and shutdown of Flink clusters usually dominate the execution time of the actual tests.
- If your pipeline contains custom state handling, you can test its correctness by enabling checkpointing and restarting \
the job within the mini cluster. For this, you need to trigger a failure by throwing an exception from (a test-only) \
user-defined function in your pipeline.
