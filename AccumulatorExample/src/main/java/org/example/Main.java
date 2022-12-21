package org.example;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Example of using environment accumulator.
 *
 * All accumulators share a single namespace per job, Thus you can use the same accumulator in different operator functions of your job.
 * Flink will internally merge all accumulators with the same name.
 *
 * A note on accumulators and iterations: Currently the result of accumulators is only available after the overall job has ended.
 *
 * To implement your own accumulator you simply have to write your implementation of the Accumulator interface
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> strings = env.fromCollection(IntStream.range(0, 100).boxed().collect(Collectors.toList()))
                    .map(new MyMapFunction());


        JobExecutionResult jobExecutionResult = env.execute("Jobname");
        log.info("Num of lines = {}", jobExecutionResult.getAccumulatorResult("num-lines").toString());
        System.out.println("RESULT:\n" + jobExecutionResult);
    }

    public static class MyMapFunction extends RichMapFunction<Integer, String> {

        private final IntCounter numLines = new IntCounter();

        @Override
        public String map(Integer value) {
            this.numLines.add(1);
            return String.valueOf(value);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            getRuntimeContext().addAccumulator("num-lines", this.numLines);
        }
    }


}