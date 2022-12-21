package com.bilik;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Here is program that continuously subtracts 1 from a series of integers until they reach zero.
 */
public class CountdownMain {

    private static final Logger log = LoggerFactory.getLogger(CountdownMain.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        IterativeStream<Long> iteration = env.generateSequence(0, 10).iterate();

        DataStream<Long> minusOne = iteration.map(value -> value - 1);

        DataStream<Long> stillGreaterThanZero = minusOne.filter(element -> element > 0);

        iteration.closeWith(stillGreaterThanZero);

        DataStream<Long> lessThanZero = minusOne.filter(value -> {
            log.info("Is " + value + " zero?:" + (value <= 0));
            return value <= 0;
        });

        lessThanZero.print();

        JobExecutionResult jobExecutionResult = env.execute("Jobname");
        log.info("RESULT:\n" + jobExecutionResult);
    }

}
