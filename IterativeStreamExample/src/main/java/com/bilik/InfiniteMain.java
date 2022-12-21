package com.bilik;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Co je IterativeStream ??
 * IterativeStream allows us to iterate multiple (infinite) times through the stream or rather throught part of the stream.
 * terative streaming programs implement a step function and embed it into an IterativeStream.
 * As a DataStream program may never finish, there is no maximum number of iterations. Instead,
 * you need to specify which part of the stream is fed back to the iteration and which part is forwarded downstream
 *
 * In this example, we are iterating over the whole stream.
 * CloseWith is method, which defines the end of the iterative program part that will be fed back to the start of the iteration.
 *
 * USECASE:
 * A common usage pattern for streaming iterations is to use output splitting to send a part of the closing data stream to the head.
 */
public class InfiniteMain {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        IterativeStream<Long> iteration = env.generateSequence(0, 40).iterate();

        DataStream<Long> iterationBody = iteration.map(element -> {
            System.out.println("mapping element: " + element);
            return element;
        });

        iteration.closeWith(iterationBody.filter(element -> element < 0));
        DataStream<Long> output = iterationBody.filter(element -> element > 10);

        output.print();

        JobExecutionResult jobExecutionResult = env.execute("Jobname");
        System.out.println("RESULT:\n" + jobExecutionResult);
    }

}
