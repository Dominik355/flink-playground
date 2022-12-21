package com.bilik;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Main {

    /**
     * To create a socket text stream, you cna for example use 'OpenBSD netcat'
     * Run: "nc -lk 9999" in console.
     * Just type some words hitting return for a new word. These will be the input to the word count program.
     *
     * "sum(1)"
     * If you want to see counts greater than 1, type the same word again and again within 5 seconds (increase the window size from 5 seconds if you cannot type that fast ☺).
     */

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        dataStream.print();

        JobExecutionResult result = env.execute("Window WordCount");
        System.out.println("RESULT:\n" + result);
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] splitted = sentence.split(" ");
            for (int i = 0; i < splitted.length; i++) {
                out.collect(new Tuple2<>(splitted[i], i));
            }
        }
    }


}