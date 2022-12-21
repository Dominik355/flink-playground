package com.bilik.SimpleState;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DeduplicatorMain {

    private static final Logger log = LoggerFactory.getLogger(DeduplicatorMain.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromCollection(events())
                .keyBy(e -> e.key)
                .process(new Deduplicate())
                .print();
        // for modulo 4, we get only 4 records, the first for every key
        // to see what would happen without deduplicaiton, just comment process() - it just prints all elements

        JobExecutionResult jobExecutionResult = env.execute("Jobname");
        System.out.println("RESULT:\n" + jobExecutionResult);
    }

    static Collection<Event> events() {
        return IntStream.rangeClosed(0, 100)
                .mapToObj(i -> new Event(String.valueOf(i % 4), i + "-name"))
                .collect(Collectors.toList());
    }


    private static class Event {
        public final String key;
        public final String name;

        public Event(String key, String name) {
            this.key = key;
            this.name = name;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "key='" + key + '\'' +
                    ", name='" + name + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Event event = (Event) o;
            return Objects.equals(key, event.key) && Objects.equals(name, event.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, name);
        }
    }

    public static class Deduplicate extends KeyedProcessFunction<String, Event, Event> {
        ValueState<Boolean> keyWasSeen;

        @Override
        public void open(Configuration conf) {
            ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("seen", Types.BOOLEAN);
            keyWasSeen = getRuntimeContext().getState(desc);
        }

        @Override
        public void processElement(Event event, Context context, Collector<Event> out) throws Exception {
            if (keyWasSeen.value() == null) {
                out.collect(event);
                keyWasSeen.update(true);
            }
        }
    }

}