package com.bilik;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Using a WatermarkStrategy this way takes a stream and produce a new stream with timestamped elements and watermarks.
 * If the original stream had timestamps and/or watermarks already, the timestamp assigner overwrites them.
 */
public class Example1Main {

    private static final Logger log = LoggerFactory.getLogger(Example1Main.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String filePath = "/home/dominik-bilik/projects/flink-playground/WatermarkExample/src/main/java/com/bilik/file1.txt";

        DataStream<String> stream = env.readFile(
                new TextInputFormat(new Path(filePath)),
                filePath,
                FileProcessingMode.PROCESS_CONTINUOUSLY,
                100,
                BasicTypeInfo.STRING_TYPE_INFO
        );

        DataStream<MyEvent> withTimestampsAndWatermarks = stream
                .map(Example1Main::parseToEvent)
                .filter( event -> event.getSeverity() == Severity.WARNING)
                .assignTimestampsAndWatermarks((WatermarkStrategy<MyEvent>) null);

        withTimestampsAndWatermarks
                .keyBy(event -> event.getGroup().toString())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)));

        withTimestampsAndWatermarks.print();

        JobExecutionResult jobExecutionResult = env.execute("Jobname");
        System.out.println("RESULT:\n" + jobExecutionResult);
    }

    static MyEvent parseToEvent(String line) {
        String[] parts = line.split(" ");
        MyEvent event = new MyEvent();
        event.setSeverity(Severity.valueOf(parts[0]));
        event.setGroup(Group.valueOf(parts[1]));
        return event;
    }

    private static Collection<MyEvent> events(int count) {
        Collection<MyEvent> col = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            MyEvent event = new MyEvent();
            event.setSeverity(Severity.values()[ThreadLocalRandom.current().nextInt(Severity.values().length)]);
            event.setGroup(Group.values()[ThreadLocalRandom.current().nextInt(Group.values().length)]);
            col.add(event);
        }
        return col;
    }

    public static class MyEvent {
        private Severity severity;
        private Group group;

        public Severity getSeverity() {
            return severity;
        }

        public void setSeverity(Severity severity) {
            this.severity = severity;
        }

        public Group getGroup() {
            return group;
        }

        public void setGroup(Group group) {
            this.group = group;
        }
    }

    enum Severity {
        WARNING,INFORMATIVE,DEADLY;
    }

    enum Group {
        USER,ADMIN,EMPLOYEE
    }

}