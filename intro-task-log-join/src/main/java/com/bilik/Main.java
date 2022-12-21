package com.bilik;

import com.bilik.domain.Click;
import com.bilik.domain.Feedback;
import com.bilik.domain.Impression;
import com.bilik.domain.Result;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;

public class Main {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FileSource<Click> clicks =
                FileSource.forRecordStreamFormat(
                        createCsvReaderFormat(Click.class),
                        Path.fromLocalFile(new File("/home/dominik-bilik/projects/flink-log-join/src/main/resources/source/clicks.csv"))
                ).build();

        FileSource<Feedback> feedbacks =
                FileSource.forRecordStreamFormat(
                        createCsvReaderFormat(Feedback.class),
                        Path.fromLocalFile(new File("/home/dominik-bilik/projects/flink-log-join/src/main/resources/source/feedbacks.csv"))
                ).build();

        FileSource<Impression> imps =
                FileSource.forRecordStreamFormat(
                        createCsvReaderFormat(Impression.class),
                        Path.fromLocalFile(new File("/home/dominik-bilik/projects/flink-log-join/src/main/resources/source/imps.csv"))
                ).build();

        DataStream<Click> clicksStream = env.fromSource(
                clicks,
                getWatermarkStrategy((element, recordTimestamp) -> element.getTimestamp()),
                "clicks-file-source"
        );
        DataStream<Feedback> feedbacksStream = env.fromSource(
                feedbacks,
                getWatermarkStrategy((element, recordTimestamp) -> element.getTimestamp()),
                "feedbacks-file-source"
        );
        DataStream<Impression> impsStream = env.fromSource(
                imps,
                getWatermarkStrategy((element, recordTimestamp) -> element.getTimestamp()),
                "imps-file-source"
        );

        DataStream<Result> joined = impsStream
                        .join(feedbacksStream)
                                .where(imp -> imp.getRandomId())
                                .equalTo(feedback -> feedback.getRandomId())
                                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                                .apply(new ImpFeedbackFlatJoinFunction());


        DataStream<Result> clickedResults = clicksStream
                        .join(joined)
                                .where(click -> click.getRandomId())
                                .equalTo(result -> result.getRandomId())
                                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                                .apply(new ClickFlatJoinFunction());

        DataStream<Result> nonClickedResults = joined.connect(clickedResults)
                        .map(new IdentityCoMap())
                        .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
                        .process(new DeduplicationProcess());

        clickedResults.sinkTo(createFilesink("/home/dominik-bilik/projects/flink-log-join/src/main/resources/result/", "Clicked"))
                .name("Clicked file sink")
                .setParallelism(1); // so it won't create multiple parts

        nonClickedResults.sinkTo(createFilesink("/home/dominik-bilik/projects/flink-log-join/src/main/resources/result/", "Non-Clicked"))
                .name("Non-Clicked file sink")
                .setParallelism(1); // so it won't create multiple parts

        env.execute();
    }

    private static <T> WatermarkStrategy<T> getWatermarkStrategy(SerializableTimestampAssigner<T> assigner) {
        return WatermarkStrategy.<T>forMonotonousTimestamps().withTimestampAssigner(assigner);
    }

    private static CsvReaderFormat createCsvReaderFormat(Class<?> clasz) {
        return CsvReaderFormat.forSchema(
                CsvMapper::new,
                mapper -> mapper
                            .schemaFor(clasz)
                            .withColumnSeparator(',')
                            .withHeader(),
                TypeInformation.of(clasz));
    }

    public static class ImpFeedbackFlatJoinFunction implements FlatJoinFunction<Impression, Feedback, Result> {

        @Override
        public void join(Impression impression, Feedback feedback, Collector<Result> out) throws Exception {
            if (impression.getTimestamp() == impression.getTimestamp()) {
                out.collect(new Result(feedback, impression));
            }
        }

    }

    public static class ClickFlatJoinFunction implements FlatJoinFunction<Click, Result, Result> {

        @Override
        public void join(Click click, Result result, Collector<Result> out) throws Exception {
            if (result.getRandomId() == click.getRandomId()) {
                result.setClicked(true);
                out.collect(result);
            }
        }
    }

    public static class IdentityCoMap implements CoMapFunction<Result, Result, Result> {

        @Override
        public Result map1(Result value) throws Exception {
            return value;
        }

        @Override
        public Result map2(Result value) throws Exception {
            return value;
        }
    }

    public static class DeduplicationProcess extends ProcessAllWindowFunction<Result, Result, TimeWindow> {

        private HashMap<Tuple2<Long, Long>, Result> resultsMap = new HashMap<>();

        @Override
        public void process(ProcessAllWindowFunction<Result, Result, TimeWindow>.Context context, Iterable<Result> elements, Collector<Result> out) throws Exception {
            // get rid of duplicated
            for (Iterator<Result> iterator = elements.iterator(); iterator.hasNext(); ) {
                Result current = iterator.next();
                Tuple2<Long, Long> key = new Tuple2<>(current.getTimestamp(), current.getRandomId());
                resultsMap.merge(
                        key,
                        current,
                        (oldV, newV) -> oldV.isClicked() ? oldV : newV);
            }

            // emit just those, who were not clicked
            for (Result result : resultsMap.values()) {
                if (!result.isClicked()) {
                    out.collect(result);
                }
            }
            resultsMap.clear();
        }
    }

    private static FileSink<Result> createFilesink(String path, String prefix) {
        return FileSink.forRowFormat(
                        new Path(new File(path).toURI()),
                        new SimpleStringEncoder<Result>())
                .withBucketAssigner(new BasePathBucketAssigner<>())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix(prefix)
                        .build())
                .build();
    }

}