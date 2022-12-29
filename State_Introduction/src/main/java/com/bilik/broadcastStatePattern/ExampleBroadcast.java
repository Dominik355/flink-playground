package com.bilik.broadcastStatePattern;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * As our running example, we will use the case where we have a stream of objects of different colors and shapes and we want to find pairs of objects of
 * the same color that follow a certain pattern, e.g. a rectangle followed by a triangle. We assume that the set of interesting patterns evolves over time.
 *
 * In this example, the first stream will contain elements of type Item with a Color and a Shape property. The other stream will contain the Rules.
 *
 * Starting from the stream of Items, we just need to key it by Color, as we want pairs of the same color.
 * This will make sure that elements of the same color end up on the same physical machine.
 */
public class ExampleBroadcast {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Item> itemStream = env.fromCollection(Item.generateItems(50));

        KeyedStream<Item, String> colorPartitionedStream = itemStream.keyBy(Item::getColor);


        // a map descriptor to store the name of the rule (string) and the rule itself.
        MapStateDescriptor<String, ShapeRule> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<>() {}));

        // broadcast the rules and create the broadcast state
        BroadcastStream<ShapeRule> ruleBroadcastStream = env.fromElements(new ShapeRule())
                .broadcast(ruleStateDescriptor);

        colorPartitionedStream
                .connect(ruleBroadcastStream)
                .process(new ExampleKeyedBroadcastProcessFunction())
                .print();

        MapStateDescriptor<String, List<Item>> mapStateDesc =
                new MapStateDescriptor<>(
                        "items",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        new ListTypeInfo<>(Item.class));


        env.execute();
    }

}
