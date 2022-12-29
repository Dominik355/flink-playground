package com.bilik.broadcastStatePattern;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Does not really matter what it does, it's just an example of broadcasting rule into all the tasks
 */
public class ExampleKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction<String, Item, ShapeRule, String> {

    // store partial matches, i.e. first elements of the pair waiting for their second element
    // we keep a list as we may have many first elements waiting
    private final MapStateDescriptor<String, List<Item>> mapStateDesc =
            new MapStateDescriptor<>(
                    "items",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    new ListTypeInfo<>(Item.class));

    // identical to our ruleStateDescriptor above
    private final MapStateDescriptor<String, ShapeRule> ruleStateDescriptor =
            new MapStateDescriptor<>(
                    "RulesBroadcastState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(new TypeHint<>() {}));

    // adds broadcasted rule to our keyed state
    @Override
    public void processBroadcastElement(ShapeRule value,
                                        Context ctx,
                                        Collector<String> out) throws Exception {
        ctx.getBroadcastState(ruleStateDescriptor).put(value.name, value);
    }

    @Override
    public void processElement(Item value,
                               ReadOnlyContext ctx,
                               Collector<String> out) throws Exception {

        final MapState<String, List<Item>> state = getRuntimeContext().getMapState(mapStateDesc); // get stored items
        final Item.Shape shape = value.getShape(); // obtain shape of current processed item

        // iterate over broadcasted rules
        for (Map.Entry<String, ShapeRule> entry : ctx.getBroadcastState(ruleStateDescriptor).immutableEntries()) {
            final String ruleName = entry.getKey();
            final ShapeRule shapeRule = entry.getValue();

            List<Item> stored = state.get(ruleName); // get stored items for that rule (there is only 1 now)
            if (stored == null) {
                stored = new ArrayList<>();
            }

            if (shape == shapeRule.second && !stored.isEmpty()) {
                for (Item i : stored) {
                    out.collect("MATCH: " + i + " - " + value);
                }
                stored.clear();
            }

            // there is no else{} to cover if rule.first == rule.second
            if (shape.equals(shapeRule.first)) {
                stored.add(value);
            }

            if (stored.isEmpty()) {
                state.remove(ruleName);
            } else {
                state.put(ruleName, stored);
            }
        }
    }


}
