package com.bilik;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.List;
import java.util.ArrayList;

/**
 * Below is an example of a stateful SinkFunction that uses CheckpointedFunction to buffer elements before
 * sending them to the outside world. It demonstrates the basic even-split redistribution list state.
 *
 * The initializeState method takes as argument a FunctionInitializationContext. This is used to initialize the non-keyed state “containers”.
 * These are a container of type ListState where the non-keyed state objects are going to be stored upon checkpointing.
 *
 * Note how the state is initialized, similar to keyed state, with a StateDescriptor that contains the state name and
 * information about the type of the value that the state holds.
 */
public class BufferingSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {

    private final int threshold;

    private transient ListState<Tuple2<String, Integer>> checkpointedState;

    private List<Tuple2<String, Integer>> bufferedElements;

    public BufferingSink(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        bufferedElements.add(value);
        if (bufferedElements.size() >= threshold) {
            for (Tuple2<String, Integer> element: bufferedElements) {
                // send it to the sink
            }
            bufferedElements.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        for (Tuple2<String, Integer> element : bufferedElements) {
            checkpointedState.add(element);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<String, Integer>> descriptor =
                new ListStateDescriptor<>(
                        "buffered-elements",
                        TypeInformation.of(new TypeHint<>() {}));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        // check if we are recovering after a failure. If this is true, i.e. we are recovering, the restore logic is applied.
        if (context.isRestored()) {
            for (Tuple2<String, Integer> element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
    }
}

