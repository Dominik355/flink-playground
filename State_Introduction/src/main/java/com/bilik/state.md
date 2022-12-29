State is stored per key, so for ever ykey Flink automatically determines it's own state.
Basic types:
- ValueState - 1 value
- ListState - list of values
- ReducingState - keeps single value that represents aggregation of al lvalues added to state, IN and OUT values are SAME
- AggregatingState - similar to reduce, but IN and OUT values might differ
- MapState 

It is important to keep in mind that these state objects are only used for interfacing with state. \
The state is not necessarily stored inside but might reside on disk or somewhere else

Each state has it's descriptor "StateDescriptor". This holds the name of the state , \
the type of the values that the state holds, and possibly a user-specified function, such as a ReduceFunction.\
StateDescriptor can be used for example in RichFunction, which uses state and it wants to first get value of already created state,\
for example, ich function is being restored from checkpoint, so there was a particular state already before. \
Might be used in it's open method like this:

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), // type information
                        Tuple2.of(0L, 0L)); // default value of the state, if nothing was set
        sum = getRuntimeContext().getState(descriptor);
    }
---
**TTL**\
State can have defined **Time To Live**. So when TTL expires, state is cleared. To configure this, StateTtlConfig is used.\
That might look like this:

    StateTtlConfig ttlConfig = StateTtlConfig
        .newBuilder(Time.seconds(1))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .build();

    ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("text state", String.class);
    stateDescriptor.enableTimeToLive(ttlConfig);

There are 3 update types (see enum StateTtlConfig.UpdateType) and that is:
- **Disabled**
- **OnCreateAndWrite**
- **OnReadAndWrite**

Then there is option to define, whether expired user value can be returned or not (StateTtlConfig.StateVisibility):
- **NeverReturnExpired** - expired value is never returned
- **ReturnExpiredIfNotCleanedUp** - returned if still available

**Only TTLs in reference to processing time are currently supported**

---
**Cleanup of Expired State **\
By default, expired values are explicitly removed on read, such as ValueState#value, and periodically garbage collected in the background\
if supported by the configured state backend. Background cleanup can be disabled in the StateTtlConfig:

    StateTtlConfig ttlConfig = StateTtlConfig
        .newBuilder(Time.seconds(1))
        .disableCleanupInBackground()
        .build();

For more fine-grained control over some special cleanup in background, you can configure it separately as described below. Currently, heap\
state backend relies on incremental cleanup and RocksDB backend uses compaction filter for background cleanup.\
Additionally, you can activate the cleanup at the moment of taking the full state snapshot which will reduce its size. 

    StateTtlConfig ttlConfig = StateTtlConfig
        .newBuilder(Time.seconds(1))
        .cleanupFullSnapshot()
        .build();

Another option is to trigger cleanup of some state entries incrementally. The trigger can be a callback from each state access or/and each record processing.

        .cleanupIncrementally(10, true)

This strategy has two parameters. The first one is number of checked state entries per each cleanup triggering. It is always triggered per each state access.\
The second parameter defines whether to trigger cleanup additionally per each record processing.\ The default background cleanup for heap backend checks \
5 entries without cleanup per record processing. Time spent for the incremental cleanup increases record processing latency.\
Currently works on for Heap, not RocksDB.

---
**Broadcast State**\
Broadcast State is a special type of Operator State. It was introduced to support use cases where records of one stream need to be \
broadcasted to all downstream tasks, where they are used to maintain the same state among all subtasks. This state can then be \
accessed while processing records of a second stream. Broadcast state differs from the rest of operator states in that:
- it has a map format,
- it is only available to specific operators that have as inputs a broadcasted stream and a non-broadcasted one, and
- such an operator can have multiple broadcast states with different names.
---
**Using Operator State**\
To use operator state, a stateful function can implement the CheckpointedFunction interface.

**CheckpointedFunction** \
(look at [BufferingSink](BufferingSink.java))\
The CheckpointedFunction interface **provides access to non-keyed state** with different redistribution schemes. \
It requires the implementation of two methods: 
- **snapshotState()** - Whenever a checkpoint has to be performed, snapshotState() is called
- **initializeState()** - is called every time the user-defined function is initialized, be that when the function is first initialized or be that \
when the function is actually recovering from an earlier checkpoint. Given this, initializeState() is not only the place where different types of \
state are initialized, but also where state recovery logic is included.

The state is expected to be a List of serializable objects, independent from each other, thus eligible for redistribution upon rescaling.\
Depending on the state accessing method, the following redistribution schemes are defined:
- **Even-split redistribution** - Each operator returns a List of state elements. The whole state is logically a concatenation of all lists.\
  On restore/redistribution, the list is evenly divided into as many sublists as there are parallel operators.\
  Each operator gets a sublist, which can be empty, or contain one or more elements. As an example, if with parallelism 1 the checkpointed \
state of an operator contains elements element1 and element2, when increasing the parallelism to 2, element1 may end up in operator instance 0, \
while element2 will go to operator instance 1.
- **Union redistribution** - Each operator returns a List of state elements. The whole state is logically a concatenation of all lists.\
  In restore/redistribution, each operator gets the complete list of state elements
---
**Stateful Source Functions**\
Stateful sources require a bit more care as opposed to other operators. In order to make the updates to the state and output collection atomic \
(required for exactly-once semantics on failure/recovery), the user is required to get a lock from the source’s context.\
(look at [CounterSource](CounterSource.java))












