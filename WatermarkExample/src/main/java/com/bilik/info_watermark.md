Attention: Both timestamps and watermarks are specified as milliseconds since the Java epoch of 1970-01-01T00:00:00Z.

There are two places in Flink applications where a WatermarkStrategy can be used: 
1) directly on sources
2) after non-source operation

**WatermarkGenerator**\
There are two different styles of watermark generation: periodic and punctuated.\
A periodic generator usually observes the incoming events via onEvent() and then emits a watermark \
when the framework calls onPeriodicEmit(). A puncutated generator will look at events in onEvent() \
and wait for special marker events or punctuations that carry watermark information in the stream.\
When it sees one of these events it emits a watermark immediately. Usually, punctuated generators don’t \
emit a watermark from onPeriodicEmit().