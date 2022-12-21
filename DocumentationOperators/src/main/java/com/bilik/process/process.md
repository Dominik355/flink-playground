**The Process Function**\
The ProcessFunction is a low-level stream processing operation, giving access to the basic building blocks of all (acyclic) streaming applications:
- events (stream elements)
- state (fault-tolerant, consistent, only on keyed stream)
- timers (event time and processing time, only on keyed stream)

The ProcessFunction can be thought of as a FlatMapFunction with access to keyed state and timers. It handles events by being invoked for each event \
received in the input stream(s).