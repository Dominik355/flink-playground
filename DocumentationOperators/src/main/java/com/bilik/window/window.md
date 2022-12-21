Every window has **Trigger** and **Function**

**Trigger**\
Specifies the conditions under which the window is considered ready for the function to be applied.\
A triggering policy might be something like “when the number of elements in the window is more than 4”, \
or “when the watermark passes the end of the window”.\
A trigger can also decide to purge a window’s contents any time between its creation and removal.\
Purging in this case only refers to the elements in the window, and not the window metadata. \
This means that new data can still be added to that window.

**Function**\
Contains the computation to be applied to the contents of the window

**Evictor**\
Apart from the above, you can specify an Evictor, which will be able to remove elements from the window \
after the trigger fires and before and/or after the function is applied.

**TUMBLING WINDOW**\
Fixed time size.\
Tumbling window assigners also take an optional offset parameter that can be used to change the alignment of windows. \
For example, without offsets hourly tumbling windows are aligned with epoch, that is you will get windows \
such as 1:00:00.000 - 1:59:59.999, 2:00:00.000 - 2:59:59.999 and so on. If you want to change that you can \
give an offset. With an offset of 15 minutes you would, for example, get 1:15:00.000 - 2:14:59.999, 2:15:00.000 - 3:14:59.999 etc

**SLIDING WINDOWS**\
Fixed size, which overlaps (slides) another windows.
Windows of size 10 minutes and slide 5 minutes might look like this : 
10:00:00-10:09:59.999, 10:05-10:14:59.999

**SESSION WINDOWS**\
The session windows assigner groups elements by sessions of activity. Session windows do not overlap and do not \
have a fixed start and end time, in contrast to tumbling windows and sliding windows. \
Instead a session window closes when it does not receive elements for a certain period of time, \
i.e., when a gap of inactivity occurred. A session window assigner can be configured with either \
a static session gap or with a session gap extractor function which defines how long the period of inactivity is. \
When this period expires, the current session closes and subsequent elements are assigned to a new session window.

**GLOBAL WINDOWS**\
A global windows assigner assigns all elements with the same key to the same single global window.\
This windowing scheme is only useful if you also specify a custom trigger.
---
**FUNCTIONS**
- Reduce
- Aggregate
- Process

The first two can be executed more efficiently, because Flink can incrementally aggregate the elements for each window as they arrive.

A ProcessWindowFunction gets an Iterable for all the elements contained in a window and additional meta \
information about the window to which the elements belong.

So Process funciton has to buffer all the elements in window and other 2 can just aggregate very arriving element.

This can be mitigated by combining a ProcessWindowFunction with a ReduceFunction, or AggregateFunction to get both incremental \
aggregation of window elements and the additional window metadata that the ProcessWindowFunction receives.

**REDUCE**\
A ReduceFunction specifies how two elements from the input are combined to produce an output element of the same type. \
Flink uses a ReduceFunction to incrementally aggregate the elements of a window.

**AGGREGATE**\
An AggregateFunction is a generalized version of a ReduceFunction that has three types:
- an input type (IN) - type of elements in the input stream
- accumulator type (ACC)
- an output type (OUT)

AggregateFunction has a method for adding one input element to an accumulator. The interface also has methods for creating \
an initial accumulator, for merging two accumulators into one accumulator and for extracting an output from an accumulator.


**PROCESS**\
A ProcessWindowFunction gets an Iterable containing all the elements of the window, and a Context object with access to time \
and state information, which enables it to provide more flexibility than other window functions.


**ProcessWindowFunction with Reduce or Aggregate**\
A ProcessWindowFunction can be combined with either a ReduceFunction, or an AggregateFunction to incrementally aggregate \
elements as they arrive in the window. When the window is closed, the ProcessWindowFunction will be provided with the aggregated result. \
This allows it to incrementally compute windows while having access to the additional window meta information of the ProcessWindowFunction.
---
**TRIGGERS**\
A Trigger determines when a window (as formed by the window assigner) is ready to be processed by the window function.\
Each WindowAssigner comes with a default Trigger. If the default trigger does not fit your needs, you can specify a custom trigger using trigger(...).\
Flink comes with a few built-in triggers:
- The (already mentioned) EventTimeTrigger fires based on the progress of event-time as measured by watermarks.
- The ProcessingTimeTrigger fires based on processing time.
- The CountTrigger fires once the number of elements in a window exceeds the given limit.
- The PurgingTrigger takes as argument another trigger and transforms it into a purging one.

---
**EVICTORS**\
Flink’s windowing model allows specifying an optional Evictor in addition to the WindowAssigner and the Trigger.\
The evictor has the ability to remove elements from a window after the trigger fires and before and/or after the window function is applied.\
Elements evicted before the application of the window function will not be processed by it.\
Flink provides no guarantees about the order of the elements within a window. This implies that although an evictor may remove elements \
from the beginning of the window, these are not necessarily the ones that arrive first or last.

---
**ALLOWED LATENESS**\
When working with event-time windowing, it can happen that elements arrive late, i.e. the watermark that Flink uses to keep track of the \
progress of event-time is already past the end timestamp of a window to which an element belongs.\
By default, late elements are dropped when the watermark is past the end of the window. \
However, Flink allows to specify a maximum allowed lateness for window operators\
Elements that arrive after the watermark has passed the end of the window but before it passes the end of the window plus the \
allowed lateness, are still added to the window.\
Depending on the trigger used, a late but not dropped element may cause the window to fire again. This is the case for the EventTimeTrigger.\
In order to make this work, Flink keeps the state of windows until their allowed lateness expires.







