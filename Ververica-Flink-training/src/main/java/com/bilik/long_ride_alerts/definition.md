The goal of the "Long Ride Alerts" exercise is to provide a warning whenever a taxi ride
lasts for more than two hours.

This should be done using the event time timestamps and watermarks that are provided in the data stream.

The stream is out-of-order, and it is possible that the END event for a ride will be processed before
its START event.

An END event may be missing, but you may assume there are no duplicated events, and no missing START events.

It is not enough to simply wait for the END event and calculate the duration, as we want to be alerted
about the long ride as soon as possible.

You should eventually clear any state you create.

### Input Data

The input data of this exercise is a `DataStream` of taxi ride events.

### Expected Output

The result of the exercise should be a `DataStream<LONG>` that contains the `rideId` for rides
with a duration that exceeds two hours.

The resulting stream should be printed to standard out.
