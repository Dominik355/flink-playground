package com.bilik.common.sources;

import com.bilik.common.datatypes.TaxiRide;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Collection;

public class LongRidesGenerator implements SourceFunction<TaxiRide> {

    private final Collection<TaxiRide> rides;

    public LongRidesGenerator(Collection<TaxiRide> rides) {
        this.rides = rides;
    }

    // watermark has 200ms emit invertvals, so we let watermark to be generated after every event
    @Override
    public void run(SourceContext<TaxiRide> ctx) throws Exception {
        rides.stream()
                .forEachOrdered(ride -> {
                    ctx.collect(ride);
                    try {
                        Thread.sleep(250);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public void cancel() {
        // no need for infinite source
    }

}
