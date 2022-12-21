package com.bilik.punctuated;

import com.bilik.Example1Main;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * A punctuated watermark generator will observe the stream of events and emit a watermark whenever it sees a special element that carries watermark information.
 */
public class PunctuatedAssigner implements WatermarkGenerator<PunctuatedMyEvent> {

    @Override
    public void onEvent(PunctuatedMyEvent event, long eventTimestamp, WatermarkOutput output) {
        if (event.hasWatermarkMarker()) {
            output.emitWatermark(new Watermark(event.getWatermarkTimestamp()));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // don't need to do anything because we emit in reaction to events above
    }

}

class PunctuatedMyEvent extends Example1Main.MyEvent {

    private boolean hasWatermarkMarker;
    private long watermarkTimestamp;

    public boolean hasWatermarkMarker() {
        return hasWatermarkMarker;
    }

    public void setHasWatermarkMarker(boolean hasWatermarkMarker) {
        this.hasWatermarkMarker = hasWatermarkMarker;
    }

    public long getWatermarkTimestamp() {
        return watermarkTimestamp;
    }

    public void setWatermarkTimestamp(long watermarkTimestamp) {
        this.watermarkTimestamp = watermarkTimestamp;
    }
}
