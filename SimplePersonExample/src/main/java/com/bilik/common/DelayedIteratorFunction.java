package com.bilik.common;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Iterator;

public class DelayedIteratorFunction<T> implements SourceFunction<T> {

    private static final long serialVersionUID = 1L;

    private final Iterator<T> iterator;
    private final int delayMilis;

    private volatile boolean isRunning = true;

    public DelayedIteratorFunction(Iterator<T> iterator, int delayMilis) {
        this.iterator = iterator;
        this.delayMilis = delayMilis;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        while (isRunning && iterator.hasNext()) {
            ctx.collect(iterator.next());
            Thread.sleep(delayMilis);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

}
