package com.amazonaws.glue.catalog.util;

import com.google.common.base.Ticker;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class FakeTicker extends Ticker {

    private final AtomicLong nanos = new AtomicLong();

    /**
     * Advances the ticker value by {@code time} in {@code timeUnit}.
     */
    public FakeTicker advance(long time, TimeUnit timeUnit) {
        nanos.addAndGet(timeUnit.toNanos(time));
        return this;
    }

    @Override
    public long read() {
        return nanos.get();
    }
}

