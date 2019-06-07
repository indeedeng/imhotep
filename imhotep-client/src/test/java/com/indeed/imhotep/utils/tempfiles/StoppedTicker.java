package com.indeed.imhotep.utils.tempfiles;

import com.google.common.base.Ticker;

import java.util.concurrent.TimeUnit;

public class StoppedTicker extends Ticker {
    private long value = 0;

    public void write(final long value) {
        this.value = value;
    }

    public void add(final long duration, final TimeUnit unit) {
        value += unit.toNanos(duration);
    }

    @Override
    public long read() {
        return value;
    }
}
