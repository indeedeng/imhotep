package com.indeed.imhotep.service;

import javax.annotation.Nonnull;

/**
 * interface to allow metrics reporting
 */
public interface MetricStatsEmitter {
    void gauge(@Nonnull String type, long value);
    void gauge(@Nonnull String type, double value);

    void count(@Nonnull String type, long value);

    void histogram(@Nonnull String type, long value);
    void histogram(@Nonnull String type, double value);

    MetricStatsEmitter NULL_EMITTER = new MetricStatsEmitter() {
        @Override
        public void gauge(@Nonnull final String type, final long value) {}

        @Override
        public void gauge(@Nonnull final String type, final double value) {}

        @Override
        public void count(@Nonnull final String type, final long value) {}

        @Override
        public void histogram(@Nonnull final String type, final long value) {}

        @Override
        public void histogram(@Nonnull final String type, final double value) {}
    };
}
