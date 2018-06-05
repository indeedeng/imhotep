package com.indeed.imhotep.service;

import com.indeed.util.core.Pair;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * interface to allow metrics reporting
 */
public interface MetricStatsEmitter {
    void gauge(@Nonnull String type, long value);
    void gauge(@Nonnull String type, long value, @Nonnull List<Pair<String, String>> tags);
    void gauge(@Nonnull String type, double value);
    void gauge(@Nonnull String type, double value, @Nonnull List<Pair<String, String>> tags);

    void count(@Nonnull String type, long value);
    void count(@Nonnull String type, long value, @Nonnull List<Pair<String, String>> tags);

    void histogram(@Nonnull String type, long value);
    void histogram(@Nonnull String type, long value, @Nonnull List<Pair<String, String>> tags);
    void histogram(@Nonnull String type, double value);
    void histogram(@Nonnull String type, double value, @Nonnull List<Pair<String, String>> tags);

    MetricStatsEmitter NULL_EMITTER = new MetricStatsEmitter() {
        @Override
        public void gauge(@Nonnull final String type, final long value) {}

        @Override
        public void gauge(@Nonnull final String type, final long value, @Nonnull final List<Pair<String, String>> tags) {}

        @Override
        public void gauge(@Nonnull final String type, final double value) {}

        @Override
        public void gauge(@Nonnull final String type, final double value, @Nonnull final List<Pair<String, String>> tags) {}

        @Override
        public void count(@Nonnull final String type, final long value) {}

        @Override
        public void count(@Nonnull final String type, final long value, @Nonnull final List<Pair<String, String>> tags) {}

        @Override
        public void histogram(@Nonnull final String type, final long value) {}

        @Override
        public void histogram(@Nonnull final String type, final long value, @Nonnull final List<Pair<String, String>> tags) {}

        @Override
        public void histogram(@Nonnull final String type, final double value) {}

        @Override
        public void histogram(@Nonnull final String type, final double value, @Nonnull final List<Pair<String, String>> tags) {}
    };
}
