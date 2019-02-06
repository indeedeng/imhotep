package com.indeed.imhotep.local;

import com.indeed.imhotep.metrics.aggregate.MultiFTGSIterator;

/**
 * Exposes the (importantly, mutable and shared) stats array passed in to the constructor, exposing
 * the current group.
 *
 * All other read operations are passed on to the wrapped delegate.
 */
class FrozenGroupStatsMultiFTGSIterator implements MultiFTGSIterator {
    private int group;
    private final long[] statsView;
    private final int[] statOffsets;
    private final MultiFTGSIterator wrapped;
    private final int[] numStats;

    FrozenGroupStatsMultiFTGSIterator(final long[] statsView, final int[] statOffsets, final MultiFTGSIterator wrapped) {
        this.statsView = statsView;
        this.statOffsets = statOffsets;
        this.wrapped = wrapped;
        this.numStats = wrapped.numStats();
    }

    public void setGroup(final int newGroup) {
        this.group = newGroup;
    }

    @Override
    public long groupStat(final int sessionIndex, final int statIndex) {
        return statsView[statOffsets[sessionIndex] + statIndex];
    }

    @Override
    public void groupStats(final int sessionIndex, final long[] stats, final int offset) {
        System.arraycopy(statsView, statOffsets[sessionIndex], stats, offset, numStats[sessionIndex]);
    }

    @Override
    public int group() {
        return group;
    }

    @Override
    public int[] numStats() {
        return wrapped.numStats();
    }

    @Override
    public int getNumGroups() {
        return wrapped.getNumGroups();
    }

    @Override
    public boolean nextField() {
        throw new UnsupportedOperationException("Mutation operations are not supported on FakeMultiFTGSIterator");
    }

    @Override
    public String fieldName() {
        return wrapped.fieldName();
    }

    @Override
    public boolean fieldIsIntType() {
        return wrapped.fieldIsIntType();
    }

    @Override
    public boolean nextTerm() {
        throw new UnsupportedOperationException("Mutation operations are not supported on FakeMultiFTGSIterator");
    }

    @Override
    public long termDocFreq() {
        return wrapped.termDocFreq();
    }

    @Override
    public long termIntVal() {
        return wrapped.termIntVal();
    }

    @Override
    public String termStringVal() {
        return wrapped.termStringVal();
    }

    @Override
    public byte[] termStringBytes() {
        return wrapped.termStringBytes();
    }

    @Override
    public int termStringLength() {
        return wrapped.termStringLength();
    }

    @Override
    public boolean nextGroup() {
        throw new UnsupportedOperationException("Mutation operations are not supported on FakeMultiFTGSIterator");
    }

    @Override
    public void close() {
        wrapped.close();
    }
}
