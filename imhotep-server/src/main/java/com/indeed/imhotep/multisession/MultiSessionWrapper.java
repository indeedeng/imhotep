package com.indeed.imhotep.multisession;

import com.indeed.imhotep.api.FTGAIterator;
import com.indeed.imhotep.metrics.aggregate.AggregateStat;
import com.indeed.imhotep.metrics.aggregate.MultiFTGSIterator;

import java.util.List;

/**
 * @author jwolfe
 */
public class MultiSessionWrapper implements FTGAIterator {
    private final MultiFTGSIterator iterator;
    private final List<AggregateStat> filters;
    private final List<AggregateStat> selects;

    private boolean atGroup = false;

    public MultiSessionWrapper(MultiFTGSIterator iterator, List<AggregateStat> filters, List<AggregateStat> selects) {
        this.iterator = iterator;
        this.filters = filters;
        this.selects = selects;
    }

    @Override
    public int getNumGroups() {
        return iterator.getNumGroups();
    }

    @Override
    public boolean nextField() {
        return iterator.nextField();
    }

    @Override
    public String fieldName() {
        return iterator.fieldName();
    }

    @Override
    public boolean fieldIsIntType() {
        return iterator.fieldIsIntType();
    }

    @Override
    public boolean nextTerm() {
        while (iterator.nextTerm()) {
            if (findGroup()) {
                atGroup = true;
                return true;
            }
        }
        atGroup = false;
        return false;
    }

    private boolean findGroup() {
        while (iterator.nextGroup()) {
            if (allFiltersPass()) {
                return true;
            }
        }
        return false;
    }

    private boolean allFiltersPass() {
        for (final AggregateStat filter : filters) {
            if (!AggregateStat.truthy(filter.apply(iterator))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public long termDocFreq() {
        return iterator.termDocFreq();
    }

    @Override
    public long termIntVal() {
        return iterator.termIntVal();
    }

    @Override
    public String termStringVal() {
        return iterator.termStringVal();
    }

    @Override
    public byte[] termStringBytes() {
        return iterator.termStringBytes();
    }

    @Override
    public int termStringLength() {
        return iterator.termStringLength();
    }

    @Override
    public boolean nextGroup() {
        if (atGroup) {
            atGroup = false;
            return true;
        }

        return findGroup();
    }

    @Override
    public int group() {
        return iterator.group();
    }

    @Override
    public int getNumStats() {
        return selects.size();
    }

    @Override
    public void groupStats(double[] stats) {
        for (int i = 0; i < selects.size(); i++) {
            stats[i] = selects.get(i).apply(iterator);
        }
    }

    @Override
    public void close() {
        iterator.close();
    }
}
