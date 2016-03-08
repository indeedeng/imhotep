package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGSIterator;

import java.util.Iterator;

/**
 * @author kenh
 */

public class TopTermsFTGSIterator implements FTGSIterator {
    final Iterator<FTGSIteratorUtil.TopTermsStatsByField.FieldAndTermStats> currentFieldIt;

    private FTGSIteratorUtil.TopTermsStatsByField.FieldAndTermStats currentField;
    private int currentTGSIdx;
    private FTGSIteratorUtil.TermStat currentTerm;
    private FTGSIteratorUtil.TermStat currentGroup;

    public TopTermsFTGSIterator(final FTGSIteratorUtil.TopTermsStatsByField topTermFTGS) {
        currentFieldIt = topTermFTGS.getEntries().iterator();
    }

    @Override
    public boolean nextField() {
        if (currentFieldIt.hasNext()) {
            currentField = currentFieldIt.next();
            currentTGSIdx = 0;
            currentTerm = null;
            currentGroup = null;

            return true;
        }
        return false;
    }

    @Override
    public String fieldName() {
        if (currentField == null) {
            throw new IllegalStateException("Invoked while not positioned in field");
        }
        return currentField.field;
    }

    @Override
    public boolean fieldIsIntType() {
        if (currentField == null) {
            throw new IllegalStateException("Invoked while not positioned in field");
        }
        return currentField.isIntType;
    }

    @Override
    public boolean nextTerm() {
        currentGroup = null;
        if (currentField == null) {
            return false;
        }

        for (; currentTGSIdx < currentField.termStats.length; ++currentTGSIdx) {
            final FTGSIteratorUtil.TermStat nextTerm = currentField.termStats[currentTGSIdx];
            if ((currentTerm == null) || !currentTerm.haveSameTerm(nextTerm)) {
                currentTerm = nextTerm;
                break;
            }
        }

        return currentTGSIdx < currentField.termStats.length;
    }

    @Override
    public long termDocFreq() {
        if (currentTerm == null) {
            throw new IllegalStateException("Invoked while not positioned in term");
        }
        return currentTerm.termDocFreq;
    }

    @Override
    public long termIntVal() {
        if (currentTerm == null) {
            throw new IllegalStateException("Invoked while not positioned in term");
        }
        return currentTerm.intTerm;
    }

    @Override
    public String termStringVal() {
        if (currentTerm == null) {
            throw new IllegalStateException("Invoked while not positioned in term");
        }
        return currentTerm.strTerm;
    }

    @Override
    public boolean nextGroup() {
        if ((currentField == null) || (currentTerm == null)) {
            return false;
        }

        for (; currentTGSIdx < currentField.termStats.length; ++currentTGSIdx) {
            final FTGSIteratorUtil.TermStat nextGroup = currentField.termStats[currentTGSIdx];

            if (currentGroup == null) {
                currentGroup = nextGroup;
                break;
            } else if (!currentGroup.haveSameTerm(nextGroup)) {
                return false;
            } else if (currentGroup.group != nextGroup.group){
                currentGroup = nextGroup;
                break;
            }
        }

        return currentTGSIdx < currentField.termStats.length;
    }

    @Override
    public int group() {
        if (currentGroup == null) {
            throw new IllegalStateException("Invoked while not positioned in group");
        }
        return currentGroup.group;
    }

    @Override
    public void groupStats(final long[] stats) {
        if (currentGroup == null) {
            throw new IllegalStateException("Invoked while not positioned in group");
        }
        System.arraycopy(currentGroup.groupStats, 0, stats, 0, currentGroup.groupStats.length);
    }

    @Override
    public void close() {
    }
}
