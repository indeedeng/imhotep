/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.imhotep;

import com.google.common.base.Charsets;
import com.indeed.imhotep.api.FTGSIterator;

import java.util.Iterator;

/**
 * @author kenh
 */

public class TopTermsFTGSIterator implements FTGSIterator {
    private final Iterator<FTGSIteratorUtil.TopTermsStatsByField.FieldAndTermStats> currentFieldIt;

    private FTGSIteratorUtil.TopTermsStatsByField.FieldAndTermStats currentField;
    private int currentTGSIdx;
    private FTGSIteratorUtil.TermStat currentTerm;
    private byte[] currentTermBytes;
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
            currentTermBytes = null;
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
                currentTermBytes = null;
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
    public byte[] termStringBytes() {
        if (currentTermBytes == null) {
            currentTermBytes = termStringVal().getBytes(Charsets.UTF_8);
        }
        return currentTermBytes;
    }

    @Override
    public int termStringLength() {
        return termStringBytes().length;
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
