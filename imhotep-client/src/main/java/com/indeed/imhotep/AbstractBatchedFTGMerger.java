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
import com.indeed.imhotep.api.FTGIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.util.Arrays;
import java.util.Collection;

/**
 * Expects reading of groups/stats to be batched, and thus exposes
 * abstract methods for implementing said behavior.
 *
 * @author jsgroth
 */
public abstract class AbstractBatchedFTGMerger implements FTGIterator {
    private static final Logger log = Logger.getLogger(AbstractBatchedFTGMerger.class);

    private final CharsetDecoder decoder = Charsets.UTF_8.newDecoder();

    protected final FTGSIterator[] iterators;
    private final boolean ignoreFieldNames;
    private final int numGroups;
    private final int numIterators;
    protected final int[] fieldIterators;
    protected int numFieldIterators;
    protected final int[] termIterators;
    protected final int[] termIteratorIndexes;
    protected int numTermIterators;
    protected int termIteratorsRemaining;

    private String fieldName;
    protected boolean fieldIsIntType;
    protected long termIntVal;

    private byte[] currentTermBytes;
    private ByteBuffer byteBuffer;
    private int currentTermLength;
    private String termStringVal;

    private boolean done;

    private final Closeable doneCallback;

    protected AbstractBatchedFTGMerger(
            final Collection<? extends FTGSIterator> iterators,
            @Nullable final Closeable doneCallback,
            final boolean ignoreFieldNames) {
        this.doneCallback = doneCallback;
        numIterators = iterators.size();
        this.iterators = iterators.toArray(new FTGSIterator[numIterators]);
        this.ignoreFieldNames = ignoreFieldNames;
        numGroups = FTGSIteratorUtil.getNumGroups(this.iterators);
        fieldIterators = new int[numIterators];
        numFieldIterators = 0;
        termIterators = new int[numIterators];
        termIteratorIndexes = new int[numIterators];
        numTermIterators = 0;
        done = false;

        currentTermBytes = new byte[100];
        byteBuffer = ByteBuffer.wrap(currentTermBytes);
        currentTermLength = 0;
        termStringVal = null;
    }

    protected static void swap(final int[] a, final int b, final int e) {
        final int t = a[b];
        a[b] = a[e];
        a[e] = t;
    }

    @Override
    public int getNumGroups() {
        return numGroups;
    }

    @Override
    public final boolean nextField() {
        if (done) {
            return false;
        }

        numFieldIterators = 0;

        final FTGSIterator first = iterators[0];
        final boolean firstHasNextField = first.nextField();
        for (int i = 1; i < numIterators; ++i) {
            if (iterators[i].nextField() != firstHasNextField) {
                throw new IllegalArgumentException("sub iterator fields do not match");
            }
        }
        if (!firstHasNextField) {
            close();
            return false;
        }
        fieldName = first.fieldName();
        fieldIsIntType = first.fieldIsIntType();
        numFieldIterators = 0;
        if (first.nextTerm()) {
            fieldIterators[numFieldIterators] = 0;
            numFieldIterators++;
        }

        for (int i = 1; i < numIterators; ++i) {
            final FTGSIterator itr = iterators[i];
            if ((!ignoreFieldNames && !itr.fieldName().equals(fieldName)) || itr.fieldIsIntType() != fieldIsIntType) {
                throw new IllegalArgumentException("sub iterator fields do not match");
            }
            if (itr.nextTerm()) {
                fieldIterators[numFieldIterators] = i;
                numFieldIterators++;
            }
        }
        numTermIterators = 0;
        return true;
    }

    @Override
    public final String fieldName() {
        return fieldName;
    }

    @Override
    public final boolean fieldIsIntType() {
        return fieldIsIntType;
    }

    @Override
    public String termStringVal() {
        if (termStringVal == null) {
            try {
                termStringVal = decoder.decode((ByteBuffer)byteBuffer.position(0).limit(currentTermLength)).toString();
            } catch (final CharacterCodingException e) {
                throw new RuntimeException(e);
            }
        }
        return termStringVal;
    }

    @Override
    public byte[] termStringBytes() {
        return currentTermBytes;
    }

    @Override
    public int termStringLength() {
        return currentTermLength;
    }

    @Override
    public boolean nextTerm() {
        for (int i = 0; i < numTermIterators; ++i) {
            final FTGSIterator itr = iterators[termIterators[i]];
            if (!itr.nextTerm()) {
                final int fi = termIteratorIndexes[i];
                numFieldIterators--;
                swap(fieldIterators, fi, numFieldIterators);
                for (int j = 0; j < numTermIterators; ++j) {
                    if (termIteratorIndexes[j] == numFieldIterators) {
                        termIteratorIndexes[j] = fi;
                    }
                }
            }
        }

        numTermIterators = 0;
        if (numFieldIterators == 0) {
            return false;
        }

        int newNumTermIterators = 0;
        if (fieldIsIntType) {
            long min = Long.MAX_VALUE;
            for (int i = 0; i < numFieldIterators; ++i) {
                final FTGSIterator itr = iterators[fieldIterators[i]];
                final long term = itr.termIntVal();
                if (term < min) {
                    newNumTermIterators = 1;
                    termIteratorIndexes[0] = i;
                    min = term;
                } else if (term == min) {
                    termIteratorIndexes[newNumTermIterators] = i;
                    newNumTermIterators++;
                }
            }
            termIntVal = min;
        } else {
            byte[] minTermBytes = null;
            int minTermLength = -1;
            for (int i = 0; i < numFieldIterators; ++i) {
                final FTGSIterator itr = iterators[fieldIterators[i]];
                final byte[] termBytes = itr.termStringBytes();
                final int termLength = itr.termStringLength();
                final int c;
                if (minTermBytes == null || (c = compareBytes(termBytes, termLength, minTermBytes, minTermLength)) < 0) {
                    newNumTermIterators = 1;
                    termIteratorIndexes[0] = i;
                    minTermBytes = termBytes;
                    minTermLength = termLength;
                } else if (c == 0) {
                    termIteratorIndexes[newNumTermIterators] = i;
                    newNumTermIterators++;
                }
            }
            if (currentTermBytes.length < minTermLength) {
                currentTermBytes = Arrays.copyOf(minTermBytes, Math.max(minTermLength, 2 * currentTermBytes.length));
                byteBuffer = ByteBuffer.wrap(currentTermBytes);
            } else {
                System.arraycopy(minTermBytes, 0, currentTermBytes, 0, minTermLength);
            }
            currentTermLength = minTermLength;
            termStringVal = null;
        }

        for (int i = 0; i < newNumTermIterators; ++i) {
            final int fi = termIteratorIndexes[i];
            final int index = fieldIterators[fi];
            termIterators[numTermIterators] = index;
            termIteratorIndexes[numTermIterators] = fi;
            numTermIterators++;
        }
        termIteratorsRemaining = numTermIterators;
        for (int i = 0; i < termIteratorsRemaining; ++i) {
            final FTGSIterator itr = iterators[termIterators[i]];
            if (!itr.nextGroup()) {
                termIteratorsRemaining--;
                swap(termIterators, i, termIteratorsRemaining);
                swap(termIteratorIndexes, i, termIteratorsRemaining);
                --i;
            }
        }
        resetBatchedGroups();
        return true;
    }

    // this is a comparison of UTF-8 bytes that is wrong in the same way String.compareTo(String) is wrong
    public static int compareBytes(final byte[] b1, final int l1, final byte[] b2, final int l2) {
        for (int i = 0, j = 0; i < l1 && j < l2; i++, j++) {
            final int v1 = b1[i] & 0xFF;
            final int v2 = b2[j] & 0xFF;
            if (v1 != v2) {
                if (((v1 & 0xF0) == 0xF0 || (v2 & 0xF0) == 0xF0) && ((v1 & 0xF0) != (v2 & 0xF0))) {
                    if ((v1 & 0xF0) == 0xF0) {
                        return UTF8ToCodePoint(v2, b2, j + 1, l2) > 0xDFFF ? -1 : 1;
                    }
                    return UTF8ToCodePoint(v1, b1, i + 1, l1) > 0xDFFF ? 1 : -1;
                }
                return v1 - v2;
            }
        }
        return l1 - l2;
    }

    static int UTF8ToCodePoint(final int firstByte, final byte[] b, int off, final int len) {
        if (firstByte < 128) {
            return firstByte;
        }

        int cp;
        final int cpl;
        if ((firstByte & 0x20) == 0) {
            cpl = 1;
            cp = (firstByte & 0x1F) << 6;
        } else if ((firstByte & 0x10) == 0) {
            cpl = 2;
            cp = (firstByte & 0x0F) << 12;
        } else if ((firstByte & 0x08) == 0) {
            cpl = 3;
            cp = (firstByte & 0x07) << 18;
        } else if ((firstByte & 0x04) == 0) {
            cpl = 4;
            cp = (firstByte & 0x03) << 24;
        } else if ((firstByte & 0x02) == 0) {
            cpl = 5;
            cp = (firstByte & 0x01) << 30;
        } else {
            throw new RuntimeException("invalid UTF-8");
        }

        if (off + cpl > len) {
            throw new RuntimeException("invalid UTF-8");
        }

        for (int k = cpl - 1; k >= 0; --k) {
            cp |= ((b[off++] & 0x3F) << (6*k));
        }

        return cp;
    }


    @Override
    public final long termDocFreq() {
        long ret = 0L;
        for (int i = 0; i < termIteratorsRemaining; ++i) {
            ret += iterators[termIterators[i]].termDocFreq();
        }
        return ret;
    }

    @Override
    public final long termIntVal() {
        return termIntVal;
    }

    @Override
    public final boolean nextGroup() {
        while (true) {
            if (nextBatchedGroup()) {
                return true;
            }
            if (termIteratorsRemaining == 0) {
                return false;
            }

            calculateNextGroupBatch();
        }
    }

    /**
     * Reads the next batch of groups into internal state.
     *
     * Is expected to mutate this abstract class's termIteratorsRemaining,
     * termIterators, and termIteratorIndexes values appropriately if we
     * reach the end of the groups for a particular input iterator
     */
    abstract void calculateNextGroupBatch();

    /**
     * Advances to the next already-batched group and returns true
     * iff such a group was found.
     * Prepares subclass internal state such that anything that reads groups
     * or data related to a group will return the data corresponding to the group
     * that was advanced to.
     * @return true iff such a group was found
     */
    abstract boolean nextBatchedGroup();

    /**
     * Reset subclass internal state such that no groups are queued,
     * regardless of whether the groups have been consumed.
     */
    abstract void resetBatchedGroups();

    @Override
    public synchronized void close() {
        if (!done) {
            done = true;
            Closeables2.closeAll(log, Closeables2.forArray(log, iterators), doneCallback);
        }
    }
}
