package com.indeed.imhotep.multicache.ftgs;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.simple.MultiShardFlamdexReader;
import com.indeed.util.core.hash.MurmurHash;

public class NativeFTGSOpIterator implements Iterator<NativeFTGSOpIterator.NativeTGSinfo>, Closeable {
    private static final int STATE_INT_FIELD_START = 1;
    private static final int STATE_INT_FIELD_ITER = 2;
    private static final int STATE_INT_FIELD_END = 3;
    private static final int STATE_STRING_FIELD_START = 4;
    private static final int STATE_STRING_FIELD_ITER = 5;
    private static final int STATE_STRING_FIELD_END = 6;
    private static final int STATE_NO_MORE_FIELDS = 7;
    private static final int STATE_DONE = 8;

    private static final int LARGE_PRIME_FOR_CLUSTER_SPLIT = 969168349;

    private final String[] intFields;
    private final String[] stringFields;
    private final MultiShardFlamdexReader multiShardFlamdexReader;
    private final int numSplits;
    private final int numWorkers;
    private int state;
    private int currentSplitNum = 0;
    private int currentIteratorIdx = 0;
    private Iterator<TermDesc> currentTermIter;

    static class NativeTGSinfo {
        public static final byte TGS_OPERATION = 1;
        public static final byte FIELD_START_OPERATION = 2;
        public static final byte FIELD_END_OPERATION = 3;
        public static final byte NO_MORE_FIELDS_OPERATION = 4;

        public TermDesc termDesc;
        public int splitIndex;
        public int socketNum;
        public byte operation;
        public String fieldName;
        public boolean isIntField;
    }

    public NativeFTGSOpIterator(FlamdexReader[] flamdexReaders,
                                String[] intFields,
                                String[] stringFields,
                                int numSplits,
                                int numWorkers) throws IOException {
        this.intFields = intFields;
        this.stringFields = stringFields;
        this.multiShardFlamdexReader = new MultiShardFlamdexReader(flamdexReaders);
        this.numSplits = numSplits;
        this.numWorkers = numWorkers;

        this.currentSplitNum = 0;
        this.currentIteratorIdx = 0;
        if (intFields.length > 0) {
            final String field = intFields[0];
            currentTermIter = multiShardFlamdexReader.intTermOffsetIterator(field);
            state = STATE_INT_FIELD_START;
        } else if (stringFields.length > 0) {
            final String field = stringFields[0];
            currentTermIter = multiShardFlamdexReader.stringTermOffsetIterator(field);
            state = STATE_STRING_FIELD_START;
        } else {
            currentTermIter = null;
            state = STATE_NO_MORE_FIELDS;
        }
    }


    private static final int minHashInt(long term, int numSplits) {
        int v;
    
        v = (int) (term * LARGE_PRIME_FOR_CLUSTER_SPLIT);
        v += 12345;
        v &= Integer.MAX_VALUE;
        v = v >> 16;
        return v % numSplits;
    }

    private static final int minHashString(final byte[] termStringBytes,
                                           final int termStringLength,
                                           final int numSplits) {
        int v;

        v = MurmurHash.hash32(termStringBytes, 0, termStringLength);
        v *= LARGE_PRIME_FOR_CLUSTER_SPLIT;
        v += 12345;
        v &= 0x7FFFFFFF;
        v = v >> 16;
        return v % numSplits;
    }

    /******
        for (String field : intFields) {
            final CloseableIter iter = createIntIterator(field, numSplits);
            for (i = 0; i < numSplits; i++) {
                yield startField(field, i);  // state 1
            }
            while (iter.hasNext()) {
                yield iter.next;      // state 2
            }
            iter.close();
            for (i = 0; i < numSplits; i++) {
                yield endField(i);    // state 3
            }
        }
        for (String field : stringFields) {
            final CloseableIter iter = createStringIterator(field, numSplits);
            for (i = 0; i < numSplits; i++) {
                yield startField(field, i);  // state 4
            }
            while (iter.hasNext()) {
                yield iter.next;      // state 5
            }
            iter.close();
            for (i = 0; i < numSplits; i++) {
                yield endField(i);    // state 6
            }
        }

        // Write "no more fields" terminator to the sockets
        for (int i = 0; i < numSplits; i++) {
            yield noMoreFields();      // state 7
        }
    *****/



    @Override
    public boolean hasNext() {
        return state != STATE_DONE;
    }

    @Override
    public NativeTGSinfo next() {
        try {
            while (true) {
                switch (state) {
                    case STATE_INT_FIELD_START:
                        if (currentSplitNum < numSplits) {
                            state = STATE_INT_FIELD_START;
                            final int splitNum = currentSplitNum;
                            currentSplitNum++;
                            return startIntFieldInfo(intFields[currentIteratorIdx], splitNum);
                        }
                    case STATE_INT_FIELD_ITER:
                        if (currentTermIter.hasNext()) {
                            state = STATE_INT_FIELD_ITER;
                            return generateIntTGSInfo(currentTermIter);
                        }
                        ((Closeable) currentTermIter).close();
                        currentTermIter = null;
                        currentSplitNum = 0;
                    case STATE_INT_FIELD_END:
                        if (currentSplitNum < numSplits) {
                            state = STATE_INT_FIELD_END;
                            final int splitNum = currentSplitNum;
                            currentSplitNum++;
                            return endIntFieldInfo(splitNum);
                        }
                        currentSplitNum = 0;
                        currentIteratorIdx++;
                        if (currentIteratorIdx < intFields.length) {
                            /* loop back to STATE_INT_FIELD_START */
                            final String field = intFields[currentIteratorIdx];
                            currentTermIter = multiShardFlamdexReader.intTermOffsetIterator(field);
                            state = STATE_INT_FIELD_START;
                            break;
                        } else if (stringFields.length > 0) {
                            currentIteratorIdx = 0;
                            final String field = stringFields[0];
                            currentTermIter = multiShardFlamdexReader.stringTermOffsetIterator(field);
                            state = STATE_STRING_FIELD_START;
                        } else {
                            state = STATE_NO_MORE_FIELDS;
                            break;
                        }
                    case STATE_STRING_FIELD_START:
                        if (currentSplitNum < numSplits) {
                            state = STATE_STRING_FIELD_START;
                            final int splitNum = currentSplitNum;
                            currentSplitNum++;
                            return startStringFieldInfo(stringFields[currentIteratorIdx], splitNum);
                        }
                    case STATE_STRING_FIELD_ITER:
                        if (currentTermIter.hasNext()) {
                            state = STATE_STRING_FIELD_ITER;
                            return generateStringTGSInfo(currentTermIter);
                        }
                        ((Closeable) currentTermIter).close();
                        currentTermIter = null;
                        currentSplitNum = 0;
                    case STATE_STRING_FIELD_END:
                        if (currentSplitNum < numSplits) {
                            state = STATE_STRING_FIELD_END;
                            final int splitNum = currentSplitNum;
                            currentSplitNum++;
                            return endStringFieldInfo(splitNum);
                        }
                        currentSplitNum = 0;
                        if (currentIteratorIdx + 1 < stringFields.length) {
                            /* loop back to STATE_STRING_FIELD_START */
                            currentIteratorIdx++;
                            final String field = stringFields[currentIteratorIdx];
                            currentTermIter = multiShardFlamdexReader.stringTermOffsetIterator(field);
                            state = STATE_STRING_FIELD_START;
                            break;
                        } else {
                            state = STATE_NO_MORE_FIELDS;
                            break;
                        }
                    case STATE_NO_MORE_FIELDS:
                        if (currentSplitNum < numSplits) {
                            state = STATE_NO_MORE_FIELDS;
                            final int splitNum = currentSplitNum;
                            currentSplitNum++;
                            if (currentSplitNum >= numSplits) {
                                state = STATE_DONE;
                            }
                            return noMoreFieldsInfo(splitNum);
                        }
                        throw new RuntimeException("Fi!");
                    case STATE_DONE:
                    default:
                        throw new NoSuchElementException();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        if (this.currentTermIter != null) {
            ((Closeable) this.currentTermIter).close();
        }
    }


    private NativeTGSinfo startIntFieldInfo(String intField, int splitNum) {
        final NativeTGSinfo info = new NativeTGSinfo();
        info.termDesc = null;
        info.splitIndex = splitNum;
        info.socketNum = splitNum / numWorkers;
        info.operation = NativeTGSinfo.FIELD_START_OPERATION;
        info.fieldName = intField;
        return info;
    }

    private NativeTGSinfo generateIntTGSInfo(Iterator<TermDesc> termIter) {
        final NativeTGSinfo info = new NativeTGSinfo();
        final TermDesc desc = termIter.next();
        info.termDesc = desc;
        info.splitIndex = minHashInt(desc.intTerm, numSplits);
        info.socketNum = info.splitIndex / numWorkers;
        info.operation = NativeTGSinfo.TGS_OPERATION;
        info.fieldName = null;
        return info;

    }

    private NativeTGSinfo endIntFieldInfo(int splitNum) {
        final NativeTGSinfo info = new NativeTGSinfo();
        info.termDesc = null;
        info.splitIndex = splitNum;
        info.socketNum = splitNum / numWorkers;
        info.operation = NativeTGSinfo.FIELD_END_OPERATION;
        info.fieldName = null;
        return info;
    }

    private NativeTGSinfo startStringFieldInfo(String stringField, int splitNum) {
        final NativeTGSinfo info = new NativeTGSinfo();
        info.termDesc = null;
        info.splitIndex = splitNum;
        info.socketNum = splitNum / numWorkers;
        info.operation = NativeTGSinfo.FIELD_START_OPERATION;
        info.fieldName = stringField;
        return info;
    }

    private NativeTGSinfo generateStringTGSInfo(Iterator<TermDesc> termIter) {
        final NativeTGSinfo info = new NativeTGSinfo();
        final TermDesc desc = termIter.next();
        info.termDesc = desc;
        info.splitIndex = minHashString(desc.stringTerm, desc.stringTermLen, numSplits);
        info.socketNum = info.splitIndex / numWorkers;
        info.operation = NativeTGSinfo.TGS_OPERATION;
        info.fieldName = null;
        return info;

    }

    private NativeTGSinfo endStringFieldInfo(int splitNum) {
        final NativeTGSinfo info = new NativeTGSinfo();
        info.termDesc = null;
        info.splitIndex = splitNum;
        info.socketNum = splitNum / numWorkers;
        info.operation = NativeTGSinfo.FIELD_END_OPERATION;
        info.fieldName = null;
        return info;
    }

    private NativeTGSinfo noMoreFieldsInfo(int splitNum) {
        final NativeTGSinfo info = new NativeTGSinfo();
        info.termDesc = null;
        info.splitIndex = splitNum;
        info.socketNum = splitNum / numWorkers;
        info.operation = NativeTGSinfo.NO_MORE_FIELDS_OPERATION;
        info.fieldName = null;
        return info;
    }

}