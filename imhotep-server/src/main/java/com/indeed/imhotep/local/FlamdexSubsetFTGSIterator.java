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
 package com.indeed.imhotep.local;

import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;

import java.util.Iterator;
import java.util.Map;

class FlamdexSubsetFTGSIterator extends AbstractFlamdexFTGSIterator {

    protected StringTermIterator stringTermIterator;
    private IntTermIterator intTermIterator;
    private final DocIdStream docIdStream;

    private final Iterator<Map.Entry<String, long[]>> intFieldToTermsIterator;
    private final Iterator<Map.Entry<String, String[]>> stringFieldToTermsIterator;

    private long[] currentIntFieldTerms;
    private String[] currentStringFieldTerms;
    private int currentFieldTermPtr = -1;

    public FlamdexSubsetFTGSIterator(
            final ImhotepLocalSession imhotepLocalSession,
            final SharedReference<FlamdexReader> flamdexReader,
            final Map<String, long[]> intFieldToTerms,
            final Map<String, String[]> stringFieldToTerms) {
        super(imhotepLocalSession, flamdexReader);
        this.intFieldToTermsIterator = intFieldToTerms.entrySet().iterator();
        this.stringFieldToTermsIterator = stringFieldToTerms.entrySet().iterator();
        docIdStream = flamdexReader.get().getDocIdStream();
    }

    @Override
    public final boolean nextField() {
        // todo: reset/cleanup term iterators etc that are in progress
        synchronized (session) {
            if (intFieldToTermsIterator.hasNext()) {
                final Map.Entry<String, long[]> entry = intFieldToTermsIterator.next();
                currentField = entry.getKey();
                currentIntFieldTerms = entry.getValue();
                currentFieldTermPtr = -1;
                currentFieldIsIntType = true;
                if (intTermIterator != null) {
                    Closeables2.closeQuietly(intTermIterator, ImhotepLocalSession.log);
                }
                intTermIterator = flamdexReader.get().getUnsortedIntTermIterator(currentField);
                termIndex = 0;
                return true;
            }
            if (stringFieldToTermsIterator.hasNext()) {
                final Map.Entry<String, String[]> entry = stringFieldToTermsIterator.next();
                currentField = entry.getKey();
                currentStringFieldTerms = entry.getValue();
                currentFieldTermPtr = -1;
                currentFieldIsIntType = false;
                if (stringTermIterator != null) {
                    Closeables2.closeQuietly(stringTermIterator, ImhotepLocalSession.log);
                }
                stringTermIterator = flamdexReader.get().getStringTermIterator(currentField);
                termIndex = 0;
                return true;
            }
            currentField = null;
            close();
            if (ImhotepLocalSession.logTiming) {
                ImhotepLocalSession.log.info("intTermsTime: "+intTermsTime/1000000d
                        +" ms, stringTermsTime: "+stringTermsTime/1000000d
                        +" ms, docsTime: "+docsTime/1000000d
                        +" ms, lookupsTime: "+lookupsTime/1000000d
                        +" ms, termFreqTime: "+termFreqTime/1000000d
                        +" ms, timingErrorTime: "+timingErrorTime/1000000d+" ms");
            }
            return false;
        }
    }

    @Override
    public final void close() {
        super.close();
        synchronized (session) {
            Closeables2.closeQuietly(docIdStream, ImhotepLocalSession.log);
            Closeables2.closeQuietly(intTermIterator, ImhotepLocalSession.log);
            intTermIterator = null;
            Closeables2.closeQuietly(stringTermIterator, ImhotepLocalSession.log);
            stringTermIterator = null;
            Closeables2.closeQuietly(flamdexReader, ImhotepLocalSession.log);
            flamdexReader = null;
        }
    }

    @Override
    public final boolean nextTerm() {
        if (currentField == null) {
            return false;
        }
        resetGroupStats = true;
        if (currentFieldIsIntType) {
            if (ImhotepLocalSession.logTiming) {
                intTermsTime -= System.nanoTime();
            }
            try {
                while (true) {
                    if (currentFieldTermPtr + 1 >= currentIntFieldTerms.length) {
                        return false;
                    }
                    currentFieldTermPtr++;
                    intTermIterator.reset(currentIntFieldTerms[currentFieldTermPtr]);
                    if (intTermIterator.next() && intTermIterator.term() == currentIntFieldTerms[currentFieldTermPtr]) {
                        docIdStream.reset(intTermIterator);
                        return true;
                    }
                }
            } finally {
                if (ImhotepLocalSession.logTiming) {
                    intTermsTime += System.nanoTime();
                }
            }
        } else {
            if (ImhotepLocalSession.logTiming) {
                stringTermsTime -= System.nanoTime();
            }
            try {
                while (true) {
                    if (currentFieldTermPtr + 1 >= currentStringFieldTerms.length) {
                        return false;
                    }
                    currentFieldTermPtr++;
                    stringTermIterator.reset(currentStringFieldTerms[currentFieldTermPtr]);
                    if (stringTermIterator.next() && stringTermIterator.term().equals(currentStringFieldTerms[currentFieldTermPtr])) {
                        docIdStream.reset(stringTermIterator);
                        return true;
                    }
                }
            } finally {
                if (ImhotepLocalSession.logTiming) {
                    stringTermsTime += System.nanoTime();
                }
            }
        }
    }

    @Override
    public final long termDocFreq() {
        return currentFieldIsIntType ? intTermIterator.docFreq() : stringTermIterator.docFreq();
    }

    @Override
    public final long termIntVal() {
        return intTermIterator.term();
    }

    @Override
    public final String termStringVal() {
        return stringTermIterator.term();
    }

    @Override
    public byte[] termStringBytes() {
        return stringTermIterator.termStringBytes();
    }

    @Override
    public int termStringLength() {
        return stringTermIterator.termStringLength();
    }

    @Override
    protected int fillDocIdBuffer(final int[] docIdBuf) {
        return docIdStream.fillDocIdBuffer(docIdBuf);
    }
}
