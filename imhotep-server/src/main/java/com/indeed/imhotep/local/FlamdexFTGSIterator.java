/*
 * Copyright (C) 2014 Indeed Inc.
n *
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

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;

class FlamdexFTGSIterator extends AbstractFlamdexFTGSIterator {

    protected StringTermDocIterator stringTermDocIterator;
    protected IntTermDocIterator intTermDocIterator;

    private final String[] intFields;
    private final String[] stringFields;

    private int intFieldPtr = 0;
    private int stringFieldPtr = 0;

    public FlamdexFTGSIterator(ImhotepLocalSession imhotepLocalSession, SharedReference<FlamdexReader> flamdexReader, String[] intFields, String[] stringFields) {
        super(imhotepLocalSession, flamdexReader);
        this.intFields = intFields;
        this.stringFields = stringFields;
    }

    @Override
    public final boolean nextField() {
        // todo: reset/cleanup term iterators etc that are in progress
        synchronized (session) {
            if (intFieldPtr < intFields.length) {
                currentField = intFields[intFieldPtr++];
                currentFieldIsIntType = true;
                if (intTermDocIterator != null) Closeables2.closeQuietly(intTermDocIterator, ImhotepLocalSession.log);
                intTermDocIterator = flamdexReader.get().getIntTermDocIterator(currentField);
                termIndex = 0;
                return true;
            }
            if (stringFieldPtr < stringFields.length) {
                currentField = stringFields[stringFieldPtr++];
                currentFieldIsIntType = false;
                if (stringTermDocIterator != null) Closeables2.closeQuietly(stringTermDocIterator, ImhotepLocalSession.log);
                stringTermDocIterator = flamdexReader.get().getStringTermDocIterator(currentField);
                termIndex = 0;
                return true;
            }
            currentField = null;
            close();
            if (ImhotepLocalSession.logTiming) {
                ImhotepLocalSession.log.info("intTermsTime: "+intTermsTime/1000000d+" ms, stringTermsTime: "+stringTermsTime/1000000d+" ms, docsTime: "+docsTime/1000000d+" ms, lookupsTime: "+lookupsTime/1000000d+" ms, timingErrorTime: "+timingErrorTime/1000000d+" ms");
            }
            return false;
        }
    }

    @Override
    public final void close() {
        synchronized (session) {
            Closeables2.closeQuietly(intTermDocIterator, ImhotepLocalSession.log);
            intTermDocIterator = null;
            Closeables2.closeQuietly(stringTermDocIterator, ImhotepLocalSession.log);
            stringTermDocIterator = null;
            Closeables2.closeQuietly(flamdexReader, ImhotepLocalSession.log);
            flamdexReader = null;
        }
    }

    @Override
    public final boolean nextTerm() {
        if (currentField == null) return false;
        resetGroupStats = true;
        if (currentFieldIsIntType) {
            if (ImhotepLocalSession.logTiming) intTermsTime -= System.nanoTime();
            final boolean ret = intTermDocIterator.nextTerm();
            if (ImhotepLocalSession.logTiming) intTermsTime += System.nanoTime();
            return ret;
        } else {
            if (ImhotepLocalSession.logTiming) stringTermsTime -= System.nanoTime();
            final boolean ret = stringTermDocIterator.nextTerm();
            if (ImhotepLocalSession.logTiming) stringTermsTime += System.nanoTime();
            return ret;
        }
    }

    @Override
    public final long termDocFreq() {
        return currentFieldIsIntType ? intTermDocIterator.docFreq() : stringTermDocIterator.docFreq();
    }

    @Override
    public final long termIntVal() {
        return intTermDocIterator.term();
    }

    @Override
    public final String termStringVal() {
        return stringTermDocIterator.term();
    }

    protected int fillDocIdBuffer() {
        return (currentFieldIsIntType?intTermDocIterator:stringTermDocIterator).fillDocIdBuffer(session.docIdBuf);
    }
}
