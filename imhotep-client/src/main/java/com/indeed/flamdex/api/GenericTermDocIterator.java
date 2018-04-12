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

package com.indeed.flamdex.api;

import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import javax.annotation.WillCloseWhenClosed;

public class GenericTermDocIterator<I extends TermIterator> implements TermDocIterator {
    private static final Logger log = Logger.getLogger(GenericTermDocIterator.class);

    protected final I termIterator;

    private final DocIdStream docIdStream;
    private boolean resetStreamBeforeUse;

    protected GenericTermDocIterator(@WillCloseWhenClosed final I termIterator,
                                     @WillCloseWhenClosed final DocIdStream docIdStream) {
        this.termIterator = termIterator;
        this.docIdStream = docIdStream;
    }

    @Override
    public final boolean nextTerm() {
        resetStreamBeforeUse = true;
        return termIterator.next();
    }

    @Override
    public final int docFreq() {
        return termIterator.docFreq();
    }

    @Override
    public final int fillDocIdBuffer(final int[] docIdBuffer) {
        if (resetStreamBeforeUse) {
            docIdStream.reset(termIterator);
            resetStreamBeforeUse = false;
        }
        return docIdStream.fillDocIdBuffer(docIdBuffer);
    }

    @Override
    public final void close() {
        Closeables2.closeAll(log, termIterator, docIdStream);
    }
}
