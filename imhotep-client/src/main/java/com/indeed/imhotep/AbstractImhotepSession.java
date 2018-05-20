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

import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;

import java.util.Iterator;

/**
 * @author jsadun
 */
public abstract class AbstractImhotepSession implements ImhotepSession {

    protected final Instrumentation.ProviderSupport instrumentation =
        new Instrumentation.ProviderSupport();

    @Override
    public FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields) {
        return getFTGSIterator(intFields, stringFields, 0);
    }

    @Override
    public FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields, final long termLimit) {
        return getFTGSIterator(intFields, stringFields, termLimit, -1);
    }

    @Override
    public int regroup(final int numRawRules, final Iterator<GroupMultiRemapRule> rawRules) throws ImhotepOutOfMemoryException {
        return regroup(numRawRules, rawRules, false);
    }

    @Override
    public int regroup(final int numRawRules, final Iterator<GroupMultiRemapRule> rawRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        final GroupMultiRemapRuleArray rulesArray = new GroupMultiRemapRuleArray(numRawRules, rawRules);
        return regroup(rulesArray.elements(), errorOnCollisions);
    }

    @Override
    public int regroup2(final int numRawRules, final Iterator<GroupRemapRule> iterator) throws ImhotepOutOfMemoryException {
        final GroupRemapRuleArray rulesArray = new GroupRemapRuleArray(numRawRules, iterator);
        return regroup(rulesArray.elements());
    }

    @Override
    public int regroup(final GroupMultiRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        return regroup(rawRules, false);
    }

    @Override
    public int regroupWithProtos(final GroupMultiRemapMessage[] rawRuleMessages,
                          final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        throw new UnsupportedOperationException("Local imhotep sessions don't use protobufs, only remote sessions do");
    }

    @Override
    public int metricRegroup(final int stat, final long min, final long max, final long intervalSize) throws ImhotepOutOfMemoryException {
        return metricRegroup(stat, min, max, intervalSize, false);
    }

    @Override
    public void addObserver(final Instrumentation.Observer observer) {
        instrumentation.addObserver(observer);
    }

    @Override
    public void removeObserver(final Instrumentation.Observer observer) {
        instrumentation.removeObserver(observer);
    }
}
