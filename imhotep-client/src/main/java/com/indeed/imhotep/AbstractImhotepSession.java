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
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.exceptions.GenericImhotepKnownException;
import com.indeed.imhotep.exceptions.ImhotepKnownException;
import com.indeed.imhotep.exceptions.QueryCancelledException;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.StatsSortOrder;

import javax.annotation.Nullable;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

/**
 * @author jsadun
 */
public abstract class AbstractImhotepSession implements ImhotepSession {

    protected final Instrumentation.ProviderSupport instrumentation =
        new Instrumentation.ProviderSupport();

    private final String sessionId;

    protected AbstractImhotepSession(final String sessionId) {
        this.sessionId = sessionId;
    }

    @Override
    public final String getSessionId() {
        return sessionId;
    }

    @Nullable
    public Path getShardPath() {
        return null;
    }

    /**
     * Read numStats without providing any guarantees about reading
     * the most up to date value
     */
    public int weakGetNumStats() {
        return -1;
    }

    /**
     * Read numGroups without providing any guarantees about reading
     * the most up to date value
     */
    public int weakGetNumGroups() {
        return -1;
    }

    @Override
    public FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields, @Nullable final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        return getFTGSIterator(intFields, stringFields, 0, stats);
    }

    @Override
    public FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields, final long termLimit, @Nullable final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        return getFTGSIterator(intFields, stringFields, termLimit, -1, stats, StatsSortOrder.UNDEFINED);
    }

    @Override
    public FTGSIterator getFTGSIterator(
            final String[] intFields,
            final String[] stringFields,
            final long termLimit,
            final int sortStat,
            @Nullable final List<List<String>> stats,
            final StatsSortOrder statsSortOrder) throws ImhotepOutOfMemoryException {
        final FTGSParams params = new FTGSParams(intFields, stringFields, termLimit, sortStat, true, stats, statsSortOrder);
        return getFTGSIterator(params);
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
    public int regroup(final GroupMultiRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        return regroup(rawRules, false);
    }

    @Override
    public int regroupWithProtos(final GroupMultiRemapMessage[] rawRuleMessages,
                          final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        throw newUnsupportedOperationException("Local imhotep sessions don't use protobufs, only remote sessions do");
    }

    @Override
    public int metricRegroup(final List<String> stat, final long min, final long max, final long intervalSize) throws ImhotepOutOfMemoryException {
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

    protected static void checkSplitParams(final int splitIndex, final int numSplits) {
        checkSplitParams(numSplits);
        if ((splitIndex < 0) || (splitIndex >= numSplits)) {
            throw new IllegalArgumentException("Illegal splitIndex: " + splitIndex);
        }
    }

    protected static void checkSplitParams(final int numSplits) {
        if (numSplits < 2) {
            throw new IllegalArgumentException("At least 2 splits expected");
        }
    }

    protected UnsupportedOperationException newUnsupportedOperationException(final String message) {
        return new UnsupportedOperationException(createMessageWithSessionId(message));
    }

    protected IllegalArgumentException newIllegalArgumentException(final String message) {
        return new IllegalArgumentException(createMessageWithSessionId(message));
    }

    protected IllegalStateException newIllegalStateException(final String s) {
        return new IllegalStateException(createMessageWithSessionId(s));
    }

    protected IllegalArgumentException newIllegalArgumentException(final String message, final Throwable cause) {
        return new IllegalArgumentException(createMessageWithSessionId(message), cause);
    }

    protected QueryCancelledException newQueryCancelledException(final Throwable cause) {
        return new QueryCancelledException(createMessageWithSessionId(cause.toString()), cause);
    }

    protected ImhotepKnownException newImhotepKnownException(final Throwable cause) {
        return new GenericImhotepKnownException(createMessageWithSessionId(cause.toString()), cause);
    }

    protected RuntimeException newRuntimeException(final Throwable cause) {
        if (cause instanceof ClosedByInterruptException) {
            return newQueryCancelledException(cause);
        } else if (cause instanceof ImhotepKnownException) {
            return newImhotepKnownException(cause);
        } else {
            return new RuntimeException(createMessageWithSessionId(cause.toString()), cause);
        }
    }

    protected RuntimeException newRuntimeException(final String message) {
        return new RuntimeException(createMessageWithSessionId(message));
    }

    protected RuntimeException newRuntimeException(final String message, final Throwable cause) {
        return new RuntimeException(createMessageWithSessionId(message), cause);
    }

    public ImhotepOutOfMemoryException newImhotepOutOfMemoryException() {
        return new ImhotepOutOfMemoryException(createMessageWithSessionId("Not enough memory"));
    }

    public static ImhotepOutOfMemoryException newImhotepOutOfMemoryException(final String sessionId) {
        return new ImhotepOutOfMemoryException(createMessageWithSessionId("Not enough memory", sessionId));
    }

    protected ImhotepOutOfMemoryException newImhotepOutOfMemoryException(final Throwable cause) {
        return new ImhotepOutOfMemoryException(createMessageWithSessionId(cause.toString()), cause);
    }

    protected String createMessageWithSessionId(final String message) {
        return createMessageWithSessionId(message, sessionId);
    }

    protected static String createMessageWithSessionId(final String message, final String sessionId) {
        return "[" + sessionId + "] " + message;
    }
}
