package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.imhotep.io.RequestTools;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.Operator;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * @see com.indeed.imhotep.AsynchronousRemoteImhotepSession
 */
public class AsynchronousRemoteImhotepMultiSession extends AbstractImhotepMultiSession<AsynchronousRemoteImhotepSession> {
    final RemoteImhotepMultiSession original;

    public AsynchronousRemoteImhotepMultiSession(final AsynchronousRemoteImhotepSession[] asyncSessions, final RemoteImhotepMultiSession original) {
        super(original.getSessionId(), asyncSessions, original.tempFileSizeBytesLeft, original.getUserName(), original.getClientName());
        this.original = original;
    }

    public void synchronizeAll() {
        for (final AsynchronousRemoteImhotepSession session : sessions) {
            session.synchronize();
        }
    }

    // TODO: These methods could run faster if we used futures for waiting and computing
    //       the per-session result, and then waited for the combined future

    @Override
    public long[] getGroupStats(final String groupsName, final List<String> stat) throws ImhotepOutOfMemoryException {
        synchronizeAll();
        return original.getGroupStats(groupsName, stat);
    }

    @Override
    public GroupStatsIterator getGroupStatsIterator(final String groupsName, final List<String> stat) throws ImhotepOutOfMemoryException {
        synchronizeAll();
        return original.getGroupStatsIterator(groupsName, stat);
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(final String groupsName, final Map<String, long[]> intFields, final Map<String, String[]> stringFields, @Nullable final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        synchronizeAll();
        return original.getSubsetFTGSIterator(groupsName, intFields, stringFields, stats);
    }

    @Override
    public FTGSIterator getFTGSIterator(final String groupsName, final FTGSParams params) throws ImhotepOutOfMemoryException {
        synchronizeAll();
        return original.getFTGSIterator(groupsName, params);
    }

    @Override
    public GroupStatsIterator getDistinct(final String groupsName, final String field, final boolean isIntField) {
        synchronizeAll();
        return original.getDistinct(groupsName, field, isIntField);
    }

    @Override
    public int regroup(final RegroupParams regroupParams, final GroupMultiRemapRule[] rawRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        for (final AsynchronousRemoteImhotepSession session : sessions) {
            session.regroup(regroupParams, rawRules, errorOnCollisions);
        }
        return -999;
    }

    @Override
    public void intOrRegroup(final RegroupParams regroupParams, final String field, final long[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        for (final AsynchronousRemoteImhotepSession session : sessions) {
            session.intOrRegroup(regroupParams, field, terms, targetGroup, negativeGroup, positiveGroup);
        }
    }

    @Override
    public void stringOrRegroup(final RegroupParams regroupParams, final String field, final String[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        for (final AsynchronousRemoteImhotepSession session : sessions) {
            session.stringOrRegroup(regroupParams, field, terms, targetGroup, negativeGroup, positiveGroup);
        }
    }

    @Override
    public int regroup(final RegroupParams regroupParams, final int[] fromGroups, final int[] toGroups, final boolean filterOutNotTargeted) throws ImhotepOutOfMemoryException {
        for (final AsynchronousRemoteImhotepSession session : sessions) {
            session.regroup(regroupParams, fromGroups, toGroups, filterOutNotTargeted);
        }
        return -999;
    }

    @Override
    public int regroupWithProtos(final RegroupParams regroupParams, final GroupMultiRemapMessage[] rawRuleMessages, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        for (final AsynchronousRemoteImhotepSession session : sessions) {
            session.regroupWithProtos(regroupParams, rawRuleMessages, errorOnCollisions);
        }
        return -999;
    }

    public void regroupWithRuleSender(final RegroupParams regroupParams, final RequestTools.GroupMultiRemapRuleSender sender, final boolean errorOnCollisions) {
        for (final AsynchronousRemoteImhotepSession session : sessions) {
            session.regroupWithRuleSender(regroupParams, sender, errorOnCollisions);
        }
    }

    public long getTempFilesBytesWritten() {
        return original.getTempFilesBytesWritten();
    }

    @Override
    public String toString() {
        return "AsynchronousRemoteImhotepMultiSession{" +
                "original=" + original +
                '}';
    }
}
