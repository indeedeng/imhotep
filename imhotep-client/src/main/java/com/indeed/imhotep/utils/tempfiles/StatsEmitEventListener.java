package com.indeed.imhotep.utils.tempfiles;

import com.google.common.collect.ImmutableList;
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.util.core.Pair;

public class StatsEmitEventListener implements EventListener {
    private final MetricStatsEmitter statsEmitter;
    private final String statsName;

    public StatsEmitEventListener(final MetricStatsEmitter statsEmitter, final String statsName) {
        this.statsEmitter = statsEmitter;
        this.statsName = statsName;
    }

    private ImmutableList<Pair<String, String>> getTags(final String eventType, final TempFileState tempFileState) {
        final boolean isReferenced = tempFileState.getRefCount() > 0;
        final boolean isRemoved = tempFileState.isRemoved();
        return ImmutableList.of(
                Pair.of("event", eventType),
                Pair.of("filetype", tempFileState.getTempFileType().getIdentifier()),
                Pair.of("removed", isRemoved ? "1" : "0"),
                Pair.of("referenced", isReferenced ? "1" : "0")
        );
    }

    @Override
    public void removeTwice(final TempFileState tempFileState) {
        statsEmitter.count(statsName, 1, getTags("removeTwice", tempFileState));
    }

    @Override
    public void removeReferencedFile(final TempFileState tempFileState) {
        statsEmitter.count(statsName, 1, getTags("removeReferenced", tempFileState));
    }

    @Override
    public void didNotCloseInputStream(final TempFileState tempFileState) {
        statsEmitter.count(statsName, 1, getTags("leakInputStream", tempFileState));
    }

    @Override
    public void didNotCloseOutputStream(final TempFileState tempFileState) {
        statsEmitter.count(statsName, 1, getTags("leakOutputStream", tempFileState));
    }

    @Override
    public void didNotRemoveTempFile(final TempFileState tempFileState) {
        statsEmitter.count(statsName, 1, getTags("leakFile", tempFileState));
    }

    @Override
    public void expired(final TempFileState tempFileState) {
        statsEmitter.count(statsName, 1, getTags("expiration", tempFileState));
    }
}
