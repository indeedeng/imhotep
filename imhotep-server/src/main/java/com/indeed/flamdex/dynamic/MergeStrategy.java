package com.indeed.flamdex.dynamic;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.SortedSet;

/**
 * @author michihiko
 */

public abstract class MergeStrategy {
    @Nonnull
    abstract Collection<? extends Collection<SegmentInfo>> splitSegmentsToMerge(@Nonnull final SortedSet<SegmentInfo> segments);

    public static class ExponentialMergeStrategy extends MergeStrategy {
        private final int exp;
        private final int maxNumOfTasks;

        public ExponentialMergeStrategy(final int exp, final int maxNumOfTasks) {
            Preconditions.checkArgument(exp > 1);
            this.exp = exp;
            this.maxNumOfTasks = maxNumOfTasks;
        }

        @Nonnull
        @Override
        List<List<SegmentInfo>> splitSegmentsToMerge(@Nonnull final SortedSet<SegmentInfo> segments) {
            final List<List<SegmentInfo>> segmentsToMerge = new ArrayList<>();
            final Queue<SegmentInfo> currentSegments = new ArrayDeque<>();
            for (final SegmentInfo segment : segments) {
                while (!currentSegments.isEmpty() && ((currentSegments.element().getNumDocs() * exp) < segment.getNumDocs())) {
                    currentSegments.remove();
                }
                currentSegments.add(segment);
                if (currentSegments.size() >= exp) {
                    segmentsToMerge.add(ImmutableList.copyOf(currentSegments));
                    currentSegments.clear();
                    if (segmentsToMerge.size() >= maxNumOfTasks) {
                        break;
                    }
                }
            }
            return segmentsToMerge;
        }
    }
}
