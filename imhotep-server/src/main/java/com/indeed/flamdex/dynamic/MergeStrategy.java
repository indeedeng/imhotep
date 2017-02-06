package com.indeed.flamdex.dynamic;

import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.SortedSet;

/**
 * @author michihiko
 */

public interface MergeStrategy {
    class Segment implements Comparable<Segment> {
        private final Path segmentPath;
        private final int numDocs;

        public Segment(@Nonnull final Path segmentPath, final int numDocs) {
            this.segmentPath = segmentPath;
            this.numDocs = numDocs;
        }

        public int getNumDocs() {
            return numDocs;
        }

        @Nonnull
        Path getSegmentDirectory() {
            return segmentPath;
        }

        @Override
        public int compareTo(@Nonnull final Segment o) {
            return ComparisonChain.start()
                    .compare(numDocs, o.numDocs)
                    .compare(segmentPath, o.segmentPath)
                    .result();
        }
    }

    @Nonnull
    Collection<? extends Collection<Segment>> splitSegmentsToMerge(@Nonnull final SortedSet<Segment> segments);

    class ExponentialMergeStrategy implements MergeStrategy {
        private final int exp;
        private final int maxNumOfTasks;

        public ExponentialMergeStrategy(final int exp) {
            this(exp, Integer.MAX_VALUE);
        }

        public ExponentialMergeStrategy(final int exp, final int maxNumOfTasks) {
            Preconditions.checkArgument(exp > 1);
            this.exp = exp;
            this.maxNumOfTasks = maxNumOfTasks;
        }

        @Nonnull
        @Override
        public List<List<Segment>> splitSegmentsToMerge(@Nonnull final SortedSet<Segment> segments) {
            final List<List<Segment>> segmentsToMerge = new ArrayList<>();
            final Queue<Segment> currentSegments = new ArrayDeque<>();
            for (final Segment segment : segments) {
                while (!currentSegments.isEmpty() && ((currentSegments.element().getNumDocs() * exp) <= segment.getNumDocs())) {
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
