package com.indeed.flamdex.dynamic;

import com.google.common.base.Function;
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

        static final Function<Segment, Path> SEGMENT_PATH_GETTER = new Function<Segment, Path>() {
            @Override
            public Path apply(final Segment segment) {
                return segment.getSegmentDirectory();
            }
        };

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

    /**
     * This is the method you have to implement;
     * for given available segments, return disjoint subsets of available segments that you want to merge.
     * Available segments are given in ascending order w.r.t. number of segments.
     */
    @Nonnull
    Collection<? extends Collection<Segment>> splitSegmentsToMerge(@Nonnull final SortedSet<Segment> availableSegments);

    /**
     * This implementation merges {@code base} segments which have "almost same size" (w.r.t. {@code base})
     * to satisfy following condition;
     * There are no subset of segments which has
     * - {@code base} number of elements, and
     * - ratio between maximum and minimum size of the segments is less than {@code base}.
     * <p>
     * This merge strategy has following feature;
     * - There are no more segments that we can merge by this strategy
     * iff the condition above is satisfied,
     * and if so, the number of remaining segments is less or equals to ({@code base} - 1) * log_{@code base} (total number of documents)
     * - The segment created by a merge is at least {@code base} times as large as a segment used to that merge.
     * Hence, each documents contribute to merge at most log_{@code base} (total number of documents) time.
     */
    class ExponentialMergeStrategy implements MergeStrategy {
        private final int base;
        private final int maxNumOfTasks;

        public ExponentialMergeStrategy(final int base) {
            this(base, Integer.MAX_VALUE);
        }

        public ExponentialMergeStrategy(final int base, final int maxNumOfTasks) {
            Preconditions.checkArgument(base > 1);
            this.base = base;
            this.maxNumOfTasks = maxNumOfTasks;
        }

        @Nonnull
        @Override
        public List<List<Segment>> splitSegmentsToMerge(@Nonnull final SortedSet<Segment> availableSegments) {
            // Use two-pointer-method to list up segments we can merge.
            // Invariants:
            // - currentSegments stores some segments in ascending order w.r.t. number of docs, and
            //   (number of docs in the first segment in the queue (= smallest)) * base
            //   > (number of docs in the last segment in the queue (= largest))

            final List<List<Segment>> segmentsToMerge = new ArrayList<>();
            final Queue<Segment> currentSegments = new ArrayDeque<>();
            for (final Segment segment : availableSegments) {
                currentSegments.add(segment);
                // Remove segments to satisfy the invariant.
                while ((currentSegments.element().getNumDocs() * base) <= segment.getNumDocs()) {
                    currentSegments.remove();
                }
                if (currentSegments.size() >= base) {
                    // We have enough segments to merge.
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
