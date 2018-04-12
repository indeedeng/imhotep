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

package com.indeed.flamdex.dynamic;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;

/**
 * @author michihiko
 */

public interface MergeStrategy {
    class Segment {
        private final Path segmentDirectory;
        private final int numDocs;

        static final Function<Segment, Path> SEGMENT_DIRECTORY_GETTER = new Function<Segment, Path>() {
            @Override
            public Path apply(final Segment segment) {
                return segment.getSegmentDirectory();
            }
        };

        public static final Comparator<Segment> NUM_DOC_COMPARATOR = new Comparator<Segment>() {
            @Override
            public int compare(final Segment o1, final Segment o2) {
                return ComparisonChain.start()
                        .compare(o1.getNumDocs(), o2.getNumDocs())
                        .compare(o1.getSegmentDirectory(), o2.getSegmentDirectory())
                        .result();
            }
        };

        public Segment(@Nonnull final Path segmentDirectory, final int numDocs) {
            this.segmentDirectory = segmentDirectory;
            this.numDocs = numDocs;
        }

        public int getNumDocs() {
            return numDocs;
        }

        @Nonnull
        Path getSegmentDirectory() {
            return segmentDirectory;
        }
    }

    /**
     * This is the method you have to implement;
     * for given available segments, return disjoint subsets of available segments that you want to merge.
     */
    @Nonnull
    Collection<? extends Collection<Segment>> splitSegmentsToMerge(@Nonnull final List<Segment> availableSegments);

    /**
     * This implementation merges {@code base} segments which have "almost same size" (w.r.t. {@code base})
     * to satisfy following condition;
     * There are no subset of segments which has
     * <ul>
     * <li> {@code base} number of elements, and
     * <li> ratio between maximum and minimum size of the segments is less than {@code base}.
     * </ul>
     * This merge strategy has following feature;
     * <ul>
     * <li> There are no more segments that we can merge by this strategy iff the condition above is satisfied,
     * and if so, the number of remaining segments is less or equals to ({@code base} - 1) * log_{@code base} (total number of documents)
     * <li> The segment created by a merge is at least {@code base} times as large as a segment used to that merge.
     * Hence, each documents contribute to merge at most log_{@code base} (total number of documents) times.
     * </ul>
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
        public List<List<Segment>> splitSegmentsToMerge(@Nonnull final List<Segment> availableSegments) {
            // Use two-pointer-method to list up segments we can merge.
            // Invariants:
            // - currentSegments stores some segments in ascending order w.r.t. number of docs, and
            //   (number of docs in the first segment in the queue (= smallest)) * base
            //   > (number of docs in the last segment in the queue (= largest))

            final List<List<Segment>> segmentsToMerge = new ArrayList<>();
            final Queue<Segment> currentSegments = new ArrayDeque<>();
            for (final Segment segment : Ordering.from(Segment.NUM_DOC_COMPARATOR).sortedCopy(availableSegments)) {
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
