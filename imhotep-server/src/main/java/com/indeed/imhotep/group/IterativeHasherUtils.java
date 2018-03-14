package com.indeed.imhotep.group;

import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.RawStringTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.api.TermIterator;

import java.io.Closeable;
import java.util.Arrays;

public class IterativeHasherUtils {
    private IterativeHasherUtils() {
    }

    // iterator that wraps term iterator and output terms hashes
    public interface TermHashIterator extends Closeable {
        boolean hasNext();
        int getHash();
        DocIdStream getDocIdStream();

        @Override
        void close();
    }

    private abstract static class TermHashIterarorImpl implements TermHashIterator {
        private final DocIdStream docIdStream;
        private final TermIterator iterator;

        TermHashIterarorImpl(final TermIterator iterator,
                             final DocIdStream docIdStream) {
            this.iterator = iterator;
            this.docIdStream = docIdStream;
        }

        @Override
        public final boolean hasNext() {
            return iterator.next();
        }

        @Override
        public final DocIdStream getDocIdStream() {
            docIdStream.reset(iterator);
            return docIdStream;
        }

        @Override
        public final void close() {
            iterator.close();
            docIdStream.close();
        }
    }

    private static class StringTermHashIteraror extends TermHashIterarorImpl {
        private final StringTermIterator iterator;
        private final IterativeHasher.StringHasher hasher;
        private StringTermHashIteraror(final StringTermIterator iterator,
                                       final DocIdStream docIdStream,
                                       final IterativeHasher.StringHasher hasher) {
            super(iterator, docIdStream);
            this.iterator = iterator;
            this.hasher = hasher;
        }

        @Override
        public int getHash() {
            final String term = iterator.term();
            return hasher.calculateHash(term);
        }
    }

    private static class ByteArrayTermHashIteraror extends TermHashIterarorImpl {
        private final RawStringTermIterator iterator;
        private final IterativeHasher.ByteArrayHasher hasher;
        private ByteArrayTermHashIteraror(final RawStringTermIterator iterator,
                                          final DocIdStream docIdStream,
                                          final IterativeHasher.ByteArrayHasher hasher) {
            super(iterator, docIdStream);
            this.iterator = iterator;
            this.hasher = hasher;
        }

        @Override
        public int getHash() {
            final byte[] termBytes = iterator.termStringBytes();
            final int len = iterator.termStringLength();
            return hasher.calculateHash(termBytes, len);
        }
    }

    private static class ConsistentLongTermHashIteraror extends TermHashIterarorImpl {
        private final IntTermIterator iterator;
        private final IterativeHasher.ConsistentLongHasher hasher;
        private ConsistentLongTermHashIteraror(final IntTermIterator iterator,
                                               final DocIdStream docIdStream,
                                               final IterativeHasher.ConsistentLongHasher hasher) {
            super(iterator, docIdStream);
            this.iterator = iterator;
            this.hasher = hasher;
        }

        @Override
        public int getHash() {
            final long term = iterator.term();
            return hasher.calculateHash(term);
        }
    }

    public static TermHashIterator create(final FlamdexReader reader,
                                          final String field,
                                          final boolean isIntField,
                                          final String salt) {
        final DocIdStream stream = reader.getDocIdStream();
        final IterativeHasher hasher = new IterativeHasher.Murmur3Hasher(salt);
        if (isIntField) {
            final IntTermIterator iterator = reader.getIntTermIterator(field);
            return new ConsistentLongTermHashIteraror(iterator, stream, hasher.consistentLongHasher());
        } else {
            final StringTermIterator iterator = reader.getStringTermIterator(field);
            if (iterator instanceof RawStringTermIterator) {
                final RawStringTermIterator rawIterator = (RawStringTermIterator)iterator;
                return new ByteArrayTermHashIteraror(rawIterator, stream, hasher.byteArrayHasher());
            } else {
                return new StringTermHashIteraror(iterator, stream, hasher.stringHasher());
            }
        }
    }

    // interface for converting hash into group index
    public interface GroupChooser {
        int getGroup(int hash);
    }

    public static class OneGroupChooser implements GroupChooser {
        @Override
        public int getGroup(final int hash) {
            return 0;
        }
    }

    public static class TwoGroupChooser implements GroupChooser {
        private final int threshold;

        public TwoGroupChooser(final int threshold) {
            this.threshold = threshold;
        }

        @Override
        public int getGroup(final int hash) {
            return (Math.abs(hash) >= threshold) ? 1 : 0;
        }
    }

    // If group percentiles correspond as small numbers ratio,
    // i.e. 20%-80% (p ={0.2}) or 30%-30%-40% (p={0.3, 0.6})
    // then we can have small array of group indexes and divisor constant
    // to calculate group index from hash as indexes[Math.abs(hash)/divisor]
    // For 20%-80% case indexes = {0, 1, 1, 1, 1} and divisor = Integer.MAX_VALUE/5
    // and for 30%-30%-40% case indexes = {0, 0, 0, 1, 1, 1, 2, 2, 2, 2} and divisor = Integer.MAX_VALUE/10
    public static class ProportionalMultiGroupChooser implements GroupChooser {
        private final int[] indexes;
        private final int divisor;

        private ProportionalMultiGroupChooser(final int divisor, final int[] indexes) {
            this.divisor = divisor;
            this.indexes = indexes;
        }

        // Create this class if there is index array not greater than maxSize
        // with relative error lower than maxError
        public static ProportionalMultiGroupChooser tryCreate(
                final double[] percentiles,
                final double maxError,
                final int maxSize) {
            if (percentiles.length < 2) {
                // another class should be used
                return null;
            }
            for (int size = percentiles.length; size < maxSize; size++) {
                boolean isOk = true;
                for (final double p: percentiles) {
                    if (Math.abs((p * size) - (int)(p*size)) > maxError) {
                        isOk = false;
                        break;
                    }
                }
                if (isOk) {
                    final int[] groups = new int[size];
                    final int[] pAsInt = new int[percentiles.length + 1];
                    for (int i = 0; i < percentiles.length; i++) {
                        pAsInt[i] = (int)(percentiles[i] * size);
                    }
                    pAsInt[percentiles.length] = size;
                    for (int i = 0; i < pAsInt.length; i++) {
                        final int from = (i == 0) ? 0 : pAsInt[i-1];
                        final int to = pAsInt[i];
                        for (int pos = from; pos < to; pos++) {
                            groups[pos] = i;
                        }
                    }
                    // getGroup(Integer.MAX_VALUE) should return last group
                    // so we need to round result of division up.
                    // Since Integer.MAX_VALUE is prime and percentiles.length is greater than 1
                    // rounding must be applied every time.
                    final int divisor = (Integer.MAX_VALUE / size) + 1;
                    return new ProportionalMultiGroupChooser(divisor, groups);
                }
            }

            return null;
        }

        @Override
        public int getGroup(final int hash) {
            return indexes[Math.abs(hash)/divisor];
        }
    }

    // finding group index based on group bounds.
    public static class MultiGroupChooser implements GroupChooser {
        private final int[] groupBounds;

        public MultiGroupChooser(final int[] bounds) {
            this.groupBounds = bounds;
        }

        @Override
        public int getGroup(final int hash) {
            final int absHash = Math.abs(hash);
            final int pos = Arrays.binarySearch(groupBounds, absHash);
            if (pos >= 0) {
                // if pos >= 0, then absHash == thresholds[pos] --> add 1
                return pos + 1;
            } else {
                // when pos < 0, pos = (-(insertion point) - 1)
                return -(pos + 1);
            }
        }
    }

    public static GroupChooser createChooser(final double[] percentages) {
        if (percentages.length == 0) {
            return new OneGroupChooser();
        }
        if (percentages.length == 1) {
            return new TwoGroupChooser((int)(percentages[0]*Integer.MAX_VALUE));
        }

        final ProportionalMultiGroupChooser proportionalGroupChooser =
                ProportionalMultiGroupChooser.tryCreate(percentages, 1e-6, 256);
        if (proportionalGroupChooser != null) {
            return proportionalGroupChooser;
        }

        final int[] groupBounds = new int[percentages.length];
        for (int i = 0; i < groupBounds.length; i++) {
            groupBounds[i] = (int)(percentages[i]*Integer.MAX_VALUE);
        }
        return new MultiGroupChooser(groupBounds);
    }
}