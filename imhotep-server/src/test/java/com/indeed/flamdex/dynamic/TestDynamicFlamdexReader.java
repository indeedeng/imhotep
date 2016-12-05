package com.indeed.flamdex.dynamic;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.simple.SimpleFlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocument;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author michihiko
 */
public class TestDynamicFlamdexReader {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final int NUM_DOCS = (2 * 3 * 5 * 7 * 11) + 10;
    private Path directory;

    private int restoreNum(@Nonnull final FlamdexReader flamdexReader, final int docId) throws FlamdexOutOfMemoryException {
        final long[] num = new long[1];
        flamdexReader.getMetric("original").lookup(new int[]{docId}, num, 1);
        assertTrue((Integer.MIN_VALUE <= num[0]) && (num[0] <= Integer.MAX_VALUE));
        return (int) num[0];
    }

    @Nonnull
    private FlamdexDocument makeDocument(final int n) {
        final FlamdexDocument.Builder builder = new FlamdexDocument.Builder();
        builder.addIntTerm("original", n);
        builder.addIntTerm("mod2i", n % 2);
        builder.addIntTerm("mod3i", n % 3);
        builder.addStringTerm("mod5s", Integer.toString(n % 5));
        builder.addIntTerms("mod7mod11i", n % 7, n % 11);
        builder.addStringTerms("mod7mod11s", Integer.toString(n % 7), Integer.toString(n % 11));
        if (((n % 3) != 0) && ((n % 5) != 0)) {
            builder.addIntTerms("mod3mod5i_nonzero", n % 3, n % 5);
        }
        return builder.build();
    }

    private List<SegmentInfo> segmentInfos;

    @Before
    public void setUp() throws IOException {
        directory = temporaryFolder.getRoot().toPath();
        segmentInfos = ImmutableList.of(
                new SegmentInfo(directory, "segment1", Optional.<String>absent()),
                new SegmentInfo(directory, "segment2", Optional.<String>absent()),
                new SegmentInfo(directory, "segment3", Optional.<String>absent()));
        DynamicFlamdexMetadataUtil.modifyMetadata(directory, new Function<List<SegmentInfo>, List<SegmentInfo>>() {
            @Nullable
            @Override
            public List<SegmentInfo> apply(@Nullable final List<SegmentInfo> empty) {
                return segmentInfos;
            }
        });
        final Random random = new Random(3);
        final List<FlamdexDocWriter> writers = new ArrayList<>();
        for (final SegmentInfo segmentInfo : segmentInfos) {
            writers.add(new SimpleFlamdexDocWriter(
                    segmentInfo.getDirectory(),
                    new SimpleFlamdexDocWriter.Config()));
        }
        for (int i = 0; i < NUM_DOCS; ++i) {
            writers.get(random.nextInt(writers.size())).addDocument(makeDocument(i));
        }
        for (final FlamdexDocWriter writer : writers) {
            writer.close();
        }
    }

    @Test
    public void testSimpleStats() throws IOException, FlamdexOutOfMemoryException {
        try (final FlamdexReader flamdexReader = new DynamicFlamdexReader(directory)) {
            assertEquals(NUM_DOCS, flamdexReader.getNumDocs());
            assertEquals(NUM_DOCS, flamdexReader.getIntTotalDocFreq("original"));
            int numMod7Mod11i = 0;
            for (int i = 0; i < NUM_DOCS; ++i) {
                numMod7Mod11i += ((i % 7) == (i % 11)) ? 1 : 2;
            }
            assertEquals(numMod7Mod11i, flamdexReader.getIntTotalDocFreq("mod7mod11i"));
            assertEquals(
                    ImmutableSet.of("original", "mod2i", "mod3i", "mod7mod11i", "mod3mod5i_nonzero"),
                    ImmutableSet.copyOf(flamdexReader.getAvailableMetrics()));
            assertEquals(
                    ImmutableSet.of("original", "mod2i", "mod3i", "mod7mod11i", "mod3mod5i_nonzero"),
                    ImmutableSet.copyOf(flamdexReader.getIntFields()));
            assertEquals(
                    ImmutableSet.of("mod5s", "mod7mod11s"),
                    ImmutableSet.copyOf(flamdexReader.getStringFields()));

            final IntTermDocIterator iterator = flamdexReader.getIntTermDocIterator("original");
            int id = 0;
            for (; iterator.nextTerm(); ++id) {
                final long term = iterator.term();
                assertEquals(id, term);
                final int[] buf = new int[10];
                final int numDocs = iterator.nextDocs(buf);
                assertEquals(1, numDocs);
                assertEquals(term, restoreNum(flamdexReader, buf[0]));
            }
            assertEquals(NUM_DOCS, id);
        }
    }

    @Test
    public void testIntTermDocIterator() throws IOException, FlamdexOutOfMemoryException {
        try (final FlamdexReader flamdexReader = new DynamicFlamdexReader(directory)) {
            final int[] okBit = new int[flamdexReader.getNumDocs()];
            {
                final IntTermDocIterator intTermDocIterator = flamdexReader.getIntTermDocIterator("mod7mod11i");
                while (intTermDocIterator.nextTerm()) {
                    final long term = intTermDocIterator.term();
                    final int[] docIds = new int[100];
                    while (true) {
                        final int numFilled = intTermDocIterator.fillDocIdBuffer(docIds);
                        if (numFilled == 0) {
                            break;
                        }
                        for (int i = 0; i < numFilled; ++i) {
                            final int num = restoreNum(flamdexReader, docIds[i]);
                            if ((num % 7) == term) {
                                okBit[docIds[i]] |= 1;
                            }
                            if ((num % 11) == term) {
                                okBit[docIds[i]] |= 2;
                            }
                        }
                    }
                }
            }
            for (final int i : okBit) {
                assertEquals(3, i);
            }
        }
    }

    @Test
    public void testStringTermDocIterator() throws IOException, FlamdexOutOfMemoryException {
        try (final FlamdexReader flamdexReader = new DynamicFlamdexReader(directory)) {
            final int[] okBit = new int[flamdexReader.getNumDocs()];
            {
                final StringTermDocIterator stringTermDocIterator = flamdexReader.getStringTermDocIterator("mod7mod11s");
                while (stringTermDocIterator.nextTerm()) {
                    final long term = Long.parseLong(stringTermDocIterator.term());
                    final int[] docIds = new int[100];
                    while (true) {
                        final int numFilled = stringTermDocIterator.fillDocIdBuffer(docIds);
                        if (numFilled == 0) {
                            break;
                        }
                        for (int i = 0; i < numFilled; ++i) {
                            final int num = restoreNum(flamdexReader, docIds[i]);
                            if ((num % 7) == term) {
                                okBit[docIds[i]] |= 1;
                            }
                            if ((num % 11) == term) {
                                okBit[docIds[i]] |= 2;
                            }
                        }
                    }
                }
            }
            for (final int i : okBit) {
                assertEquals(3, i);
            }
        }
    }

    @Test
    public void testTombstone() throws IOException, FlamdexOutOfMemoryException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        for (final SegmentInfo segmentInfo : segmentInfos) {
            try (final Closer closer = Closer.create()) {
                final Path segmentPath = segmentInfo.getDirectory();
                final Path tombstonePath = segmentPath.resolve("tombstone");
                try (final SegmentReader segmentReader = new SegmentReader(segmentInfo)) {
                    final int numDoc = segmentReader.maxNumDocs();
                    final FastBitSet tombstone = new FastBitSet(numDoc);
                    for (int docId = 0; docId < numDoc; ++docId) {
                        final int n = restoreNum(segmentReader, docId);
                        if ((n % 5) == 4) {
                            tombstone.set(docId, true);
                        }
                    }
                    try (final ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(tombstonePath.toFile()))) {
                        oos.writeObject(tombstone);
                    }
                }
            }
        }
        DynamicFlamdexMetadataUtil.modifyMetadata(directory, new Function<List<SegmentInfo>, List<SegmentInfo>>() {
            @Nullable
            @Override
            public List<SegmentInfo> apply(@Nullable final List<SegmentInfo> segmentInfo) {
                final List<SegmentInfo> newSegmentInfo = new ArrayList<>();
                for (final SegmentInfo data : Preconditions.checkNotNull(segmentInfo)) {
                    newSegmentInfo.add(new SegmentInfo(data.getShardDirectory(), data.getName(), Optional.of("tombstone")));
                }
                return newSegmentInfo;
            }
        });

        try (final FlamdexReader flamdexReader = new DynamicFlamdexReader(directory)) {
            final int[] okBit = new int[flamdexReader.getNumDocs()];
            {
                final IntTermDocIterator intTermDocIterator = flamdexReader.getIntTermDocIterator("mod7mod11i");
                while (intTermDocIterator.nextTerm()) {
                    final long term = intTermDocIterator.term();
                    final int[] docIds = new int[100];
                    while (true) {
                        final int numFilled = intTermDocIterator.fillDocIdBuffer(docIds);
                        if (numFilled == 0) {
                            break;
                        }
                        for (int i = 0; i < numFilled; ++i) {
                            final int num = restoreNum(flamdexReader, docIds[i]);
                            if ((num % 7) == term) {
                                okBit[num] |= 1;
                            }
                            if ((num % 11) == term) {
                                okBit[num] |= 2;
                            }
                        }
                    }
                }
            }
            for (int i = 0; i < okBit.length; ++i) {
                if ((i % 5) == 4) {
                    assertEquals(0, okBit[i]);
                } else {
                    assertEquals(3, okBit[i]);
                }
            }
        }
    }
}