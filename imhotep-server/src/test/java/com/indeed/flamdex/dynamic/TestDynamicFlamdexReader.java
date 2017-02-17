package com.indeed.flamdex.dynamic;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.simple.SimpleFlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * @author michihiko
 */

public class TestDynamicFlamdexReader {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final int NUM_DOCS = (2 * 3 * 5 * 7 * 11) + 10;
    private Path indexDirectory;

    private List<Path> segmentDirectories;

    @Before
    public void setUp() throws IOException {
        indexDirectory = temporaryFolder.getRoot().toPath();
        segmentDirectories = Lists.newArrayList(
                indexDirectory.resolve("segment1"),
                indexDirectory.resolve("segment2"),
                indexDirectory.resolve("segment3"),
                indexDirectory.resolve("segment4") // empty segment
        );
        final Random random = new Random(3);
        final List<FlamdexDocWriter> writers = new ArrayList<>();
        for (final Path segmentDirectory : segmentDirectories) {
            writers.add(new SimpleFlamdexDocWriter(
                    segmentDirectory,
                    new SimpleFlamdexDocWriter.Config()));
        }
        final int[] numDocs = new int[segmentDirectories.size()];
        for (int i = 0; i < NUM_DOCS; ++i) {
            final int segment = random.nextInt(writers.size() - 1);
            ++numDocs[segment];
            writers.get(segment).addDocument(DynamicFlamdexTestUtils.makeDocument(i));
        }
        for (final FlamdexDocWriter writer : writers) {
            writer.close();
        }
    }

    @Test
    public void testSimpleStats() throws IOException, FlamdexOutOfMemoryException {
        try (final FlamdexReader flamdexReader = new DynamicFlamdexReader(indexDirectory)) {
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
                assertEquals(term, DynamicFlamdexTestUtils.restoreOriginalNumber(flamdexReader, buf[0]));
            }
            assertEquals(NUM_DOCS, id);
        }
    }

    @Test
    public void testIntTermDocIterator() throws IOException, FlamdexOutOfMemoryException {
        try (final FlamdexReader flamdexReader = new DynamicFlamdexReader(indexDirectory)) {
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
                            final int num = DynamicFlamdexTestUtils.restoreOriginalNumber(flamdexReader, docIds[i]);
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
        try (final FlamdexReader flamdexReader = new DynamicFlamdexReader(indexDirectory)) {
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
                            final int num = DynamicFlamdexTestUtils.restoreOriginalNumber(flamdexReader, docIds[i]);
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
    public void testTombstoneSet() throws IOException, FlamdexOutOfMemoryException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        for (final Path segmentDirectory : segmentDirectories) {
            try (final Closer closer = Closer.create()) {
                final Path tombstoneSetPath = segmentDirectory.resolve("tombstoneSet.bin");
                try (final SegmentReader segmentReader = new SegmentReader(segmentDirectory)) {
                    final int numDoc = segmentReader.maxNumDocs();
                    final FastBitSet tombstoneSet = new FastBitSet(numDoc);
                    for (int docId = 0; docId < numDoc; ++docId) {
                        final int n = DynamicFlamdexTestUtils.restoreOriginalNumber(segmentReader, docId);
                        if ((n % 5) == 4) {
                            tombstoneSet.set(docId, true);
                        }
                    }
                    try (final ByteChannel byteChannel = Files.newByteChannel(tombstoneSetPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
                        byteChannel.write(tombstoneSet.serialize());
                    }
                }
            }
        }

        try (final FlamdexReader flamdexReader = new DynamicFlamdexReader(indexDirectory)) {
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
                            final int num = DynamicFlamdexTestUtils.restoreOriginalNumber(flamdexReader, docIds[i]);
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
