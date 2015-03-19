package com.indeed.flamdex.simple;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.multicache.ftgs.TermDesc;
import com.indeed.util.core.io.Closeables2;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

public class MultiShardStringTermIteratorTest {
    private static final Logger log = Logger.getLogger(MultiShardStringTermIteratorTest.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    @Test
    public void testMultiShardStringTermIterator() throws IOException {
        BasicConfigurator.configure();
        final List<File> shardDirectories = Lists.newArrayList();
        final TreeMap<String, Long> termToPosition1;
        {
            final MemoryFlamdex memoryFlamdex = new MemoryFlamdex();
            memoryFlamdex.addDocument(getDocument(ImmutableMultimap.<String, Object>of("int", 1, "int", 2, "foo", "bar", "foo", "bar1")));
            memoryFlamdex.addDocument(getDocument(ImmutableMultimap.<String, Object>of("foo", "more than bar", "foo", "bar1", "foo", "board")));
            memoryFlamdex.addDocument(getDocument(ImmutableMultimap.<String, Object>of("foo", "bar1")));
            memoryFlamdex.addDocument(getDocument(ImmutableMultimap.<String, Object>of("foo", "bar1")));
            memoryFlamdex.addDocument(getDocument(ImmutableMultimap.<String, Object>of("zoo", "car")));
            final File flamdexDir = write(memoryFlamdex);
            shardDirectories.add(flamdexDir);
            termToPosition1 = readTermOffsetsForField(flamdexDir, "foo");
        }

        final TreeMap<String, Long> termToPosition2;
        {
            final MemoryFlamdex memoryFlamdex = new MemoryFlamdex();
            memoryFlamdex.addDocument(getDocument(ImmutableMultimap.<String, Object>of("goo", "do", "blah", "random", "foo", "tar", "foo", "far")));
            memoryFlamdex.addDocument(getDocument(ImmutableMultimap.<String, Object>of("no", "yes", "random", "arbitrary", "foo", "bar1")));
            final File flamdexDir = write(memoryFlamdex);
            shardDirectories.add(flamdexDir);
            termToPosition2 = readTermOffsetsForField(flamdexDir, "foo");
        }

        final TreeMap<String, Long> termToPosition3;
        {
            final MemoryFlamdex memoryFlamdex = new MemoryFlamdex();
            memoryFlamdex.addDocument(getDocument(ImmutableMultimap.<String, Object>of("car", "dar", "war", "far", "foo", "bar", "int", 3)));
            final File flamdexDir = write(memoryFlamdex);
            shardDirectories.add(flamdexDir);
            termToPosition3 = readTermOffsetsForField(flamdexDir, "foo");
        }

        final HashMap<String, LongList> mergedResults = merge(ImmutableList.of(termToPosition1, termToPosition2, termToPosition3));
        final List<SimpleFlamdexReader> simpleFlamdexReaders = new ArrayList<>();
        try {
            for (final File shardDirectory : shardDirectories) {
                simpleFlamdexReaders.add(SimpleFlamdexReader.open(shardDirectory.getCanonicalPath()));
            }
            final long[] positionsBuffer = new long[3];
            final MultiShardFlamdexReader multiShardFlamdexReader = new MultiShardFlamdexReader(simpleFlamdexReaders.toArray(new SimpleFlamdexReader[simpleFlamdexReaders.size()]));
            final Iterator<TermDesc> smsstoi =  multiShardFlamdexReader.stringTermOffsetIterator("foo");
            while (smsstoi.hasNext()) {
                TermDesc desc = smsstoi.next();
                if (Arrays.equals("bar".getBytes(Charsets.UTF_8), Arrays.copyOf(desc.stringTerm, desc.stringTermLen))) {
                    System.arraycopy(desc.offsets, 0, positionsBuffer, 0, desc.offsets.length);
                    final LongList expectedPositions = mergedResults.get("bar");
                    final long[] expectedPositionsArray = expectedPositions.toArray(new long[expectedPositions.size()]);
                    if (log.isDebugEnabled()) {
                        log.debug("expected: "+ Arrays.toString(expectedPositionsArray));
                        log.debug("actual: "+ Arrays.toString(positionsBuffer));
                    }
                    Assert.assertArrayEquals(expectedPositionsArray, positionsBuffer);
                }
            }
        } finally {
            for (final SimpleFlamdexReader simpleFlamdexReader : simpleFlamdexReaders) {
                Closeables2.closeQuietly(simpleFlamdexReader, log);
            }
        }
    }

    private FlamdexDocument getDocument(Multimap<String, Object> fieldToTerm) {
        final FlamdexDocument flamdexDocument = new FlamdexDocument();
        for (final String field : fieldToTerm.keySet()) {
            final Collection<Object> values = fieldToTerm.get(field);
            for (final Object value : values) {
                //noinspection ChainOfInstanceofChecks
                if (value instanceof Integer ) {
                    flamdexDocument.addIntTerm(field, (Integer) value);
                } else if (value instanceof String) {
                    flamdexDocument.addStringTerm(field, value.toString());
                } else {
                    throw new RuntimeException("unhandled type !"+value.getClass().getCanonicalName());
                }
            }
        }
        return flamdexDocument;
    }


    private File write(final MemoryFlamdex memoryFlamdex) throws IOException {
        final File directory = folder.newFolder(String.valueOf(System.currentTimeMillis()));
        final SimpleFlamdexWriter simpleFlamdexWriter = new SimpleFlamdexWriter(directory.getCanonicalPath(), memoryFlamdex.getNumDocs());
        SimpleFlamdexWriter.writeFlamdex(memoryFlamdex, simpleFlamdexWriter);
        return directory;
    }

    private TreeMap<String, Long> readTermOffsetsForField(final File dir, final String field) throws IOException {
        try (final SimpleFlamdexReader simpleFlamdexReader = SimpleFlamdexReader.open(dir.getCanonicalPath());
             final SimpleStringTermIterator simpleStringTermIterator = simpleFlamdexReader.getStringTermIterator(field)) {
            //noinspection NestedTryStatement
            final TreeMap<String, Long> termToPosition = new TreeMap<>();
            while (simpleStringTermIterator.next()) {
                termToPosition.put(simpleStringTermIterator.term(), simpleStringTermIterator.getOffset());
            }
            return termToPosition;
        }
    }

    private HashMap<String, LongList> merge(List<TreeMap<String, Long>> termToPositions) {
        final HashMap<String, LongList> ret = new HashMap<>();
        final Set<String> allTerms = new HashSet<>();
        for (final TreeMap<String, Long> termToPosition : termToPositions) {
            allTerms.addAll(termToPosition.keySet());
        }
        for (final String term : allTerms) {
            final LongList shardToPosition = new LongArrayList(termToPositions.size());
            for (final TreeMap<String, Long> termToPosition : termToPositions) {
                final long position;
                if (termToPosition.containsKey(term)) {
                    position = termToPosition.get(term);
                } else {
                    position = 0;
                }
                shardToPosition.add(position);
            }
            ret.put(term, shardToPosition);
        }
        return ret;
    }

}