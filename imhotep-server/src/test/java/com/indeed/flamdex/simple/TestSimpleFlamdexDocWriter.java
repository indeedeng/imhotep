/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.flamdex.simple;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.ParameterizedUtils;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.utils.FlamdexReinverter;
import com.indeed.flamdex.writer.FlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.io.TestFileUtils;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.varia.LevelRangeFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author jsgroth
 */
@RunWith(Parameterized.class)
public class TestSimpleFlamdexDocWriter {
    private Path tempDir;

    private final SimpleFlamdexReader.Config config;

    @Parameterized.Parameters
    public static Iterable<SimpleFlamdexReader.Config[]> configs() {
        return ParameterizedUtils.getFlamdexConfigs();
    }

    public TestSimpleFlamdexDocWriter(final SimpleFlamdexReader.Config config) {
        this.config = config;
    }

    @BeforeClass
    public static void initLog4j() {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure();

        final Layout LAYOUT = new PatternLayout("[ %d{ISO8601} %-5p ] [%c{1}] %m%n");

        final LevelRangeFilter ERROR_FILTER = new LevelRangeFilter();
        ERROR_FILTER.setLevelMin(Level.ERROR);
        ERROR_FILTER.setLevelMax(Level.FATAL);

        // everything including ERROR
        final Appender STDOUT = new ConsoleAppender(LAYOUT, ConsoleAppender.SYSTEM_OUT);

        // just things <= ERROR
        final Appender STDERR = new ConsoleAppender(LAYOUT, ConsoleAppender.SYSTEM_ERR);
        STDERR.addFilter(ERROR_FILTER);

        final Logger ROOT_LOGGER = Logger.getRootLogger();

        ROOT_LOGGER.removeAllAppenders();

        ROOT_LOGGER.setLevel(Level.WARN); // don't care about higher

        ROOT_LOGGER.addAppender(STDOUT);
        ROOT_LOGGER.addAppender(STDERR);
    }

    @Before
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("flamdex-test");
    }

    @After
    public void tearDown() throws IOException {
        TestFileUtils.deleteDirTree(tempDir);
    }

    @Test
    public void bigRandomTest() throws IOException {
        runRandomTest(150);
        runRandomTest(10);
        runRandomTest(3);
    }

    private void runRandomTest(final int mergeFactor) throws IOException {
        long elapsed = -System.currentTimeMillis();
        final FlamdexDocWriter w = new SimpleFlamdexDocWriter(tempDir, new SimpleFlamdexDocWriter.Config().setDocBufferSize(100).setMergeFactor(mergeFactor));

        final Random rand = new Random();
        final int numDocs = rand.nextInt(20000) + 20000;
        final List<FlamdexDocument> expected = Lists.newArrayList();
        for (int i = 0; i < numDocs; ++i) {
            final FlamdexDocument doc = new FlamdexDocument();
            final int nif = rand.nextInt(5) + 5;
            for (int j = 0; j < nif; ++j) {
                final int nt = rand.nextInt(5) + 5;
                for (int k = 0; k < nt; ++k) {
                    doc.addIntTerm("if" + j, rand.nextInt() & Integer.MAX_VALUE);
                }
            }
            final int nsf = rand.nextInt(5) + 5;
            for (int j = 0; j < nsf; ++j) {
                final int nt = rand.nextInt(3) + 1;
                for (int k = 0; k < nt; ++k) {
                    final int nc = rand.nextInt(20) + 1;
                    final StringBuilder sb = new StringBuilder(nc);
                    for (int l = 0; l < nc; ++l) {
                        sb.append((char)(rand.nextInt('z' - 'a') + 'a'));
                    }
                    doc.addStringTerm("sf" + j, sb.toString());
                }
            }
            w.addDocument(doc);
            expected.add(doc);
        }

        w.close();

        final long size = FileUtils.sizeOfDirectory(tempDir.toFile());
        elapsed += System.currentTimeMillis();
        System.out.println("time for writing " + size + " byte index with " + numDocs + " documents: " + elapsed + " ms");

        final SimpleFlamdexReader r = SimpleFlamdexReader.open(tempDir, config);
        final List<FlamdexDocument> actual = FlamdexReinverter.reinvertInMemory(r);
        r.close();

        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); ++i) {
            final FlamdexDocument ed = expected.get(i);
            final FlamdexDocument ad = actual.get(i);
            assertTrue(unorderedEquals(ed.getIntFields(), ad.getIntFields()));
            assertTrue(unorderedEquals(ed.getStringFields(), ad.getStringFields()));
        }
    }

    private static <T> boolean unorderedEquals(
            final Map<String, ? extends List<T>> o1,
            final Map<String, ? extends List<T>> o2) {
        if (!o1.keySet().equals(o2.keySet())) {
            return false;
        }

        for (final String s : o1.keySet()) {
            final Set<T> s1 = Sets.newHashSet(o1.get(s));
            final Set<T> s2 = Sets.newHashSet(o2.get(s));
            if (!s1.equals(s2)) {
                return false;
            }
        }

        return true;
    }

    @Test
    public void testBufferedOnly() throws IOException {
        final SimpleFlamdexDocWriter.Config config = new SimpleFlamdexDocWriter.Config().setDocBufferSize(999999999).setMergeFactor(999999999);
        writeFlamdex(tempDir, config);
        final FlamdexReader r = SimpleFlamdexReader.open(tempDir, this.config);
        assertEquals(2, r.getIntFields().size());
        assertTrue(r.getIntFields().contains("if1"));
        assertTrue(r.getIntFields().contains("if2"));
        assertEquals(2, r.getStringFields().size());
        assertTrue(r.getStringFields().contains("sf1"));
        assertTrue(r.getStringFields().contains("sf2"));

        final DocIdStream dis = r.getDocIdStream();
        final int[] docIdBuffer = new int[64];
        final IntTermIterator iter = r.getIntTermIterator("if1");
        assertTrue(iter.next());
        assertEquals(0, iter.term());
        assertEquals(2, iter.docFreq());
        dis.reset(iter);
        assertEquals(2, dis.fillDocIdBuffer(docIdBuffer));
        assertEquals(Ints.asList(0, 3), Ints.asList(docIdBuffer).subList(0, 2));
        assertTrue(iter.next());
        assertEquals(5, iter.term());
        assertEquals(1, iter.docFreq());
        dis.reset(iter);
        assertEquals(1, dis.fillDocIdBuffer(docIdBuffer));
        assertEquals(Ints.asList(0), Ints.asList(docIdBuffer).subList(0, 1));
        iter.close();
        dis.close();
        r.close();
    }

    @Test
    public void testEmpty() throws IOException {
        new SimpleFlamdexDocWriter(tempDir, new SimpleFlamdexDocWriter.Config()).close();
        final FlamdexReader r = SimpleFlamdexReader.open(tempDir, config);
        assertEquals(0, r.getNumDocs());
        r.close();
    }

    private void writeFlamdex(final Path dir, final SimpleFlamdexDocWriter.Config config) throws IOException {
        final FlamdexDocWriter w = new SimpleFlamdexDocWriter(dir, config);

        final FlamdexDocument doc0 = new FlamdexDocument();
        doc0.setIntField("if1", Longs.asList(0, 5, 99));
        doc0.setIntField("if2", Longs.asList(3, 7));
        doc0.setStringField("sf1", Arrays.asList("a", "b", "c"));
        doc0.setStringField("sf2", Arrays.asList("0", "-234", "bob"));
        w.addDocument(doc0);

        final FlamdexDocument doc1 = new FlamdexDocument();
        doc1.setIntField("if2", Longs.asList(6, 7, 99));
        doc1.setStringField("sf1", Arrays.asList("b", "d", "f"));
        doc1.setStringField("sf2", Arrays.asList("a", "b", "bob"));
        w.addDocument(doc1);

        final FlamdexDocument doc2 = new FlamdexDocument();
        doc2.setStringField("sf1", Arrays.asList("", "a", "aa"));
        w.addDocument(doc2);

        final FlamdexDocument doc3 = new FlamdexDocument();
        doc3.setIntField("if1", Longs.asList(0, 10000));
        doc3.setIntField("if2", Longs.asList(9));
        w.addDocument(doc3);
        w.close();
    }
}
