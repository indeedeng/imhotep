package com.indeed.flamdex.reader;

import com.google.common.primitives.Longs;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexDocWriter;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.flamdex.writer.FlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.io.TestFileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class GenericFlamdexReaderTest {
    private Path tempIndexDir;

    @Before
    public void setUp() throws IOException {
        tempIndexDir = Files.createTempDirectory("flamdex-test");
    }

    @After
    public void tearDown() throws IOException {
        TestFileUtils.deleteDirTree(tempIndexDir);
    }

    @Test
    public void testSimpleFlamdexReaderWithoutMetadata() throws IOException {
        final SimpleFlamdexDocWriter.Config config = new SimpleFlamdexDocWriter.Config().setDocBufferSize(999999999).setMergeFactor(999999999);
        writeFlamdex(tempIndexDir, config);

        final Path metadataPath = tempIndexDir.resolve("metadata.txt");
        assertTrue(metadataPath.toFile().delete());

        try (final FlamdexReader reader = GenericFlamdexReader.open(tempIndexDir, 4)) {
            assertTrue(reader instanceof SimpleFlamdexReader);
        }
    }

    private void writeFlamdex(final Path dir, final SimpleFlamdexDocWriter.Config config) throws IOException {
        try (final FlamdexDocWriter w = new SimpleFlamdexDocWriter(dir, config)) {

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
        }
    }
}