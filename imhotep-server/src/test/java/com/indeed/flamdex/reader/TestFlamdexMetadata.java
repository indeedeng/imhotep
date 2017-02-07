package com.indeed.flamdex.reader;

import com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

public class TestFlamdexMetadata {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testReadWrite() throws Exception {
        final FlamdexMetadata metadata = new FlamdexMetadata(31, ImmutableList.of("int1", "int2"), ImmutableList.of("string1", "string2"), FlamdexFormatVersion.SIMPLE);

        final Path tempDir = folder.getRoot().toPath();
        FlamdexMetadata.writeMetadata(tempDir, metadata);
        final FlamdexMetadata metadata1 = FlamdexMetadata.readMetadata(tempDir);
        assertEquals(metadata.getFormatVersion(), metadata1.getFormatVersion());
        assertEquals(metadata.getFlamdexFormatVersion(), metadata1.getFlamdexFormatVersion());
        assertEquals(metadata.getIntFields(), metadata1.getIntFields());
        assertEquals(metadata.getStringFields(), metadata1.getStringFields());
        assertEquals(metadata.getNumDocs(), metadata1.getNumDocs());
    }
}
