package com.indeed.flamdex.simple;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.indeed.util.io.BufferedFileDataInputStream;
import com.indeed.util.io.BufferedFileDataOutputStream;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.writer.FlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.flamdex.writer.FlamdexWriter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jsgroth
 */
public final class SimpleFlamdexDocWriter implements FlamdexDocWriter {
    private final String outputDirectory;
    private final int docBufferSize;
    private final int mergeFactor;

    private final List<List<File>> segmentsOnDisk;

    private MemoryFlamdex currentBuffer = new MemoryFlamdex();
    private String currentSegment = "_0";

    public SimpleFlamdexDocWriter(String outputDirectory, Config config) throws IOException {
        createOutputDir(outputDirectory);

        this.outputDirectory = outputDirectory;
        this.docBufferSize = config.getDocBufferSize();
        this.mergeFactor = config.getMergeFactor();

        segmentsOnDisk = Lists.newArrayList();
        segmentsOnDisk.add(new ArrayList<File>());
    }

    private static void createOutputDir(String outputDirectory) throws IOException {
        final File f = new File(outputDirectory);
        if (f.exists() && !f.isDirectory()) {
            throw new FileNotFoundException(f + " is not a directory");
        }
        if (!f.exists() && !f.mkdirs()) {
            throw new IOException("unable to create directory " + f);
        }
    }

    @Override
    public void addDocument(FlamdexDocument doc) throws IOException {
        currentBuffer.addDocument(doc);
        if (currentBuffer.getNumDocs() == docBufferSize) {
            flush();
            currentBuffer = new MemoryFlamdex();
        }
    }

    private void flush() throws IOException {
        if (currentBuffer.getNumDocs() == 0) return;

        final File outFile = new File(outputDirectory, currentSegment);
        final BufferedFileDataOutputStream out = new BufferedFileDataOutputStream(outFile, ByteOrder.nativeOrder(), 65536);
        currentBuffer.write(out);
        out.close();

        segmentsOnDisk.get(0).add(outFile);

        currentSegment = nextSegmentDirectory(currentSegment);

        int i = 0;
        while (segmentsOnDisk.get(i).size() == mergeFactor) {
            final List<File> segments = segmentsOnDisk.get(i);
            final List<FlamdexReader> readers = Lists.newArrayListWithCapacity(segments.size());
            long numDocs = 0;
            for (final File segment : segments) {
                final FlamdexReader reader;
                if (i == 0) {
                    reader = MemoryFlamdex.streamer(new BufferedFileDataInputStream(segment, ByteOrder.nativeOrder(), 65536));
                } else {
                    reader = SimpleFlamdexReader.open(segment.getAbsolutePath(), new SimpleFlamdexReader.Config().setWriteBTreesIfNotExisting(false));
                }
                readers.add(reader);
                numDocs += reader.getNumDocs();
            }

            final File mergeDir = new File(outputDirectory, currentSegment);
            currentSegment = nextSegmentDirectory(currentSegment);
            final FlamdexWriter w = new SimpleFlamdexWriter(mergeDir.getAbsolutePath(), numDocs, true, false);
            SimpleFlamdexWriter.merge(readers, w);
            w.close();

            for (final FlamdexReader reader : readers) {
                reader.close();
            }

            for (final File segment : segments) {
                rmrf(segment);
            }
            segments.clear();

            if (i == segmentsOnDisk.size() - 1) {
                segmentsOnDisk.add(new ArrayList<File>());
            }
            segmentsOnDisk.get(i + 1).add(mergeDir);

            ++i;
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        long numDocs = 0;
        final List<FlamdexReader> allReaders = Lists.newArrayList();
        for (final File file : Iterables.concat(Lists.reverse(segmentsOnDisk.subList(1, segmentsOnDisk.size())))) {
            final SimpleFlamdexReader reader = SimpleFlamdexReader.open(file.getAbsolutePath(), new SimpleFlamdexReader.Config().setWriteBTreesIfNotExisting(false));
            allReaders.add(reader);
            numDocs += reader.getNumDocs();
        }
        for (final File file : segmentsOnDisk.get(0)) {
            final FlamdexReader reader = MemoryFlamdex.streamer(new BufferedFileDataInputStream(file, ByteOrder.nativeOrder(), 65536));
            allReaders.add(reader);
            numDocs += reader.getNumDocs();
        }

        final FlamdexWriter w = new SimpleFlamdexWriter(outputDirectory, numDocs, true, true);
        SimpleFlamdexWriter.merge(allReaders, w);
        w.close();

        for (final FlamdexReader reader : allReaders) {
            reader.close();
        }

        for (final File file : Iterables.concat(segmentsOnDisk)) {
            rmrf(file);
        }
    }

    private static String nextSegmentDirectory(String s) {
        int i = s.length() - 1;
        while (s.charAt(i) == 'z') {
            --i;
        }

        final StringBuilder sb = new StringBuilder(i == 0 ? s.length() + 1 : s.length());
        sb.append(s.substring(0, i));
        if (i == 0) {
            sb.append("_0");
        } else {
            sb.append(s.charAt(i) == '9' ? 'a' : (char)(s.charAt(i) + 1));
        }
        for (int j = i + 1; j < s.length(); ++j) {
            sb.append('0');
        }
        return sb.toString();
    }

    public static class Config {
        private int docBufferSize = 500;
        private int mergeFactor = 100;

        public int getDocBufferSize() {
            return docBufferSize;
        }

        public int getMergeFactor() {
            return mergeFactor;
        }

        public Config setDocBufferSize(int docBufferSize) {
            this.docBufferSize = docBufferSize;
            return this;
        }

        public Config setMergeFactor(int mergeFactor) {
            this.mergeFactor = mergeFactor;
            return this;
        }
    }

    private static void rmrf(final File file) throws IOException {
        final Process rmrf = Runtime.getRuntime().exec(new String[]{"rm", "-rf", file.getAbsolutePath()});
        try {
            final int exit = rmrf.waitFor();
            if (exit != 0) {
                throw new IOException ("rm -rf " + file + " failed with exit code " + exit);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
