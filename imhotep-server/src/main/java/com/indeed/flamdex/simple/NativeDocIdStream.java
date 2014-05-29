package com.indeed.flamdex.simple;

import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.TermIterator;
import com.indeed.util.mmap.DirectMemory;
import com.indeed.util.mmap.MMapBuffer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author jplaisance
 */
public final class NativeDocIdStream implements DocIdStream {

    private static final Logger log = Logger.getLogger(NativeDocIdStream.class);

    private DirectMemory memory;
    private SharedReference<MMapBuffer> file;

    private final NativeDocIdBuffer buffer = new NativeDocIdBuffer();

    private String currentFileOpen;

    private boolean closed = false;

    private int lastDoc = 0;

    private final MapCache mapCache;

    NativeDocIdStream(MapCache mapCache) {
        this.mapCache = mapCache;
    }

    @Override
    public void reset(TermIterator term) {
        if (!(term instanceof SimpleTermIterator)) throw new IllegalArgumentException("invalid term iterator");

        try {
            internalReset((SimpleTermIterator)term);
        } catch (IOException e) {
            close();
            throw new RuntimeException(e);
        }
    }

    private void internalReset(SimpleTermIterator term) throws IOException {
        final String filename = term.getFilename();
        if (!filename.equals(currentFileOpen)) {

            if (file != null) file.close();
            file = mapCache.copyOrOpen(filename);

            memory = file.get().memory();
            currentFileOpen = filename;
        }
        buffer.reset(memory.getAddress()+term.getOffset(), term.docFreq());
        lastDoc = 0;
    }

    @Override
    public int fillDocIdBuffer(int[] docIdBuffer) {
        final int n = buffer.fillDocIdBuffer(docIdBuffer, docIdBuffer.length);
        for (int i = 0; i < n; i++) {
            lastDoc += docIdBuffer[i];
            docIdBuffer[i] = lastDoc;
        }
        return n;

    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            Closeables2.closeQuietly(buffer, log);
            Closeables2.closeQuietly(file, log);
        }
    }
}
