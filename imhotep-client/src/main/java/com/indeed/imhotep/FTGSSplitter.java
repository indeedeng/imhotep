package com.indeed.imhotep;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.indeed.util.core.hash.MurmurHash;
import com.indeed.util.io.ByteBufferDataOutputStream;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.RawFTGSIterator;
import com.indeed.imhotep.io.Octopus;
import com.indeed.imhotep.service.FTGSPipeWriter;
import com.indeed.imhotep.service.TGSBuffer;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
* @author jplaisance
*/
public final class FTGSSplitter implements Runnable, Closeable {
    private static final Logger log = Logger.getLogger(FTGSSplitter.class);

    private final FTGSIterator iterator;

    private final int numSplits;

    private final FTGSPipeWriter[] outputs;
    private final Octopus octopus;
    private final RawFTGSIterator[] ftgsIterators;
    private final TGSBuffer[] tgsBuffers;

    private final AtomicBoolean done = new AtomicBoolean(false);

    private final Thread runThread;

    public FTGSSplitter(FTGSIterator ftgsIterator, final int numSplits, final int numStats) throws IOException {
        this.iterator = ftgsIterator;
        this.numSplits = numSplits;
        outputs = new FTGSPipeWriter[numSplits];
        octopus = new Octopus(numSplits, 65536);
        ftgsIterators = new RawFTGSIterator[numSplits];
        tgsBuffers = new TGSBuffer[numSplits];
        final AtomicInteger doneCounter = new AtomicInteger();
        for (int i = 0; i < numSplits; i++) {
            outputs[i] = new FTGSPipeWriter();
            ftgsIterators[i] = new InputStreamFTGSIterator(octopus.getInputStream(i), numStats) {
                boolean closed = false;

                @Override
                public void close() {
                    if (!closed) {
                        closed = true;
                        super.close();
                        if (doneCounter.incrementAndGet() == numSplits) {
                            FTGSSplitter.this.close();
                        }
                    }
                }
            };
            tgsBuffers[i] = new TGSBuffer(ftgsIterator, numStats);
        }
        runThread = new Thread(this, "FTGSSplitterThread");
        runThread.setDaemon(true);
        runThread.start();
    }

    public RawFTGSIterator[] getFtgsIterators() {
        return ftgsIterators;
    }

    /**
     * here be dragons and cephalopods
     */
    public void run() {
        try {
            final RawFTGSIterator rawIterator;
            if (iterator instanceof RawFTGSIterator) {
                rawIterator = (RawFTGSIterator) iterator;
            } else {
                rawIterator = null;
            }
            final ByteBuffer[] byteBuffers = new ByteBuffer[numSplits];
            final ByteBufferDataOutputStream out = new ByteBufferDataOutputStream(128, false);
            while (iterator.nextField()) {
                final boolean fieldIsIntType = iterator.fieldIsIntType();
                for (int i = 0; i < outputs.length; i++) {
                    final ByteBufferDataOutputStream tmp = new ByteBufferDataOutputStream(128, false);
                    outputs[i].switchField(iterator.fieldName(), fieldIsIntType, tmp);
                    byteBuffers[i] = tmp.getBufferUnsafe();
                    byteBuffers[i].flip();
                    tgsBuffers[i].resetField(fieldIsIntType);
                }
                octopus.broadcast(byteBuffers);

                while (iterator.nextTerm()) {
                    final TGSBuffer tgsBuffer;
                    final int split;
                    if (fieldIsIntType) {
                        final long term = iterator.termIntVal();
                        split = (int)((term & Long.MAX_VALUE) % numSplits);
                        tgsBuffer = tgsBuffers[split];
                    } else {
                        if (rawIterator != null) {
                            split = hashStringTerm(rawIterator);
                        } else {
                            split = hashStringTerm(iterator);
                        }
                        tgsBuffer = tgsBuffers[split];
                    }
                    out.clear();
                    tgsBuffer.resetBufferToCurrentTGS(out);
                    final ByteBuffer primary = out.getBufferUnsafe();
                    primary.flip();
                    if (!octopus.write(primary, split)) {
                        //would block, means we have to write empty term to all iterators
                        for (int i = 0; i < outputs.length; i++) {
                            if (i != split) {
                                final ByteBufferDataOutputStream tmpBuffer = new ByteBufferDataOutputStream(128, false);
                                tgsBuffers[i].resetBufferToCurrentTermOnly(tmpBuffer);
                                byteBuffers[i] = tmpBuffer.getBufferUnsafe();
                                byteBuffers[i].flip();
                            } else {
                                byteBuffers[i] = primary;
                            }
                        }
                        octopus.broadcast(byteBuffers);
                    }
                }
            }
            for (int i = 0; i < outputs.length; i++) {
                final ByteBufferDataOutputStream tmp = new ByteBufferDataOutputStream(128, false);
                outputs[i].close(tmp);
                byteBuffers[i] = tmp.getBufferUnsafe();
                byteBuffers[i].flip();
            }
            octopus.broadcast(byteBuffers);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        } finally {
            close();
        }
    }

    private int hashStringTerm(final RawFTGSIterator rawIterator) {
        return (MurmurHash.hash32(
                rawIterator.termStringBytes(), 0, rawIterator.termStringLength()
        ) & 0x7FFFFFFF) % numSplits;
    }

    private int hashStringTerm(final FTGSIterator iterator) {
        final byte[] bytes = iterator.termStringVal().getBytes(Charsets.UTF_8);
        return (MurmurHash.hash32(bytes) & 0x7FFFFFFF) % numSplits;
    }

    @Override
    public void close() {
        if (done.compareAndSet(false, true)) {
            try {
                try {
                    octopus.closeOutput();
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            } finally {
                try {
                    if (Thread.currentThread() != runThread) {
                        while (true) {
                            try {
                                runThread.interrupt();
                                runThread.join(10);
                                if (!runThread.isAlive()) break;
                            } catch (InterruptedException e) {
                                //ignore
                            }
                        }
                    }
                } finally {
                    iterator.close();
                }
            }
        }
    }
}
