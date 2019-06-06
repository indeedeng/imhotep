package com.indeed.imhotep.utils.tempfiles;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;

public class ImhotepTempFiles extends AbstractTempFiles<ImhotepTempFiles.Type> {
    private static final Logger LOGGER = Logger.getLogger(ImhotepTempFiles.class);
    private static final AtomicReference<ImhotepTempFiles> INSTANCE = new AtomicReference<>(
            builder()
                    .setEventListener(new LoggingEventListener(Level.WARN, LOGGER))
                    .setRoot(Paths.get(System.getProperty("java.io.tmpdir")))
                    .build()
    );

    public static void initialize(final AbstractTempFiles.Builder<Type, ImhotepTempFiles> imhotepTempFiles) {
        INSTANCE.set(imhotepTempFiles.build());
    }

    public static ImhotepTempFiles getInstance() {
        return INSTANCE.get();
    }

    private ImhotepTempFiles(final Builder<Type, ImhotepTempFiles> builder) {
        super(builder);
    }

    public static TempFile create(final Type type, final String sessionId) throws IOException {
        return getInstance().createTempFile(type, sessionId);
    }

    public static TempFile createFTGSTempFile(final String sessionId) throws IOException {
        return getInstance().createTempFile(Type.FTGS, sessionId);
    }

    public static TempFile createFTGATempFile(final String sessionId) throws IOException {
        return getInstance().createTempFile(Type.FTGA, sessionId);
    }

    public static TempFile createFTGSSplitterTempFile() throws IOException {
        return getInstance().createTempFile(Type.FTGS_SPLITTER);
    }

    public static Builder<Type, ImhotepTempFiles> builder() {
        return new Builder<>(Type.class, ImhotepTempFiles::new);
    }

    @AllArgsConstructor
    @Getter
    public enum Type implements TempFileType<Type> {
        FTGS("ftgs"),
        FTGA("ftga"),
        FTGS_SPLITTER("ftgsSplitter"),
        BATCH_GROUP_STATS_ITERATOR("batchGroupStatsIterator"),
        MULTI_FTGS("multiftgs"),
        GROUP_STATS_ITERATOR("groupStatsIterator"),
        ;
        private final String identifier;
    }
}
