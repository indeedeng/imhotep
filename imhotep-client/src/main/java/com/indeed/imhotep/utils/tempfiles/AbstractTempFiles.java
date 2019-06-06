package com.indeed.imhotep.utils.tempfiles;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableList;
import com.indeed.imhotep.utils.StackTraceUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractTempFiles<E extends Enum<E> & TempFileType<E>> {
    private static final Logger LOGGER = Logger.getLogger(AbstractTempFiles.class);

    private final Path root;
    private final E[] namings;
    private final boolean takeStackTrace;
    @Nullable
    private final Cache<Path, TempFile> availableFiles;
    private final EventListener eventListener;

    protected AbstractTempFiles(
            final Builder<E, ?> builder
    ) {
        this.root = Objects.requireNonNull(builder.getRoot());
        this.namings = Objects.requireNonNull(builder.getTypeEnum().getEnumConstants());
        this.takeStackTrace = builder.isTakeStackTrace();
        this.availableFiles = builder.buildCacheStructure();
        this.eventListener = Objects.requireNonNull(builder.getEventListener());
    }

    private TempFile createTempFileImpl(final E type, @Nullable final String extraInfo) throws IOException {
        final String prefixString;
        if (extraInfo == null) {
            prefixString = type.getIdentifier() + ".";
        } else {
            prefixString = type.getIdentifier() + "." + extraInfo + ".";
        }
        final Path path = Files.createTempFile(root, prefixString, ".tmp");
        @Nullable final StackTraceElement[] stackTraceElements;
        if (takeStackTrace) {
            stackTraceElements = StackTraceUtils.tryGetStackTrace();
        } else {
            stackTraceElements = null;
        }
        final TempFile result = new TempFile(type, path, stackTraceElements, eventListener);
        if (availableFiles != null) {
            availableFiles.put(path, result);
        }
        return result;
    }

    public TempFile createTempFile(final E type) throws IOException {
        return createTempFileImpl(type, null);
    }

    public TempFile createTempFile(final E type, final String extraInfo) throws IOException {
        return createTempFileImpl(type, extraInfo);
    }

    @VisibleForTesting
    void tryCleanupExpiredFiles() {
        if (availableFiles != null) {
            availableFiles.cleanUp();
        }
    }

    /**
     * Returns the current list of available files. Returns null if it's not configured to monitor available files.
     */
    @Nullable
    public List<TempFileState> getAllOpenedStates() {
        if (availableFiles == null) {
            return null;
        }
        final ImmutableList.Builder<TempFileState> states = ImmutableList.builder();
        for (final TempFile tempFile : availableFiles.asMap().values()) {
            if (tempFile == null) {
                continue; // It's GCed
            }
            states.add(tempFile.getState());
        }
        return states.build();
    }

    public void tryCleanupTempDirectory() {
        final List<Path> pathsToDelete = new ArrayList<>();
        try (final Stream<Path> files = Files.list(root)) {
            files.forEach(file -> {
                final String fileName = file.getFileName().toString();
                final boolean shouldDelete = Arrays.stream(namings).anyMatch(prefix -> fileName.startsWith(prefix.getIdentifier() + ".") && fileName.endsWith(".tmp"));
                if (shouldDelete) {
                    pathsToDelete.add(file);
                }
            });
        } catch (final IOException e) {
            LOGGER.warn("Unable to list files on " + root, e);
        }

        if (pathsToDelete.isEmpty()) {
            return;
        }
        LOGGER.info("Cleanup temp files: " + pathsToDelete.stream().map(Path::toString).collect(Collectors.joining(", ")));
        for (final Path path : pathsToDelete) {
            try {
                Files.deleteIfExists(path);
            } catch (final IOException e) {
                LOGGER.warn("Unable to delete temp file " + path, e);
            }
        }
    }

    @Accessors(chain = true)
    @Getter(AccessLevel.PRIVATE)
    public static class Builder<T extends Enum<T> & TempFileType<T>, Concrete extends AbstractTempFiles<T>> {
        private final Class<T> typeEnum;
        private final Function<Builder<T, Concrete>, Concrete> constructor;
        @Setter
        private Path root = Paths.get(System.getProperty("java.io.tmpdir"));
        @Setter
        private boolean takeStackTrace = false;
        @Setter
        private EventListener eventListener;
        @Setter
        @Nullable
        private Ticker ticker = null;
        @Setter
        private long expirationMillis = -1;

        public Builder(final Class<T> typeEnum, final Function<Builder<T, Concrete>, Concrete> constructor) {
            this.typeEnum = typeEnum;
            this.eventListener = new LoggingEventListener(Level.WARN, Logger.getLogger(typeEnum));
            this.constructor = constructor;
        }

        @Nullable
        private Cache<Path, TempFile> buildCacheStructure() {
            if (expirationMillis < 0) {
                return null;
            }
            final CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder().weakValues();
            cacheBuilder.expireAfterAccess(expirationMillis, TimeUnit.MILLISECONDS);
            if (ticker != null) {
                cacheBuilder.ticker(ticker);
            }
            if (eventListener != null) {
                final RemovalListener<Path, TempFile> removalListener = notification -> {
                    @Nullable final TempFile tempFile = notification.getValue();
                    switch (notification.getCause()) {
                        case EXPIRED:
                            if (tempFile != null) { // Null means it's already GCed.
                                eventListener.expired(tempFile.getState());
                            }
                            break;
                        case COLLECTED: // Should be handled by finalizer
                        case EXPLICIT: // We don't use any of the following
                        case REPLACED:
                        case SIZE:
                    }
                };
                // It just returns casted self, so ignore that.
                //noinspection ResultOfMethodCallIgnored
                cacheBuilder.removalListener(removalListener);
            }
            return cacheBuilder.build();
        }

        public Concrete build() {
            return constructor.apply(this);
        }
    }
}
