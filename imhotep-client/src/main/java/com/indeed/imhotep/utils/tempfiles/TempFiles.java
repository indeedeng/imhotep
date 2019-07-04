package com.indeed.imhotep.utils.tempfiles;

import org.apache.log4j.Logger;

import javax.annotation.Nullable;

public class TempFiles {
    private static final Logger LOGGER = Logger.getLogger(TempFiles.class);

    private TempFiles() {
    }

    public static void removeFileQuietly(@Nullable final TempFile tempFile) {
        if (tempFile != null) {
            try {
                tempFile.removeFile();
            } catch (final Throwable e) {
                LOGGER.warn("Failed to delete temp file " + tempFile.unsafeGetPath());
            }
        }
    }
}
