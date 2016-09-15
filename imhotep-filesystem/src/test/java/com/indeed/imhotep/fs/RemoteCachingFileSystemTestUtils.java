package com.indeed.imhotep.fs;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author kenh
 */

class RemoteCachingFileSystemTestUtils {
    private RemoteCachingFileSystemTestUtils() {
    }

    static void writeToFile(final File file, final String...output) throws IOException {
        com.indeed.util.io.Files.writeToTextFileOrDie(output, file.toString());
    }

    static String readFromPath(final Path path) throws IOException {
        try (InputStream inputStream = Files.newInputStream(path)) {
            final StringWriter stringWriter = new StringWriter();
            IOUtils.copy(inputStream, stringWriter);
            return stringWriter.toString();
        }
    }
}
