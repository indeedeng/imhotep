package com.indeed.imhotep;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author kenh
 */

public class InputStreamFTGSIterators {
    private InputStreamFTGSIterators() {}

    private static InputStream createInputStream(final File file) throws FileNotFoundException {
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file));

        return new FilterInputStream(bufferedInputStream) {
            public void close() throws IOException {
                bufferedInputStream.close();
            }
        };
    }

    public static InputStreamFTGSIterator create(final File file, final int numStats) throws FileNotFoundException {
        return new InputStreamFTGSIterator(createInputStream(file), numStats);
    }
}
