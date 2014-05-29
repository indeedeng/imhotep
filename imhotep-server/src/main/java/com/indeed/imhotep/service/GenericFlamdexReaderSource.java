package com.indeed.imhotep.service;

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.reader.GenericFlamdexReader;

import java.io.IOException;

/**
 * @author jsgroth
 */
public class GenericFlamdexReaderSource implements FlamdexReaderSource {
    @Override
    public FlamdexReader openReader(String directory) throws IOException {
        return GenericFlamdexReader.open(directory);
    }
}
