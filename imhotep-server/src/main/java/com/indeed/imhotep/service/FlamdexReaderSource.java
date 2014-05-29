package com.indeed.imhotep.service;

import com.indeed.flamdex.api.FlamdexReader;

import java.io.IOException;

/**
 * @author jsgroth
 */
public interface FlamdexReaderSource {
    FlamdexReader openReader(String directory) throws IOException;
}
