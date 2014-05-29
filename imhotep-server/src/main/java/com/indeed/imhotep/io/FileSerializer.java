package com.indeed.imhotep.io;

import java.io.File;
import java.io.IOException;

/**
 * @author jsgroth
 */
public interface FileSerializer<T> {
    void serialize(T t, File file) throws IOException;
    T deserialize(File file) throws IOException;
}
