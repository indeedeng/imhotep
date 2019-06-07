package com.indeed.imhotep.utils.tempfiles;

public interface TempFileType<E extends Enum<E>> {
    String getIdentifier();
}
