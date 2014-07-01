package com.indeed.imhotep.archive;

/**
 * @author jsgroth
 */
public interface FileMetadataFilter {
    boolean accept(FileMetadata file);
}
