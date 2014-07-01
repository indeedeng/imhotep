package com.indeed.imhotep.archive;

/**
 * @author jsgroth
 */
public class AcceptAllFileMetadataFilter implements FileMetadataFilter {
    @Override
    public boolean accept(FileMetadata file) {
        return true;
    }
}
