package com.indeed.imhotep.io.caching;

import java.io.*;
import java.security.DigestInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;
import com.indeed.imhotep.archive.ArchiveUtils;
import com.indeed.imhotep.archive.FileMetadata;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;

public class SqarRemoteFileSystem extends RemoteFileSystem {
    private static final String SUFFIX = ".sqar";
    private static final String METADATA_FILE = "metadata.txt";

    private String mountPoint;
    private RemoteFileSystemMounter mounter;
    private RemoteFileSystem parentFS;
    private List<FileMetadata> filesInArchive;
    private Comparator<FileMetadata> metadataComparator;

    public SqarRemoteFileSystem(Map<String,String> settings, 
                              RemoteFileSystem parent,
                              RemoteFileSystemMounter mounter) {
        this.parentFS = parent;
        
        mountPoint = settings.get("mountpoint").trim();
        if (! mountPoint.endsWith(DELIMITER)) {
            /* add delimiter to the end */
            mountPoint = mountPoint + DELIMITER;
        }
        mountPoint = mounter.getRootMountPoint() + mountPoint;
        mountPoint = mountPoint.replace("//", "/");
        
        this.mounter = mounter;
        
        this.filesInArchive = new ArrayList<FileMetadata>(800);
        this.metadataComparator = new Comparator<FileMetadata>() {
            @Override
            public int compare(FileMetadata fmd1, FileMetadata fmd2) {
                return fmd1.getFilename().compareTo(fmd2.getFilename());
            }
        };
        
        try {
            loadFileList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static FileMetadata parseMetadata(String line) throws IOException {
        final String[] split = line.split("\t");
        if (split.length < 5) {
            throw new IOException("malformed metadata line: " + line);
        }
        final String filename = split[0];
        final long size = Long.parseLong(split[1]);
        final long timestamp = Long.parseLong(split[2]);
        final String checksum = split[3];
        final long startOffset = Long.parseLong(split[4]);
        final SquallArchiveCompressor compressor = split.length > 5 ?
                SquallArchiveCompressor.fromKey(split[5]) : SquallArchiveCompressor.NONE;
        final String archiveFilename = split.length > 6 ? split[6] : "archive.bin";
        return new FileMetadata(filename, size, timestamp, checksum, startOffset, compressor,
                                archiveFilename);
   }

    private void loadFileList() throws IOException {
        final InputStream metadataIS;
        final String sqarpath;
        final BufferedReader r;
        
        /* download metadata file */
        sqarpath = mountPoint.substring(0, mountPoint.length() - DELIMITER.length()) + SUFFIX;
        metadataIS = parentFS.getInputStreamForFile(sqarpath + DELIMITER + METADATA_FILE, 0, -1);
        
        /* parse file */
        r = new BufferedReader(new InputStreamReader(metadataIS, Charsets.UTF_8));
        try {
            for (String line = r.readLine(); line != null; line = r.readLine()) {
                final FileMetadata metadata = parseMetadata(line);
                filesInArchive.add(metadata);
            }
        } finally {
            r.close();
        }
        
        /* sort metadata by file path */
        Collections.sort(filesInArchive, metadataComparator);
    }
    
    
    @Override
    public void copyFileInto(String fullPath, File localFile) throws IOException {
        final String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        final FileMetadata metadata;
        final FileMetadata searchKey;
        final int idx;
        final SquallArchiveCompressor compressor;
        final InputStream is;
        
        /* find the info about this compressed file */
        searchKey = new FileMetadata(relativePath, 0, 0, null, 0, null, null);
        idx = Collections.binarySearch(filesInArchive, searchKey, metadataComparator);
        if (idx < 0) {
            throw new FileNotFoundException("Could not locate " + relativePath + " in archive.");
        }
        metadata = filesInArchive.get(idx);
        
        /* download compressed file */
        final long startOffset = metadata.getStartOffset();
        final long originalSize = metadata.getSize();
        final String archiveFile = metadata.getArchiveFilename();
        final String sqarpath;
        final String archivePath;
        final DigestInputStream digestStream;
        final OutputStream os;

        sqarpath = mountPoint.substring(0, mountPoint.length() - DELIMITER.length()) + SUFFIX;
        archivePath = sqarpath + DELIMITER + archiveFile;
        is = parentFS.getInputStreamForFile(archivePath, startOffset, originalSize);
        try {
            compressor = metadata.getCompressor();
            
            digestStream = new DigestInputStream(compressor.newInputStream(is), 
                                                 ArchiveUtils.getMD5Digest());
            os = new BufferedOutputStream(new FileOutputStream(localFile));
            ArchiveUtils.streamCopy(digestStream, os, originalSize);
            os.close();
        } finally {
            is.close();
        }

        final String checksum = ArchiveUtils.toHex(digestStream.getMessageDigest().digest());
        if (!checksum.equals(metadata.getChecksum())) {
            throw new IOException("invalid checksum for file " + fullPath + 
                                  " in archive " + archivePath + 
                                  ": file checksum = " + checksum + 
                                  ", checksum in metadata = " + metadata.getChecksum());
        }
    }

    @Override
    public File loadFile(String fullPath) throws IOException {
        final File file;
        
        file = File.createTempFile("sqar", ".cachedFile");
        copyFileInto(fullPath, file);
        return file;
    }

    @Override
    public RemoteFileInfo stat(String fullPath) {
        final String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        final String relPathWithDelimiter;
        final FileMetadata metadata;
        FileMetadata searchKey;
        int idx;

        if (relativePath.isEmpty()) {
            /* root of a sqar file is a directory by definition */
            return new RemoteFileInfo(fullPath, RemoteFileInfo.TYPE_DIR);
        }

        relPathWithDelimiter = relativePath + DELIMITER;
        
        /* find the info about this compressed file */
        searchKey = new FileMetadata(relativePath, 0, 0, null, 0, null, null);
        idx = Collections.binarySearch(filesInArchive, searchKey, metadataComparator);
        if (idx >= 0) {
            /* found a file */
            return new RemoteFileInfo(fullPath, RemoteFileInfo.TYPE_FILE);
        }
        
        /* look for a directory with this name */
        searchKey = new FileMetadata(relPathWithDelimiter, 0, 0, null, 0, null, null);
        idx = Collections.binarySearch(filesInArchive, searchKey, metadataComparator);
        if (idx >= 0) {
            /* uh, this should not be */
            throw new RuntimeException("Bad filenam in archive.  file: " + relPathWithDelimiter);
        }
        idx = -idx - 1;
        if (idx >= filesInArchive.size()) {
            /* not found */
            return null;
        }

        metadata = filesInArchive.get(idx);
        if (metadata.getFilename().startsWith(relativePath)) {
            return new RemoteFileInfo(fullPath, RemoteFileInfo.TYPE_DIR);
        }
        
        /* not found */
        return null;
    }

    @Override
    public List<RemoteFileInfo> readDir(String fullPath) {
        String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        final FileMetadata searchKey;
        final FileMetadata metadata;
        final List<RemoteFileInfo> listing;
        int idx;

        if (!relativePath.isEmpty()) {
            relativePath += DELIMITER;
        }

        /* find the info about this compressed file */
        searchKey = new FileMetadata(relativePath, 0, 0, null, 0, null, null);
        idx = Collections.binarySearch(filesInArchive, searchKey, metadataComparator);
        if (idx >= 0) {
            /* not a directory */
            return null;
        }
        
        idx = -idx - 1;
        if (idx >= filesInArchive.size()) {
            /* not found */
            return null;
        }

        metadata = filesInArchive.get(idx);
        if (! metadata.getFilename().startsWith(relativePath)) {
            /* not found */
            return null;
        }
        
        listing = scanListForMatchingPaths(filesInArchive, relativePath, idx);
        return listing;
    }
    
    private List<RemoteFileInfo> scanListForMatchingPaths(List<FileMetadata> files, 
                                                          String prefix,
                                                          int startIdx) {
        final List<RemoteFileInfo> listing = new ArrayList<RemoteFileInfo>(100);
        String lastPathSegment = "";

        for (int idx = startIdx; idx < filesInArchive.size(); idx++) {
            final int type;
            final FileMetadata metadata;
            final String filename;
            final String pathSegment;

            metadata = filesInArchive.get(idx);
            filename = metadata.getFilename();
            if (! filename.startsWith(prefix)) {
                break;
            }
            
            pathSegment = nextPathSegment(filename, prefix);
            if (pathSegment.equals(lastPathSegment)) {
                continue;
            }
            if (pathSegment.contains(DELIMITER)) {
                type = RemoteFileInfo.TYPE_DIR;
            } else {
                type = RemoteFileInfo.TYPE_FILE;
            }
            listing.add(new RemoteFileInfo(pathSegment, type));
            lastPathSegment = pathSegment;
        }
        
        return listing;
    }
    
    private String nextPathSegment(String path, String prefix) {
        int idx;
        
        idx = path.indexOf(DELIMITER, prefix.length());
        if (idx == -1) {
            /* delimiter not found */
            idx = path.length();
        }
        
        return path.substring(prefix.length(), idx);
    }

    @Override
    public String getMountPoint() {
        return this.mountPoint;
    }

    /*
     * Unneeded for now
     */
    @Override
    public Map<String, File> loadDirectory(String fullPath, File location) throws IOException {
        throw new UnsupportedOperationException();
    }


    /*
     * Unneeded for now
     */
    @Override
    public InputStream getInputStreamForFile(String archivePath,
                                             long startOffset,
                                             long maxReadLength) {
        throw new UnsupportedOperationException();
    }
}
