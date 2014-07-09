package com.indeed.imhotep.io.caching;

import java.io.*;
import java.security.DigestInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.indeed.imhotep.archive.ArchiveUtils;
import com.indeed.imhotep.archive.FileMetadata;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;

public class SqarRemoteFileSystem extends RemoteFileSystem {
    private static final String SUFFIX = ".sqar";
    private static final String METADATA_FILE = "metadata.txt";

    final private String mountPoint;
    final private RemoteFileSystemMounter mounter;
    final private RemoteFileSystem parentFS;

    public SqarRemoteFileSystem(Map<String,Object> settings, 
                              RemoteFileSystem parent,
                              RemoteFileSystemMounter mounter) {
        String mp;
        
        this.parentFS = parent;
        
        mp = (String)settings.get("mountpoint");
        if (! mp.endsWith(DELIMITER)) {
            /* add delimiter to the end */
            mp = mp + DELIMITER;
        }
        mp = mounter.getRootMountPoint() + mp;
        mountPoint = mp.replace("//", "/");
        
        this.mounter = mounter;
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
    
    private FileMetadata scanMetadataForFile(String file) throws IOException {
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
                if (file.equals(metadata.getFilename())) {
                    return metadata;
                }
            }
            /* found nothing */
            return null;
        } finally {
            r.close();
            metadataIS.close();
        }
    }

    private List<FileMetadata> scanMetadataForPrefix(String prefix, boolean stopAfterFirst) throws IOException {
        final InputStream metadataIS;
        final String sqarpath;
        final BufferedReader r;
        final List<FileMetadata> results = new ArrayList<FileMetadata>(500);
        
        /* download metadata file */
        sqarpath = mountPoint.substring(0, mountPoint.length() - DELIMITER.length()) + SUFFIX;
        metadataIS = parentFS.getInputStreamForFile(sqarpath + DELIMITER + METADATA_FILE, 0, -1);
        
        /* parse file */
        r = new BufferedReader(new InputStreamReader(metadataIS, Charsets.UTF_8));

        try {
            for (String line = r.readLine(); line != null; line = r.readLine()) {
                final FileMetadata metadata = parseMetadata(line);
                if (metadata.getFilename().startsWith(prefix)) {
                    results.add(metadata);
                    if (stopAfterFirst) {
                        break;
                    }
                }
            }
            return results;
        } finally {
            r.close();
            metadataIS.close();
        }
    }

    
    @Override
    public void copyFileInto(String fullPath, File localFile) throws IOException {
        final String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        final FileMetadata metadata;
        final SquallArchiveCompressor compressor;
        final InputStream is;
        
        /* find the info about this compressed file */
        metadata = scanMetadataForFile(relativePath);
        if (metadata == null) {
            throw new FileNotFoundException("Could not locate " + relativePath + " in archive.");
        }
        
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
        is = parentFS.getInputStreamForFile(archivePath, 
                                            startOffset, 
                                            originalSize + 2048 /* for safety */);
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
        final List<FileMetadata> listing;

        if (relativePath.isEmpty()) {
            /* root of a sqar file is a directory by definition */
            return new RemoteFileInfo(fullPath, RemoteFileInfo.TYPE_DIR);
        }

        relPathWithDelimiter = relativePath + DELIMITER;
        
        try {
            /* find the info about this compressed file */
            metadata = scanMetadataForFile(relativePath);
            if (metadata != null) {
                /* found a file */
                return new RemoteFileInfo(fullPath, RemoteFileInfo.TYPE_FILE);
            }
            
            /* look for a directory with this name */
            listing = scanMetadataForPrefix(relPathWithDelimiter, true);
            if (listing.size() == 0) {
                /* not found */
                return null;
            }
    
            return new RemoteFileInfo(fullPath, RemoteFileInfo.TYPE_DIR);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public List<RemoteFileInfo> readDir(String fullPath) {
        String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        final List<RemoteFileInfo> infoListing;
        final List<FileMetadata> mdListing;

        if (!relativePath.isEmpty()) {
            relativePath += DELIMITER;
        }

        /* find the info about this compressed file */
        /* look for a directory with this name */
        try {
            mdListing = scanMetadataForPrefix(relativePath, false);
            if (mdListing.size() == 0) {
                /* not a directory */
                return null;
            }
            if (mdListing.size() == 1 && mdListing.get(0).getFilename().equals(relativePath)) {
                /* not a directory */
                return null;
            }
            
            infoListing = scanListForMatchingPaths(mdListing, relativePath);
            return infoListing;
        } catch (IOException e) {
            return null;
        }
    }
    
    private List<RemoteFileInfo> scanListForMatchingPaths(List<FileMetadata> files, 
                                                          String prefix) {
        final List<RemoteFileInfo> listing = new ArrayList<RemoteFileInfo>(100);
        String lastPathSegment = "";

        for (FileMetadata metadata : files) {
            final int type;
            final String filename;
            final String pathSegment;

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

    @Override
    public Map<String, File> loadDirectory(String fullPath, File location) throws IOException {
        String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        final List<FileMetadata> mdListing;
        final Map<String, File> results = new HashMap<String,File>();

        if (!relativePath.isEmpty()) {
            relativePath += DELIMITER;
        }

        /* find the info about this compressed file */
        /* look for a directory with this name */
        try {
            mdListing = scanMetadataForPrefix(relativePath, false);
            if (mdListing.size() == 0) {
                /* not a directory */
                return null;
            }
            if (mdListing.size() == 1 && mdListing.get(0).getFilename().equals(relativePath)) {
                /* not a directory */
                return null;
            }
            
            /* download all the files */
            for (FileMetadata md : mdListing) {
                final String path = mountPoint + md.getFilename();
                final String dirRelPath = md.getFilename().substring(relativePath.length());
                final File f = new File(location, dirRelPath);
                
                /* create any parent dirs that do not already exist */
                f.getParentFile().mkdirs();
                
                copyFileInto(path, f);
                
                results.put(path, f);
            }
            
            return results;
        } catch (IOException e) {
            return null;
        }
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
