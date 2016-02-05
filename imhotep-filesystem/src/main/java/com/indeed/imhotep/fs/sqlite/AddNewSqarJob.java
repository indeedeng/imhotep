package com.indeed.imhotep.fs.sqlite;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteJob;
import com.almworks.sqlite4java.SQLiteStatement;
import com.indeed.imhotep.archive.FileMetadata;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;

import java.util.List;

/**
 * Created by darren on 12/10/15.
 */
public class AddNewSqarJob extends SQLiteJob<Integer> {
    private static final String BEGIN_TRANSACTION = "Begin transaction;";
    private static final String INSERT_SHARD_NAME =
            "INSERT OR IGNORE INTO sqar_names (name) VALUES (?);";
    private static final String SELECT_SHARD_ID = "SELECT id FROM sqar_names WHERE name = ?;";
    private static final String INSERT_FILE_NAME =
            "INSERT OR IGNORE INTO file_names (name) VALUES (?);";
    private static final String SELECT_FILE_NAME_ID =
            "SELECT id FROM file_names WHERE name = ?;";
    private static final String INSERT_FILE_JOIN_VALUE =
            "INSERT OR IGNORE INTO file_ids (sqar_id, file_name_id, type) VALUES (?, ?, ?);";
    private static final String SELECT_FILE_ID =
            "SELECT id FROM file_ids WHERE sqar_id = ? AND file_name_id = ?;";
    private static final String INSERT_ARCHIVE_NAME =
            "INSERT OR IGNORE INTO archive_names (name) VALUES (?);";
    private static final String SELECT_ARCHIVE_ID =
            "SELECT id FROM archive_names WHERE name = ?;";
    private static final String INSERT_FILE_INFO = "INSERT or replace into file_info "
            + "(file_id, archive_id, archive_offset, "
            + "unpacked_size, packed_size, timestamp, sig_hi, sig_low,"
            + "compressor_type, is_file) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
    private static final String END_TRANSACTION = "End transaction;";

    private final String shardName;
    private final List<FileMetadata> metadata;

    public AddNewSqarJob(String shardName, List<FileMetadata> metadata) {
        this.shardName = shardName;
        this.metadata = metadata;
    }

    @Override
    protected Integer job(SQLiteConnection connection) throws Throwable {
        SQLiteStatement beginStatement = connection.prepare(BEGIN_TRANSACTION, true);
        SQLiteStatement endStatement = connection.prepare(END_TRANSACTION, true);

        try {
            beginStatement.stepThrough();
            final int sqarId = getXId(SELECT_SHARD_ID, INSERT_SHARD_NAME, shardName, connection);
            for (FileMetadata md : metadata) {
                final int filenameId = getXId(SELECT_FILE_NAME_ID,
                                              INSERT_FILE_NAME,
                                              md.getFilename(),
                                              connection);
                final int fileId = getFileId(sqarId, filenameId, md.isFile(), connection);
                final int archiveId;
                if (md.isFile()) {
                    archiveId = getXId(SELECT_ARCHIVE_ID,
                                       INSERT_ARCHIVE_NAME,
                                       md.getArchiveFilename(),
                                       connection);
                } else {
                    archiveId = -1;
                }
                insertIntoFileInfo(fileId,
                                   archiveId,
                                   md.getStartOffset(),
                                   md.getSize(),
                                   md.getCompressedSize(),
                                   md.getTimestamp(),
                                   md.getChecksumHi(),
                                   md.getChecksumLow(),
                                   md.getCompressor(),
                                   md.isFile(),
                                   connection);
            }
            endStatement.stepThrough();
        } finally {
            beginStatement.dispose();
            endStatement.dispose();
        }
        return null;
    }

    private static int getXId(String selectString,
                              String insertString,
                              String name,
                              SQLiteConnection connection) throws Exception {
        SQLiteStatement selectStatement = connection.prepare(selectString, true);
        SQLiteStatement insertStatement = connection.prepare(insertString, true);

        try {
            selectStatement.bind(1, name);
            if (selectStatement.step()) {
                return selectStatement.columnInt(0);
            } else {
                insertStatement.bind(1, name);
                insertStatement.stepThrough();
                selectStatement.reset(false);
                if (selectStatement.step()) {
                    return selectStatement.columnInt(0);
                } else {
                    throw new Exception("Shard missing after insert.");
                }
            }
        } finally {
            selectStatement.dispose();
            insertStatement.dispose();
        }
    }

    private static int getFileId(int sqarId,
                                 int filenameId,
                                 boolean isFile,
                                 SQLiteConnection connection) throws
            Exception {
        SQLiteStatement selectStatement = connection.prepare(SELECT_FILE_ID, true);
        SQLiteStatement insertStatement = connection.prepare(INSERT_FILE_JOIN_VALUE, true);

        try {
            selectStatement.bind(1, sqarId);
            selectStatement.bind(2, filenameId);
            if (selectStatement.step()) {
                return selectStatement.columnInt(0);
            } else {
                insertStatement.bind(1, sqarId);
                insertStatement.bind(2, filenameId);
                insertStatement.bind(3, isFile ? 1 : 0);
                insertStatement.stepThrough();
                selectStatement.reset(false);
                if (selectStatement.step()) {
                    return selectStatement.columnInt(0);
                } else {
                    throw new Exception("Shard missing after insert.");
                }
            }
        } finally {
            selectStatement.dispose();
            insertStatement.dispose();
        }
    }

    private void insertIntoFileInfo(int fileId,
                                    int archiveId,
                                    long archiveOffset,
                                    long unpackedSize,
                                    long packedSize,
                                    long timestamp,
                                    long sigHi,
                                    long sigLow,
                                    SquallArchiveCompressor compressor,
                                    boolean isFile,
                                    SQLiteConnection connection) throws SQLiteException {
        SQLiteStatement insertStatement = connection.prepare(INSERT_FILE_INFO, true);

        try {
            insertStatement.bind(1, fileId);
            insertStatement.bind(2, archiveId);
            insertStatement.bind(3, archiveOffset);
            insertStatement.bind(4, unpackedSize);
            insertStatement.bind(5, packedSize);
            insertStatement.bind(6, timestamp);
            insertStatement.bind(7, sigHi);
            insertStatement.bind(8, sigLow);
            insertStatement.bind(9, compressor.getKey());
            insertStatement.bind(10, isFile ? 1 : 0);
            insertStatement.stepThrough();
        } finally {
            insertStatement.dispose();
        }
    }
}
