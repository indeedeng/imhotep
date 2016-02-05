package com.indeed.imhotep.fs.sqlite;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteJob;
import com.almworks.sqlite4java.SQLiteStatement;
import com.indeed.imhotep.archive.FileMetadata;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;

/**
 * Created by darren on 12/10/15.
 */
public class ReadPathInfoJob extends SQLiteJob<FileMetadata> {
    private static final String SELECT_FILE_INFO = "select " +
            "file_info.archive_id, " +
            "file_info.archive_offset, " +
            "file_info.unpacked_size, " +
            "file_info.packed_size, " +
            "file_info.timestamp, " +
            "file_info.sig_hi, " +
            "file_info.sig_low, " +
            "file_info.compressor_type, " +
            "file_info.is_file " +
            "from file_name_to_id " +
            "inner join file_info " +
            "on file_name_to_id.id = file_info.file_id " +
            "where file_name_to_id.sqar_id = ? AND file_name_to_id.name = ?;";
    private static final String SELECT_ARCHIVE_FILE_NAME =
            "select name from archive_names where id = ?;";

    private final int sqarId;
    private final String fileName;

    public ReadPathInfoJob(int sqarId, String fileName) {
        this.sqarId = sqarId;
        this.fileName = fileName;
    }

    @Override
    protected FileMetadata job(SQLiteConnection connection) throws Throwable {
        SQLiteStatement fileInfoSelectStatement = connection.prepare(SELECT_FILE_INFO, true);
        SQLiteStatement archiveNameSelectStatement =
                connection.prepare(SELECT_ARCHIVE_FILE_NAME, true);

        try {
            /* bind file names select statement */
            fileInfoSelectStatement.bind(1, sqarId);
            fileInfoSelectStatement.bind(2, fileName);

            if (fileInfoSelectStatement.step()) {
                final int archiveId = fileInfoSelectStatement.columnInt(0);
                final long archiveOffset = fileInfoSelectStatement.columnLong(1);
                final long unpackedSize = fileInfoSelectStatement.columnLong(2);
                final long packedSize = fileInfoSelectStatement.columnLong(3);
                final long timestamp = fileInfoSelectStatement.columnLong(4);
                final long sigHi = fileInfoSelectStatement.columnLong(5);
                final long sigLow = fileInfoSelectStatement.columnLong(6);
                final SquallArchiveCompressor compType = SquallArchiveCompressor.fromKey(
                        fileInfoSelectStatement.columnString(7)
                );
                final boolean isFile = fileInfoSelectStatement.columnInt(8) != 0;

                fileInfoSelectStatement.stepThrough();

                if (isFile) {
                    final String archiveFileName;
                    archiveNameSelectStatement.bind(1, archiveId);
                    if (archiveNameSelectStatement.step()) {
                        archiveFileName = archiveNameSelectStatement.columnString(0);
                    } else {
                        throw new Exception("Archive name missing. id: " + archiveId);
                    }

                    return new FileMetadata(fileName,
                                            unpackedSize,
                                            packedSize,
                                            timestamp,
                                            sigHi,
                                            sigLow,
                                            archiveOffset,
                                            compType,
                                            archiveFileName,
                                            isFile);
                } else {
                    /* path is a directory */
                    return new FileMetadata(fileName, isFile);
                }
            } else {
                /* no file found */
                return null;
            }
        } finally {
            fileInfoSelectStatement.dispose();
            archiveNameSelectStatement.dispose();
        }
    }
}
