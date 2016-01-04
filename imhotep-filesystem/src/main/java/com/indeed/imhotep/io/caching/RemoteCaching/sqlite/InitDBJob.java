package com.indeed.imhotep.io.caching.RemoteCaching.sqlite;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteJob;
import com.almworks.sqlite4java.SQLiteStatement;

/**
 * Created by darren on 12/10/15.
 */
public class InitDBJob extends SQLiteJob<Boolean> {
    private static final String CREATE_SQAR_TBL = "create table if not exists sqar_names "
            + "(name varchar(255), id INTEGER PRIMARY KEY);";
    private static final String CREATE_SQAR_IDX =
            "create unique index if not exists sqar_name_idx " + "on sqar_names (name);";
    private static final String CREATE_FILE_NAMES_TBL = "create table if not exists file_names "
            + "(name varchar(255), id INTEGER PRIMARY KEY);";
    private static final String CREATE_FILE_NAMES_IDX =
            "create unique index if not exists file_name_idx " + "on file_names (name);";
    private static final String CREATE_FILE_IDS_TBL = "create table if not exists file_ids "
            + "(id INTEGER PRIMARY KEY, shard_id INTEGER, file_name_id INTEGER);";
    private static final String CREATE_FILE_IDS_IDX =
            "create unique index if not exists file_ids_idx "
                    + "on file_ids (sqar_id, file_name_id);";
    private static final String CREATE_FILENAME_2_ID_VIEW =
            "create view if not exists file_name_to_id " +
                    "(id, sqar_id, file_name) as " +
                    "select file_ids.id, file_ids.sqar_id, file_names.name from file_names " +
                    "inner join file_ids on file_ids.file_name_id = file_names.id;";
    private static final String CREATE_ARCHIVE_TBL = "create table if not exists archive_names "
            + "(name varchar(255), id INTEGER PRIMARY KEY);";
    private static final String CREATE_ARCHIVE_IDX =
            "create unique index if not exists archive_name_idx " + "on archive_names (name);";
    private static final String CREATE_FILE_INFO_TBL = "create table if not exists file_info " +
            "(file_id INTEGER, " +
            "archive_id INTEGER, " +
            "archive_offset BIGINT, " +
            "unpacked_size BIGINT, " +
            "packed_size BIGINT, " +
            "timestamp BIGINT, " +
            "sig_hi BIGINT, " +
            "sig_low BIGINT," +
            "compressor VARCHAR(16)," +
            "isFile INTEGER);";
    private static final String CREATE_FILE_INFO_IDX =
            "create unique index if not exists file_info_idx " + "on file_info (file_id);";

    @Override
    protected Boolean job(SQLiteConnection connection) throws Throwable {
        SQLiteStatement sqarTblStatement = connection.prepare(CREATE_SQAR_TBL, false);
        SQLiteStatement sqarIdxStatement = connection.prepare(CREATE_SQAR_IDX, false);
        SQLiteStatement fileTblStatement = connection.prepare(CREATE_FILE_NAMES_TBL, false);
        SQLiteStatement fileIdxStatement = connection.prepare(CREATE_FILE_NAMES_IDX, false);
        SQLiteStatement fileIdsTblStatement = connection.prepare(CREATE_FILE_IDS_TBL, false);
        SQLiteStatement fileIdsIdxStatement = connection.prepare(CREATE_FILE_IDS_IDX, false);
        SQLiteStatement fileN2IdStatement = connection.prepare(CREATE_FILENAME_2_ID_VIEW, false);
        SQLiteStatement archiveTblStatement = connection.prepare(CREATE_ARCHIVE_TBL, false);
        SQLiteStatement archiveIdxStatement = connection.prepare(CREATE_ARCHIVE_IDX, false);
        SQLiteStatement fileInfoTblStatement = connection.prepare(CREATE_FILE_INFO_TBL, false);
        SQLiteStatement fileInfoIdxStatement = connection.prepare(CREATE_FILE_INFO_IDX, false);

        try {
            sqarTblStatement.stepThrough();
            sqarIdxStatement.stepThrough();
            fileTblStatement.stepThrough();
            fileIdxStatement.stepThrough();
            fileIdsTblStatement.stepThrough();
            fileIdsIdxStatement.stepThrough();
            fileN2IdStatement.stepThrough();
            archiveTblStatement.stepThrough();
            archiveIdxStatement.stepThrough();
            fileInfoTblStatement.stepThrough();
            fileInfoIdxStatement.stepThrough();
        } finally {
            sqarTblStatement.dispose();
            sqarIdxStatement.dispose();
            fileTblStatement.dispose();
            fileIdxStatement.dispose();
            fileIdsTblStatement.dispose();
            fileIdsIdxStatement.dispose();
            fileN2IdStatement.dispose();
            archiveTblStatement.dispose();
            archiveIdxStatement.dispose();
            fileInfoTblStatement.dispose();
        }

        return null;
    }
}
