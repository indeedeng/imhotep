package com.indeed.imhotep.fs.sqlite;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteJob;
import com.almworks.sqlite4java.SQLiteStatement;

/**
 * Created by darren on 12/10/15.
 */
public class InitDBJob extends SQLiteJob<Boolean> {
    private static final String CREATE_SQAR_TBL = "CREATE TABLE IF NOT EXISTS sqar_names "
            + "(name varchar(255), id INTEGER PRIMARY KEY);";
    private static final String CREATE_SQAR_IDX =
            "CREATE UNIQUE INDEX IF NOT EXISTS sqar_name_idx ON sqar_names (name);";
    private static final String CREATE_FILE_NAMES_TBL = "CREATE TABLE IF NOT EXISTS file_names "
            + "(name varchar(255), id INTEGER PRIMARY KEY);";
    private static final String CREATE_FILE_NAMES_IDX =
            "CREATE UNIQUE INDEX IF NOT EXISTS file_name_idx ON file_names (name);";
    private static final String CREATE_FILE_IDS_TBL = "CREATE TABLE IF NOT EXISTS file_ids "
            + "(id INTEGER PRIMARY KEY, sqar_id INTEGER, file_name_id INTEGER, type INTEGER);";
    private static final String CREATE_FILE_IDS_IDX =
            "CREATE UNIQUE INDEX IF NOT EXISTS file_ids_idx "
                    + "ON file_ids (sqar_id, file_name_id);";
    private static final String CREATE_FILENAME_2_ID_VIEW =
            "CREATE VIEW IF NOT EXISTS file_name_to_id "
//                    + "(id, sqar_id, name, type) "
                    + "AS "
                    + "SELECT file_ids.id, file_ids.sqar_id, file_names.name, file_ids.type "
                    + "FROM file_names INNER JOIN file_ids "
                    + "WHERE file_ids.file_name_id = file_names.id;";
    private static final String CREATE_ARCHIVE_TBL = "CREATE TABLE IF NOT EXISTS archive_names "
            + "(name varchar(255), id INTEGER PRIMARY KEY);";
    private static final String CREATE_ARCHIVE_IDX =
            "CREATE UNIQUE INDEX IF NOT EXISTS archive_name_idx ON archive_names (name);";
    private static final String CREATE_FILE_INFO_TBL = "CREATE TABLE IF NOT EXISTS file_info " +
            "(file_id INTEGER, " +
            "archive_id INTEGER, " +
            "archive_offset BIGINT, " +
            "unpacked_size BIGINT, " +
            "packed_size BIGINT, " +
            "timestamp BIGINT, " +
            "sig_hi BIGINT, " +
            "sig_low BIGINT," +
            "compressor_type VARCHAR(16)," +
            "is_file INTEGER);";
    private static final String CREATE_FILE_INFO_IDX =
            "CREATE UNIQUE INDEX IF NOT EXISTS file_info_idx ON file_info (file_id);";

    @Override
    protected Boolean job(SQLiteConnection connection) throws Throwable {
        executeStatement(connection, CREATE_SQAR_TBL);
        executeStatement(connection, CREATE_SQAR_IDX);
        executeStatement(connection, CREATE_FILE_NAMES_TBL);
        executeStatement(connection, CREATE_FILE_NAMES_IDX);
        executeStatement(connection, CREATE_FILE_IDS_TBL);
        executeStatement(connection, CREATE_FILE_IDS_IDX);
        executeStatement(connection, CREATE_FILENAME_2_ID_VIEW);
        executeStatement(connection, CREATE_ARCHIVE_TBL);
        executeStatement(connection, CREATE_ARCHIVE_IDX);
        executeStatement(connection, CREATE_FILE_INFO_TBL);
        executeStatement(connection, CREATE_FILE_INFO_IDX);

        return null;
    }

    private static void executeStatement(SQLiteConnection connection, String sql) throws
            SQLiteException {
        SQLiteStatement statement = connection.prepare(sql, false);
        try {
            statement.stepThrough();
        } finally {
            statement.dispose();
        }
    }
}
