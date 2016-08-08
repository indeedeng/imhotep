//package com.indeed.imhotep.controller.sqlite;
//
//import com.almworks.sqlite4java.SQLiteConnection;
//import com.almworks.sqlite4java.SQLiteException;
//import com.almworks.sqlite4java.SQLiteJob;
//import com.almworks.sqlite4java.SQLiteStatement;
//
///**
// * Created by darren on 12/10/15.
// */
//public class InitDBJob extends SQLiteJob<Boolean> {
//    private static final String CREATE_DATASET_TBL = "CREATE TABLE IF NOT EXISTS datasets "
//            + "(name varchar(255), id INTEGER PRIMARY KEY);";
//    private static final String CREATE_DATASET_IDX =
//            "CREATE UNIQUE INDEX IF NOT EXISTS dataset_name_idx ON datasets (name);";
//    private static final String CREATE_SHARDS_TBL = "CREATE TABLE IF NOT EXISTS shards "
//            + "(dataset_id INTEGER,"
//            + " start_time INTEGER,"
//            + " end_time INTEGER,"
//            + " version INTEGER,"
//            + " format INTEGER,"
//            + " id INTEGER PRIMARY KEY);";
//    private static final String CREATE_SHARDS_START_TIME_IDX =
//            "CREATE INDEX IF NOT EXISTS shards_start_idx ON shards (dataset_id, start_time);";
//    private static final String CREATE_SHARDS_END_TIME_IDX =
//            "CREATE INDEX IF NOT EXISTS shards_end_idx ON shards (dataset_id, end_time);";
//    private static final String CREATE_FIELD_NAMES_TBL = "CREATE TABLE IF NOT EXISTS field_names "
//            + "(name varchar(255), id INTEGER PRIMARY KEY);";
//    private static final String CREATE_FIELD_NAMES_IDX =
//            "CREATE UNIQUE INDEX IF NOT EXISTS field_name_idx ON field_names (name);";
//    private static final String CREATE_FIELD_IDS_TBL = "CREATE TABLE IF NOT EXISTS field_ids "
//            + "(id INTEGER PRIMARY KEY,"
//            + " shard_id INTEGER,"
//            + " field_name_id INTEGER,"
//            + " type INTEGER,"
//            + " num_docs INTEGER);";
//    private static final String CREATE_FIELD_IDS_IDX =
//            "CREATE UNIQUE INDEX IF NOT EXISTS field_ids_idx "
//                    + "ON field_ids (shard_id, field_name_id);";
//
//    @Override
//    protected Boolean job(SQLiteConnection connection) throws Throwable {
//        executeStatement(connection, CREATE_DATASET_TBL);
//        executeStatement(connection, CREATE_DATASET_IDX);
//        executeStatement(connection, CREATE_SHARDS_TBL);
//        executeStatement(connection, CREATE_SHARDS_START_TIME_IDX);
//        executeStatement(connection, CREATE_SHARDS_END_TIME_IDX);
//        executeStatement(connection, CREATE_FIELD_NAMES_TBL);
//        executeStatement(connection, CREATE_FIELD_NAMES_IDX);
//        executeStatement(connection, CREATE_FIELD_IDS_TBL);
//        executeStatement(connection, CREATE_FIELD_IDS_IDX);
//
//        return null;
//    }
//
//    private static void executeStatement(SQLiteConnection connection, String sql) throws
//            SQLiteException {
//        SQLiteStatement statement = connection.prepare(sql, false);
//        try {
//            statement.stepThrough();
//        } finally {
//            statement.dispose();
//        }
//    }
//}
