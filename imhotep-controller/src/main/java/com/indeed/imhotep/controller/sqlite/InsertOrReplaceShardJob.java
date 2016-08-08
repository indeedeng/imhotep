//package com.indeed.imhotep.controller.sqlite;
//
//import com.almworks.sqlite4java.SQLiteConnection;
//import com.almworks.sqlite4java.SQLiteJob;
//import com.almworks.sqlite4java.SQLiteStatement;
//import com.indeed.imhotep.controller.ShardInfo;
//
///**
// * Created by darren on 12/10/15.
// */
//public class InsertOrReplaceShardJob extends SQLiteJob<Integer> {
//    private static final String INSERT_SHARD_NAME =
//            "INSERT OR REPLACE INTO shards "
//                    + "("
//                    + " dataset_id,"
//                    + " start_time,"
//                    + " end_time,"
//                    + " version,"
//                    + " format"
//                    + ")"
//                    + " VALUES"
//                    + " (?,?,?,?,?);";
//
//    private final int datasetId;
//    private final long startTime;
//    private final long endTime;
//    private final long version;
//    private final int format;
//
//    public InsertOrReplaceShardJob(int datasetId, ShardInfo shard) {
//        this.datasetId = datasetId;
//        this.startTime = shard.startTime;
//        this.endTime = shard.endTime;
//        this.version = shard.version;
//        this.format = shard.format;
//    }
//
//    @Override
//    protected Integer job(SQLiteConnection connection) throws Exception {
//        SQLiteStatement insertStatement = connection.prepare(INSERT_SHARD_NAME, true);
//
//        try {
//            insertStatement.bind(1, datasetId);
//            insertStatement.bind(2, startTime);
//            insertStatement.bind(3, endTime);
//            insertStatement.bind(4, version);
//            insertStatement.bind(5, format);
//            insertStatement.stepThrough();
//        } finally {
//            insertStatement.dispose();
//        }
//
//        return null;
//    }
//}
