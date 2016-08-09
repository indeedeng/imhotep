//package com.indeed.imhotep.controller.sqlite;
//
//import com.almworks.sqlite4java.SQLiteConnection;
//import com.almworks.sqlite4java.SQLiteJob;
//import com.almworks.sqlite4java.SQLiteStatement;
//
///**
// * Created by darren on 12/10/15.
// */
//public class SelectOrInsertDatasetJob extends SQLiteJob<Integer> {
//    private static final String INSERT_DATASET_NAME =
//            "INSERT OR IGNORE INTO datasets (name) VALUES (?);";
//    private static final String SELECT_DATASET_ID =
//            "SELECT id FROM datasets WHERE name = ?;";
//
//    private final String datasetName;
//
//    public SelectOrInsertDatasetJob(String datasetName) {
//        this.datasetName = datasetName;
//    }
//
//    @Override
//    protected Integer job(SQLiteConnection connection) throws Exception {
//        SQLiteStatement selectStatement = connection.prepare(SELECT_DATASET_ID, true);
//        SQLiteStatement insertStatement = connection.prepare(INSERT_DATASET_NAME, true);
//
//        try {
//            selectStatement.bind(1, datasetName);
//            if (selectStatement.step()) {
//                return selectStatement.columnInt(0);
//            } else {
//                insertStatement.bind(1, datasetName);
//                insertStatement.stepThrough();
//                selectStatement.reset(false);
//                if (selectStatement.step()) {
//                    return selectStatement.columnInt(0);
//                } else {
//                    throw new Exception("Dataset missing after insert.");
//                }
//            }
//        } finally {
//            selectStatement.dispose();
//            insertStatement.dispose();
//        }
//    }
//
//}
