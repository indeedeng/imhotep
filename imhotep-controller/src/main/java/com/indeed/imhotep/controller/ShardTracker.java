package com.indeed.imhotep.controller;

import gnu.trove.list.array.TIntArrayList;
import org.apache.log4j.Logger;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by darren on 3/3/16.
 */
public class ShardTracker {
    private static final Logger log = Logger.getLogger(RemoteFileScanner.class);

    private final Connection connection;
    private final SqlStatements sqlStatements;

    public ShardTracker(Map<String, String> settings) throws ClassNotFoundException, SQLException {
        final File dbFile = new File(settings.get("database-location"));

        /* initialize DB */
        Class.forName("org.h2.Driver");
        this.connection = DriverManager.getConnection("jdbc:h2:" + dbFile);
        this.sqlStatements = new SqlStatements(this.connection);
    }

    public ArrayList<ShardInfo> findShards(String datasetName, long startTime, long endTime) throws
            SQLException {
        final int datasetId;
        final ArrayList<ShardInfo> infos;

        datasetId = sqlStatements.execute_SELECT_DATASET_ID_STATEMENT(datasetName);
        infos = sqlStatements.execute_SELECT_SHARD_RANGE_STATEMENT(datasetId, startTime, endTime);

        if (infos.size() == 0) {
            return infos;
        }

        /* find duplicates */
        final TIntArrayList dupIndexes = new TIntArrayList();
        long last_ts = infos.get(0).startTime;
        long last_version = infos.get(0).version;
        for (int i = 1; i < infos.size(); i++) {
            final ShardInfo si = infos.get(i);
            if (last_ts == si.startTime && last_version < si.version) {
                dupIndexes.add(i - 1);
            }
            last_ts = si.startTime;
            last_version = si.version;
        }

        /* remove duplicates */
        for (int i = dupIndexes.size() - 1; i >= 0; i--)
            infos.remove(i);

        return infos;
    }
}
