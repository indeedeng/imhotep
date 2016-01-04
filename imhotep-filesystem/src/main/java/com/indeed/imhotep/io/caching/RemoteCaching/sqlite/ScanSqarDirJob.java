package com.indeed.imhotep.io.caching.RemoteCaching.sqlite;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteJob;
import com.almworks.sqlite4java.SQLiteStatement;
import com.indeed.imhotep.io.caching.RemoteCaching.RemoteFileStore;
import com.indeed.imhotep.io.caching.RemoteCaching.RemoteFileStore.RemoteFileInfo;
import com.indeed.imhotep.io.caching.RemoteCaching.SqarManager;

import java.util.ArrayList;

/**
 * Created by darren on 12/10/15.
 */
public class ScanSqarDirJob extends SQLiteJob<ArrayList<RemoteFileInfo>> {
    private static final String SELECT_FILE_NAMES_WITH_PREFIX =
            "select file_name from file_name_to_id " +
            "where sqar_id = ? AND instr(file_name, ?) = 1;";
    private static final String SELECT_FILE_NAMES_NO_PREFIX =
            "select file_name from file_name_to_id " +
            "where sqar_id = ?;";

    private final int sqarId;
    private final String prefix;

    public ScanSqarDirJob(int sqarId, String prefix) {
        this.sqarId = sqarId;
        this.prefix = prefix;
    }

    @Override
    protected ArrayList<RemoteFileInfo> job(SQLiteConnection connection) throws Throwable {
        final SQLiteStatement fileNamesSelectStatement;
        final boolean noPrefix;
        final ArrayList<RemoteFileInfo> results;

        if (prefix == null || prefix.isEmpty()) {
            noPrefix = true;
            fileNamesSelectStatement = connection.prepare(SELECT_FILE_NAMES_NO_PREFIX, true);
        } else {
            noPrefix = false;
            fileNamesSelectStatement = connection.prepare(SELECT_FILE_NAMES_WITH_PREFIX, true);
        }

        try {
            /* bind sqar id and prefix */
            fileNamesSelectStatement.bind(1, sqarId);
            if (!noPrefix) {
                fileNamesSelectStatement.bind(2, prefix);
            }

            results = processFilenames(fileNamesSelectStatement, noPrefix);
        } finally {
            fileNamesSelectStatement.dispose();
        }

        return results;
    }

    private ArrayList<RemoteFileInfo> processFilenames(SQLiteStatement fileNamesSelectStatement,
                                                       boolean noPrefix) throws SQLiteException {
        final ArrayList<RemoteFileInfo> results = new ArrayList<>(1024);
        String lastPathSegment = "";

        while (fileNamesSelectStatement.step()) {
            final String path = fileNamesSelectStatement.columnString(0);
            final String pathSegment;
            final boolean isFile;

            if (noPrefix) {
                pathSegment = path;
            } else {
                pathSegment = nextPathSegment(path, prefix);
                if (pathSegment.equals(lastPathSegment)) {
                    continue;
                }
            }
            isFile = pathSegment.indexOf(SqarManager.DELIMITER) == -1;
            results.add(new RemoteFileInfo(pathSegment, -1, isFile));

            lastPathSegment = pathSegment;
        }

        return results;
    }

    private String nextPathSegment(String path, String prefix) {
        int idx;

        idx = path.indexOf(SqarManager.DELIMITER, prefix.length());
        if (idx == -1) {
            /* delimiter not found */
            idx = path.length();
        }

        return path.substring(prefix.length(), idx);
    }

}
