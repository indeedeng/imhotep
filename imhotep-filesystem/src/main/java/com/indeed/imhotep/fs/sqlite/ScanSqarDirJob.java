package com.indeed.imhotep.fs.sqlite;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteJob;
import com.almworks.sqlite4java.SQLiteStatement;
import com.indeed.imhotep.fs.RemoteFileStore;
import com.indeed.imhotep.fs.SqarManager;

import java.util.ArrayList;

/**
 * Created by darren on 12/10/15.
 */
public class ScanSqarDirJob extends SQLiteJob<ArrayList<RemoteFileStore.RemoteFileInfo>> {
    private static final String SELECT_FILE_NAMES_WITH_PREFIX =
            "select substr(name, ?3), type from file_name_to_id "
            + "where sqar_id = ?1 AND instr(name, ?2) = 1 "
            + "AND instr(substr(name, ?3), '" + SqarManager.DELIMITER + "')= 0;";
    private static final String SELECT_FILE_NAMES_NO_PREFIX =
            "select name, type from file_name_to_id " +
            "where sqar_id = ?1;";

    private final int sqarId;
    private final String prefix;
    private final boolean noPrefix;

    public ScanSqarDirJob(int sqarId, String prefix) {
        this.sqarId = sqarId;
        this.noPrefix = prefix == null || prefix.isEmpty();

        if (this.noPrefix) {
            this.prefix = null;
            return;
        }

        if (prefix.charAt(prefix.length() - 1) == SqarManager.DELIMITER) {
            this.prefix = prefix;
        } else {
            this.prefix = prefix + SqarManager.DELIMITER;
        }
    }

    @Override
    protected ArrayList<RemoteFileStore.RemoteFileInfo> job(SQLiteConnection connection) throws Throwable {
        final SQLiteStatement fileNamesSelectStatement;
        final ArrayList<RemoteFileStore.RemoteFileInfo> results;

        if (noPrefix) {
            fileNamesSelectStatement = connection.prepare(SELECT_FILE_NAMES_NO_PREFIX, true);
        } else {
            fileNamesSelectStatement = connection.prepare(SELECT_FILE_NAMES_WITH_PREFIX, true);
        }

        try {
            /* bind sqar id and prefix */
            fileNamesSelectStatement.bind(1, sqarId);
            if (!noPrefix) {
                fileNamesSelectStatement.bind(2, prefix);
                fileNamesSelectStatement.bind(3, prefix.length() + 1);
            }

            results = processFilenames(fileNamesSelectStatement);
        } finally {
            fileNamesSelectStatement.dispose();
        }

        return results;
    }

    private ArrayList<RemoteFileStore.RemoteFileInfo>
    processFilenames(SQLiteStatement fileNamesSelectStatement) throws SQLiteException {
        final ArrayList<RemoteFileStore.RemoteFileInfo> results = new ArrayList<>(1024);

        while (fileNamesSelectStatement.step()) {
            final String path = fileNamesSelectStatement.columnString(0);
            final int isFile = fileNamesSelectStatement.columnInt(1);

            results.add(new RemoteFileStore.RemoteFileInfo(path, -1, isFile == 1));
        }

        return results;
    }

}
