package com.indeed.imhotep.fs.sqlite;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteJob;
import com.almworks.sqlite4java.SQLiteStatement;

/**
 * Created by darren on 12/10/15.
 */
public class LookupSqarIdJob extends SQLiteJob<Integer> {
    private static final String SELECT_SQAR_ID = "select id from sqar_names where name = ?;";

    private final String sqar;

    public LookupSqarIdJob(String sqar) {
        this.sqar = sqar;
    }

    @Override
    protected Integer job(SQLiteConnection connection) throws Throwable {
        SQLiteStatement sqarIdSelectStatement = connection.prepare(SELECT_SQAR_ID, true);

        try {
            sqarIdSelectStatement.bind(1, sqar);
            if (sqarIdSelectStatement.step()) {
                return sqarIdSelectStatement.columnInt(0);
            } else {
                return -1;
            }
        } finally {
            sqarIdSelectStatement.dispose();
        }
    }
}
