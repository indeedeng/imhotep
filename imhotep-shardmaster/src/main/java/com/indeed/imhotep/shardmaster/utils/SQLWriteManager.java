package com.indeed.imhotep.shardmaster.utils;

import com.sun.istack.NotNull;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author kornerup
 */

public class SQLWriteManager implements Runnable{
    private final Queue<PreparedStatement> sqlStatementQueue;

    public SQLWriteManager() {
        sqlStatementQueue = new ConcurrentLinkedQueue<>();
    }

    public void addStatementToQueue(@NotNull PreparedStatement statement) {
        sqlStatementQueue.add(statement);
    }

    public synchronized void run() {
        try {
            while (!sqlStatementQueue.isEmpty()) {
                sqlStatementQueue.peek().executeBatch();
                sqlStatementQueue.remove();
            }
        } catch (SQLException e) {
            // We can ignore requests that produce Duplicate entry exceptions
            System.out.println(sqlStatementQueue.peek());
            e.printStackTrace();
        }
    }

    public boolean isQueueEmpty() {
        return sqlStatementQueue.isEmpty();
    }
}
