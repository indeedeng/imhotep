package com.indeed.imhotep.shardmaster.utils;

import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;

import javax.annotation.Nonnull;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author kornerup
 */

public class SQLWriteManager implements Runnable{
    private final Queue<Runnable> sqlStatementQueue;
    private static final Logger LOGGER = Logger.getLogger(SQLWriteManager.class);
    private int failedAttempts = 0;

    public SQLWriteManager() {
        sqlStatementQueue = new ConcurrentLinkedQueue<>();
    }

    public void addStatementToQueue(@Nonnull final Runnable runnable) {
        sqlStatementQueue.add(runnable);
    }

    public synchronized void run() {
        try {
            while (!sqlStatementQueue.isEmpty()) {
                sqlStatementQueue.peek().run();
                sqlStatementQueue.remove();
                failedAttempts = 0;
            }
        } catch (final DuplicateKeyException e) {
            // We can ignore requests that produce Duplicate entry exceptions
            sqlStatementQueue.remove();
        } catch (final DataAccessException e) {
            LOGGER.error("Could not execute SQL statement. This is try number: " + ++failedAttempts, e);
        }
    }
}
