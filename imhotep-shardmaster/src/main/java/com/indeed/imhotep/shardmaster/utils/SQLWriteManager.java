package com.indeed.imhotep.shardmaster.utils;

import org.jooq.exception.DataAccessException;
import org.springframework.dao.DuplicateKeyException;

import javax.annotation.Nonnull;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author kornerup
 */

public class SQLWriteManager implements Runnable{
    private final Queue<Runnable> sqlStatementQueue;

    public SQLWriteManager() {
        sqlStatementQueue = new ConcurrentLinkedQueue<>();
    }

    public void addStatementToQueue(@Nonnull Runnable runnable) {
        sqlStatementQueue.add(runnable);
    }

    public synchronized void run() {
        try {
            while (!sqlStatementQueue.isEmpty()) {
                sqlStatementQueue.peek().run();
                sqlStatementQueue.remove();
            }
        } catch (DuplicateKeyException e) {
            // We can ignore requests that produce Duplicate entry exceptions
            sqlStatementQueue.remove();
        } catch (DataAccessException e) {
            e.printStackTrace();
        }
    }
}
