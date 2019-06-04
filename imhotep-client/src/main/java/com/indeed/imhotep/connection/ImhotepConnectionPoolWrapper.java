package com.indeed.imhotep.connection;

/**
 * @author xweng
 */
public class ImhotepConnectionPoolWrapper {
    public static final ImhotepConnectionPool INSTANCE = new ImhotepConnectionPool(new ImhotepConnectionPoolConfig());
}
