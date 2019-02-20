package com.indeed.imhotep;

import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.indeed.imhotep.client.Host;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

/**
 * @author xweng
 */
public class RemoteImhotepConnectionPool implements ImhotepConnectionPool{
    private static final Logger log = Logger.getLogger(ImhotepConnectionPool.class);
    private static final int DEFAULT_POOL_SIZE = 8;

    private int poolSize;
    private Host host;

    private int created;
    private BlockingQueue<Socket> avaliableConnections;

    // totally control of sockets
    // in case of the repeated release and IO leak caused by users forget to release
    private Set<Socket> occupiedConnections;

    public RemoteImhotepConnectionPool(final Host host) {
        this(host, DEFAULT_POOL_SIZE);
    }

    public RemoteImhotepConnectionPool(final Host host, final int poolSize) {
        this.host = host;
        this.poolSize = poolSize;

        created = 0;
        avaliableConnections = Queues.newArrayBlockingQueue(poolSize);
        occupiedConnections = Sets.newConcurrentHashSet();
    }

    @Override
    public Socket getConnection() throws InterruptedException, IOException {
        if (!avaliableConnections.isEmpty()) {
            return internalGet();
        }

        if (created < poolSize) {
            synchronized (this) {
                if (created < poolSize) {
                    final Socket socket = createConnection();
                    occupiedConnections.add(socket);
                    return socket;
                }
            }
        }
        return internalGet();
    }

    private Socket internalGet() throws InterruptedException {
        final Socket socket = avaliableConnections.take();
        occupiedConnections.add(socket);
        return socket;
    }

    // the capacity is always enough, no need to call Synchronized put method
    // TODO: might need something to avoid repeated release
    @Override
    public boolean releaseConnection(final Socket socket) {
        if (!occupiedConnections.contains(socket)) {
            return false;
        }

        occupiedConnections.remove(socket);
        final boolean result = avaliableConnections.offer(socket);
        if (!result) {
            occupiedConnections.remove(socket);
            return false;
        }
        return true;
    }


    @Override
    public void close() throws IOException {
        for (Socket socket : avaliableConnections) {
            socket.close();
        }
        for (Socket socket : occupiedConnections) {
            socket.close();
        }
    }

    private Socket createConnection() throws IOException {
        final Socket socket = new Socket(host.hostname, host.port);
        socket.setKeepAlive(true);
        created++;
        return socket;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(final int poolSize) {
        this.poolSize = poolSize;
    }

    public Host getHost() {
        return host;
    }

    public void setHost(final Host host) {
        this.host = host;
    }
}
