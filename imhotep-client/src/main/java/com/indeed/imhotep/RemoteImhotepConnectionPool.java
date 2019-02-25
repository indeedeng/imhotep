package com.indeed.imhotep;

import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.indeed.imhotep.client.Host;

import java.io.IOException;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author xweng
 */
public class RemoteImhotepConnectionPool implements ImhotepConnectionPool{
    private static final int DEFAULT_POOL_SIZE = 8;
    private static final int DEFAULT_SOCKET_TIMEOUT = (int)TimeUnit.MINUTES.toMillis(10);

    private final int poolSize;
    private final Host host;

    private int created;
    private final BlockingQueue<Socket> availableConnections;

    // totally control of sockets
    // in case of repeated release and IO leak caused by users forget to release
    private final Set<Socket> occupiedConnections;

    public RemoteImhotepConnectionPool(final Host host) {
        this(host, DEFAULT_POOL_SIZE);
    }

    public RemoteImhotepConnectionPool(final Host host, final int poolSize) {
        this.host = host;
        this.poolSize = poolSize;

        created = 0;
        availableConnections = Queues.newArrayBlockingQueue(poolSize);
        occupiedConnections = Sets.newConcurrentHashSet();
    }

    @Override
    public Socket getConnection() throws InterruptedException, IOException {
        if (!availableConnections.isEmpty()) {
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
        final Socket socket = availableConnections.take();
        occupiedConnections.add(socket);
        return socket;
    }

    @Override
    public boolean releaseConnection(final Socket socket) {
        if (!occupiedConnections.contains(socket)) {
            return false;
        }

        occupiedConnections.remove(socket);
        final boolean result = availableConnections.offer(socket);
        if (!result) {
            occupiedConnections.add(socket);
            return false;
        }
        return true;
    }


    @Override
    public void close() throws IOException {
        for (Socket socket : availableConnections) {
            socket.close();
        }
        for (Socket socket : occupiedConnections) {
            socket.close();
        }
    }

    private Socket createConnection() throws IOException {
        final Socket socket = new Socket(host.hostname, host.port);
        socket.setSoTimeout(DEFAULT_SOCKET_TIMEOUT);
        socket.setReceiveBufferSize(65536);
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
        created++;
        return socket;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public Host getHost() {
        return host;
    }
}
