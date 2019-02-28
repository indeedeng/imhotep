package com.indeed.imhotep;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.indeed.imhotep.client.Host;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author xweng
 */
public class RemoteImhotepConnectionPool implements ImhotepConnectionPool {
    private static final Logger logger = Logger.getLogger(RemoteImhotepConnectionPool.class);

    private static final int INITIAL_POOL_SIZE = 32;
    private static final int DEFAULT_SOCKET_TIMEOUT = (int) TimeUnit.MINUTES.toMillis(30);
    private static final int DEFAULT_IDLE_SOCKET_LIVE_TIME = (int) TimeUnit.MINUTES.toMillis(5);

    private final Host host;

    private final BlockingQueue<SocketTimePair> availableSockets;
    // whole control of sockets in case of incorrect usage of release and discard
    private final Set<Socket> occupiedSockets;
    // the live time for idle sockets in milliseconds
    private final int idleSocketLiveTime;

    public RemoteImhotepConnectionPool(final Host host, final int idleSocketLiveTime) {
        this.host = host;
        this.idleSocketLiveTime = idleSocketLiveTime;

        availableSockets = Queues.newArrayBlockingQueue(INITIAL_POOL_SIZE);
        occupiedSockets = Sets.newConcurrentHashSet();
    }

    public RemoteImhotepConnectionPool(final Host host) {
        this(host, DEFAULT_IDLE_SOCKET_LIVE_TIME);
    }

    @Override
    public ImhotepConnection getConnection(final int millisecondTimeout) throws IOException {
        return internalGetConnection(millisecondTimeout);
    }

    @Override
    public ImhotepConnection getConnection() throws IOException {
        return internalGetConnection(0);
    }

    private ImhotepConnection internalGetConnection(final int millisecondTimeout) throws IOException {
        Socket socket = cleanUpAndGetSocket();
        if (socket == null) {
            socket = createConnection(millisecondTimeout);
        }

        occupiedSockets.add(socket);
        return new ImhotepConnection(this, socket);
    }

    // TODO: optimize the case that dequeue all sockets and then create a new socket
    private @Nullable Socket cleanUpAndGetSocket() {
        final long idleReleaseTime = System.currentTimeMillis() - idleSocketLiveTime;
        SocketTimePair pair;
        while ((pair = availableSockets.poll()) != null) {
            if (pair.getLastReleaseTime() > idleReleaseTime) {
                return pair.getSocket();
            }
            discardSocket(pair.getSocket());
        }
        return null;
    }

    /**
     * Discard a connection from pool and close it silently without any exceptions
     * If the connection is not from the pool, just close it directly
     *
     * @param connection the bad connection
     */
    @Override
    public void discardConnection(final ImhotepConnection connection) {
        discardSocket(connection.getSocket());
    }

    private void discardSocket(final Socket socket) {
        occupiedSockets.remove(socket);

        Closeables2.closeQuietly(socket, logger);
    }

    @Override
    public void releaseConnection(final ImhotepConnection connection) {
        final Socket socket = connection.getSocket();
        if (occupiedSockets.remove(socket)) {
            availableSockets.offer(SocketTimePair.of(socket, System.currentTimeMillis()));
        }
    }

    @Override
    public void close() throws IOException {
        if (!availableSockets.isEmpty()) {
            logger.warn("Closing " + availableSockets.size() + " sockets that are in use");
        }
        final List<Socket> allSockets = Stream.concat(
                availableSockets.stream().map(pair -> pair.getSocket()),
                occupiedSockets.stream()
        ).collect(Collectors.toList());

        Closeables2.closeAll(logger, allSockets);
    }

    private Socket createConnection(final int millisecondsTimeout) throws IOException {
        final Socket socket = new Socket();
        final SocketAddress endpoint = new InetSocketAddress(host.getHostname(), host.getPort());
        // it means no timeout if millisecondsTimeout is 0
        socket.connect(endpoint, millisecondsTimeout);
        logger.info("create a new socket " + socket);

        socket.setSoTimeout(DEFAULT_SOCKET_TIMEOUT);
        socket.setReceiveBufferSize(65536);
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);

        return socket;
    }

    public Host getHost() {
        return host;
    }

    /**
     * it's not a thread-safe method and only for test
     */
    @Override
    @VisibleForTesting
    public int getConnectionCount() {
        return availableSockets.size() + occupiedSockets.size();
    }

    private static class SocketTimePair {
        private Socket socket;
        private long lastReleaseTime;

        private SocketTimePair(final Socket socket, final long lastReleaseTime) {
            this.socket = socket;
            this.lastReleaseTime = lastReleaseTime;
        }

        public static SocketTimePair of(final Socket socket, final long lastTimestamp) {
            return new SocketTimePair(socket, lastTimestamp);
        }

        public Socket getSocket() {
            return socket;
        }

        public long getLastReleaseTime() {
            return lastReleaseTime;
        }
    }
}
