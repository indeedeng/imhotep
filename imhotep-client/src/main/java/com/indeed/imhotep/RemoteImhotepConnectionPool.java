package com.indeed.imhotep;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.indeed.imhotep.client.Host;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author xweng
 */
public class RemoteImhotepConnectionPool implements ImhotepConnectionPool{
    private static final Logger logger = Logger.getLogger(RemoteImhotepConnectionPool.class);
    private static final int DEFAULT_MAX_POOL_SIZE = 32;
    private static final int DEFAULT_SOCKET_TIMEOUT = (int)TimeUnit.MINUTES.toMillis(30);

    private final int maxPoolSize;
    private final Host host;

    private final AtomicInteger socketCount;
    private final BlockingQueue<Socket> availableSockets;
    // whole control of sockets in case of incorrect usage of release and discard
    private final Set<Socket> occupiedSockets;

    public RemoteImhotepConnectionPool(final Host host) {
        this(host, DEFAULT_MAX_POOL_SIZE);
    }

    public RemoteImhotepConnectionPool(final Host host, final int maxPoolSize) {
        this.host = host;
        this.maxPoolSize = maxPoolSize;

        socketCount = new AtomicInteger(0);
        availableSockets = Queues.newArrayBlockingQueue(maxPoolSize);
        occupiedSockets = Sets.newConcurrentHashSet();
    }

    @Override
    public ImhotepConnection getConnection(final int millisecondTimeout) throws InterruptedException, IOException, TimeoutException {
        final Socket socket = internalGetSocket(millisecondTimeout);
        if (socket == null) {
            throw new TimeoutException("Errors when waiting for dequeue");
        }
        return wrapSocket(socket);
    }

    @Override
    public ImhotepConnection getConnection() throws InterruptedException, IOException {
        return wrapSocket(internalGetSocket(0));
    }

    private Socket internalGetSocket(final int millisecondTimeout) throws InterruptedException, IOException {
        final boolean hasTimeout = millisecondTimeout <= 0 ? false : true;
        Socket socket = availableSockets.poll();
        if (socket != null) {
            return socket;
        }

        if (socketCount.get() < maxPoolSize) {
            synchronized (this) {
                if (socketCount.get() < maxPoolSize) {
                    socket = createConnection(millisecondTimeout);
                    socketCount.incrementAndGet();
                }
            }
        }

        if (socket == null) {
            socket = hasTimeout ?
                    availableSockets.poll(millisecondTimeout, TimeUnit.MILLISECONDS) :
                    availableSockets.take();
        }
        return socket;
    }

    private ImhotepConnection wrapSocket(final Socket socket) {
        occupiedSockets.add(socket);
        return new ImhotepConnection(this, socket);
    }

    /**
     * Discard a connection from pool and close it silently without any exceptions
     * If the connection is not from the pool, just close it directly
     * @param connection the bad connection
     */
    @Override
    public void discardConnection(final ImhotepConnection connection) {
        final Socket socket = connection.getSocket();
        if (occupiedSockets.remove(socket)) {
            // only decrease the count if the connection belongs to the pool
            socketCount.decrementAndGet();
        }

        try {
            socket.close();
        } catch (final IOException e) {
            logger.warn("Errors happened when closing socket " + socket);
        }
    }

    @Override
    public void releaseConnection(final ImhotepConnection connection) {
        final Socket socket = connection.getSocket();
        if (occupiedSockets.remove(socket)) {
            availableSockets.offer(socket);
        }
    }

    @Override
    public void close() throws IOException {
        final List<Socket> allSockets = Stream.of(availableSockets, occupiedSockets).
                flatMap(Collection::stream).
                collect(Collectors.toList());
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

    @Override
    @VisibleForTesting
    public int getConnectionCount() {
        return availableSockets.size() + occupiedSockets.size();
    }
}
