package com.indeed.imhotep.connection;

import com.indeed.imhotep.client.Host;
import com.indeed.util.core.io.Closeables2;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

/**
 * @author xweng
 */
public class ImhotepConnectionKeyedPooledObjectFactory implements KeyedPooledObjectFactory<Host, Socket> {
    private static final Logger logger = Logger.getLogger(ImhotepConnectionKeyedPooledObjectFactory.class);

    private final ImhotepConnectionPoolConfig poolConfig;

    public ImhotepConnectionKeyedPooledObjectFactory(final ImhotepConnectionPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
    }

    @Override
    public PooledObject<Socket> makeObject(final Host host) throws IOException {
        final Socket socket = new Socket();
        final SocketAddress endpoint = new InetSocketAddress(host.getHostname(), host.getPort());
        socket.connect(endpoint, poolConfig.getSocketConnectingTimeoutMills());

        socket.setSoTimeout(poolConfig.getSocketReadTimeoutMills());
        socket.setReceiveBufferSize(65536);
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);

        return new DefaultPooledObject<>(socket);
    }

    @Override
    public void destroyObject(final Host host, final PooledObject<Socket> pooledObject) {
        Closeables2.closeQuietly(pooledObject.getObject(), logger);
    }

    @Override
    public boolean validateObject(final Host host, final PooledObject<Socket> pooledObject) {
        if (pooledObject.getIdleTimeMillis() >= poolConfig.getSocketMaxIdleTimeMills()) {
            return false;
        }

        final Socket socket = pooledObject.getObject();
        if (!socket.isConnected() || socket.isClosed()) {
            return false;
        }
        try {
            // minimum check if there is remained data in the socket stream
            return socket.getInputStream().available() == 0;
        } catch (final Exception e) {
            return false;
        }
    }

    @Override
    public void activateObject(final Host host, final PooledObject<Socket> pooledObject) {
    }

    @Override
    public void passivateObject(final Host host, final PooledObject<Socket> pooledObject) {
    }
}
