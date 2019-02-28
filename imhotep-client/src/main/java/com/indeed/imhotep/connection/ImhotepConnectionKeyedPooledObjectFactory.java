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
import java.util.concurrent.TimeUnit;

/**
 * @author xweng
 */
public class ImhotepConnectionKeyedPooledObjectFactory implements KeyedPooledObjectFactory<Host, ImhotepConnection> {
    private static final Logger logger = Logger.getLogger(ImhotepConnectionKeyedPooledObjectFactory.class);

    private static final int DEFAULT_SOCKET_TIMEOUT = (int) TimeUnit.MINUTES.toMillis(30);

    // keyedObjectPool doesn't handle the timeout during makeObject, we have to specify it in case of connection block
    private static final int DEFAULT_SOCKET_CONNECT_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(30);

    @Override
    public PooledObject<ImhotepConnection> makeObject(final Host host) throws IOException {
        final Socket socket = new Socket();
        final SocketAddress endpoint = new InetSocketAddress(host.getHostname(), host.getPort());
        socket.connect(endpoint, DEFAULT_SOCKET_CONNECT_TIMEOUT);

        socket.setSoTimeout(DEFAULT_SOCKET_TIMEOUT);
        socket.setReceiveBufferSize(65536);
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);

        return new DefaultPooledObject<>(new ImhotepConnection(socket, host));
    }

    @Override
    public void destroyObject(final Host host, final PooledObject<ImhotepConnection> pooledObject) {
        final ImhotepConnection connection = pooledObject.getObject();
        Closeables2.closeQuietly(connection.getSocket(), logger);
    }

    @Override
    public boolean validateObject(final Host host, final PooledObject<ImhotepConnection> pooledObject) { return true; }

    @Override
    public void activateObject(final Host host, final PooledObject<ImhotepConnection> pooledObject) { }

    @Override
    public void passivateObject(final Host host, final PooledObject<ImhotepConnection> pooledObject) { }
}
