package com.indeed.imhotep.shardmanager.rpc;

import com.indeed.imhotep.shardmanager.protobuf.ShardManagerRequest;
import com.indeed.imhotep.shardmanager.protobuf.ShardManagerResponse;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author kenh
 */

public class RequestResponseServer implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(RequestResponseServer.class);

    private final ServerSocket serverSocket;
    private final RequestHandler requestHandler;

    public RequestResponseServer(final int port, final RequestHandler requestHandler) throws IOException {
        this.requestHandler = requestHandler;
        serverSocket = new ServerSocket(port);
    }

    public int getActualPort() {
        if (serverSocket.isClosed()) {
            return 0;
        } else {
            return serverSocket.getLocalPort();
        }
    }

    @Override
    public void close() throws IOException {
        serverSocket.close();
    }

    public void run() throws IOException {
        while (!serverSocket.isClosed()) {
            final Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (final IOException e) {
                if ("Socket closed".equals(e.getMessage())) {
                    LOGGER.warn("Shutting down due to server socket closed");
                    break;
                }
                throw e;
            }

            try {
                socket.setTcpNoDelay(true); // disable nagle
                final ShardManagerRequest request = ShardManagerMessageUtil.receiveRequest(socket.getInputStream());

                ShardManagerResponse response;
                try {
                    response = requestHandler.handleRequest(request);
                } catch (final Throwable e) {
                    LOGGER.error("Failed to handle request " + request, e);
                    response = ShardManagerResponse.newBuilder()
                            .setResponseCode(ShardManagerResponse.ResponseCode.ERROR)
                            .setErrorMessage(e.getMessage())
                            .build();
                }
                ShardManagerMessageUtil.sendMessage(response, socket.getOutputStream());
            } finally {
                socket.close();
            }
        }
    }
}
