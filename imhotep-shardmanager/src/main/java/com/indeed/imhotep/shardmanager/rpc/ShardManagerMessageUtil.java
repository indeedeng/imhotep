package com.indeed.imhotep.shardmanager.rpc;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.protobuf.Message;
import com.indeed.imhotep.io.Bytes;
import com.indeed.imhotep.shardmanager.protobuf.ShardManagerRequest;
import com.indeed.imhotep.shardmanager.protobuf.ShardManagerResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author kenh
 */

class ShardManagerMessageUtil {
    private ShardManagerMessageUtil() {
    }

    static void sendMessage(final Message message, final OutputStream os) throws IOException {
        os.write(Ints.toByteArray(message.getSerializedSize()));
        message.writeTo(os);
        os.flush();
    }

    static ShardManagerRequest receiveRequest(final InputStream is) throws IOException {
        return ShardManagerRequest.parseFrom(readPayloadStream(is));
    }

    static ShardManagerResponse receiveResponse(final InputStream is) throws IOException {
        return ShardManagerResponse.parseFrom(readPayloadStream(is));
    }

    private static InputStream readPayloadStream(final InputStream is) throws IOException {
        final byte[] payloadLengthBytes = new byte[4];
        ByteStreams.readFully(is, payloadLengthBytes);
        final int payloadLength = Bytes.bytesToInt(payloadLengthBytes);
        return ByteStreams.limit(is, payloadLength);
    }
}
