package com.indeed.imhotep.shardmaster.rpc;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.protobuf.Message;
import com.indeed.imhotep.io.Bytes;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterRequest;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author kenh
 */

class ShardMasterMessageUtil {
    private ShardMasterMessageUtil() {
    }

    static void sendMessage(final Message message, final OutputStream os) throws IOException {
        os.write(Ints.toByteArray(message.getSerializedSize()));
        message.writeTo(os);
        os.flush();
    }

    static ShardMasterRequest receiveRequest(final InputStream is) throws IOException {
        return ShardMasterRequest.parseFrom(readPayloadStream(is));
    }

    static ShardMasterResponse receiveResponse(final InputStream is) throws IOException {
        return ShardMasterResponse.parseFrom(readPayloadStream(is));
    }

    private static InputStream readPayloadStream(final InputStream is) throws IOException {
        final byte[] payloadLengthBytes = new byte[4];
        ByteStreams.readFully(is, payloadLengthBytes);
        final int payloadLength = Bytes.bytesToInt(payloadLengthBytes);
        return ByteStreams.limit(is, payloadLength);
    }
}
