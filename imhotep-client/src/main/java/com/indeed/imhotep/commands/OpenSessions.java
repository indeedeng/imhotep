package com.indeed.imhotep.commands;

import com.google.common.base.Preconditions;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.RemoteImhotepMultiSession;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.api.CommandSerializationParameters;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.io.RequestTools;
import com.indeed.imhotep.protobuf.HostAndPort;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ShardBasicInfoMessage;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.indeed.imhotep.ImhotepRemoteSession.CURRENT_CLIENT_VERSION;

@EqualsAndHashCode(callSuper = true)
@ToString
public class OpenSessions extends VoidAbstractImhotepCommand {
    public final Map<Host, List<Shard>> hostToShards;

    public final OpenSessionData openSessionData;

    public final int socketTimeout;
    public final long localTempFileSizeLimit;
    @Nullable
    private final AtomicLong localTempFileSizeBytesLeft;
    public final boolean allowSessionForwarding;
    public final boolean p2pCache;

    public OpenSessions(
            final Map<Host, List<Shard>> hostToShards,
            final OpenSessionData openSessionData,
            final int socketTimeout,
            final long localTempFileSizeLimit,
            final boolean allowSessionForwarding,
            final boolean p2pCache
    ) {
        super(UUID.randomUUID().toString(), Collections.emptyList(), Collections.singletonList(ImhotepSession.DEFAULT_GROUPS));
        this.hostToShards = hostToShards;
        this.openSessionData = openSessionData;
        this.socketTimeout = socketTimeout;
        this.localTempFileSizeLimit = localTempFileSizeLimit;
        this.localTempFileSizeBytesLeft = (localTempFileSizeLimit > 0) ? new AtomicLong(localTempFileSizeLimit) : null;
        Preconditions.checkArgument(!allowSessionForwarding, "Batch OpenSessions does not currently support session forwarding");
        this.allowSessionForwarding = allowSessionForwarding;
        this.p2pCache = p2pCache;
    }


    @Override
    protected RequestTools.ImhotepRequestSender imhotepRequestSenderInitializer() {
        throw new IllegalStateException("Shouldn't be calling this...");
    }

    @Override
    public void writeToOutputStream(
            final OutputStream os,
            final CommandSerializationParameters serializationParameters
    ) throws IOException {
        final List<Shard> shards = hostToShards.get(serializationParameters.getHostAndPort());
        final ImhotepRequest.Builder request = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.OPEN_SESSION)
                .addAllShards(shards.stream().map(shard -> {
                            final ShardBasicInfoMessage.Builder builder = ShardBasicInfoMessage.newBuilder()
                                    .setShardName(shard.getFileName())
                                    .setNumDocs(shard.numDocs);
                            if (p2pCache) {
                                builder.setShardOwner(
                                        HostAndPort.newBuilder()
                                                .setHost(shard.getOwner().getHostname())
                                                .setPort(shard.getOwner().getPort())
                                                .build()
                                );
                            }
                            return builder.build();
                        }
                ).collect(Collectors.toList()))
                .setClientVersion(CURRENT_CLIENT_VERSION)
                .setSessionId(sessionId)
                .setAllowSessionForwarding(allowSessionForwarding);
        openSessionData.writeToImhotepRequest(request);
        new RequestTools.ImhotepRequestSender.Simple(request.build()).writeToStreamNoFlush(os);
    }

    @Override
    public void applyVoid(final ImhotepSession imhotepSession) {
        throw new IllegalStateException("Should not call apply() on OpenSessions");
    }

    public RemoteImhotepMultiSession makeSession() {
        final ImhotepRemoteSession[] remoteSessions = new ImhotepRemoteSession[hostToShards.size()];
        final InetSocketAddress[] nodes = new InetSocketAddress[hostToShards.size()];
        int index = 0;
        for (final Map.Entry<Host, List<Shard>> entry : hostToShards.entrySet()) {
            final Host host = entry.getKey();
            final int numDocs = entry.getValue().stream().mapToInt(Shard::getNumDocs).sum();
            remoteSessions[index] = new ImhotepRemoteSession(host.hostname, host.port, sessionId, localTempFileSizeBytesLeft, socketTimeout, numDocs);
            nodes[index] = new InetSocketAddress(host.hostname, host.port);
            index += 1;
        }
        return new RemoteImhotepMultiSession(
                remoteSessions,
                sessionId,
                nodes,
                localTempFileSizeLimit,
                localTempFileSizeBytesLeft,
                openSessionData.getUsername(),
                openSessionData.getClientName(),
                openSessionData.getPriority()
        );
    }
}
