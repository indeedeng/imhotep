package com.indeed.imhotep.commands;

import com.indeed.imhotep.protobuf.ImhotepRequest;
import lombok.Value;

@Value
public class OpenSessionData {
    String dataset;
    int mergeThreadLimit;
    String username;
    String clientName;
    byte priority;
    boolean optimizeGroupZeroLookups;
    long daemonTempFileSizeLimit;
    long sessionTimeout;
    boolean useFtgsPooledConnection;
    boolean executeBatchInParallel;

    public void writeToImhotepRequest(final ImhotepRequest.Builder request) {
        request
                .setDataset(dataset)
                .setMergeThreadLimit(mergeThreadLimit)
                .setUsername(username)
                .setClientName(clientName)
                .setSessionPriority(priority)
                .setOptimizeGroupZeroLookups(optimizeGroupZeroLookups)
                .setTempFileSizeLimit(daemonTempFileSizeLimit)
                .setSessionTimeout(sessionTimeout)
                .setUseFtgsPooledConnection(useFtgsPooledConnection)
                .setExecuteBatchInParallel(executeBatchInParallel);
    }

    public static OpenSessionData readFromImhotepRequest(final ImhotepRequest request) {
        return new OpenSessionData(
                request.getDataset(),
                request.getMergeThreadLimit(),
                request.getUsername(),
                request.getClientName(),
                (byte) request.getSessionPriority(),
                request.getOptimizeGroupZeroLookups(),
                request.getTempFileSizeLimit(),
                request.getSessionTimeout(),
                request.getUseFtgsPooledConnection(),
                request.getExecuteBatchInParallel()
        );
    }
}
