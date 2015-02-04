// TODO: seems unused. can we delete it?

///*
// * Copyright (C) 2014 Indeed Inc.
// *
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// * in compliance with the License. You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software distributed under the
// * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// * express or implied. See the License for the specific language governing permissions and
// * limitations under the License.
// */
// package com.indeed.imhotep.service;
//
//import com.google.common.base.Strings;
//import com.google.common.collect.Lists;
//import com.google.common.collect.Sets;
//import com.indeed.imhotep.DatasetInfo;
//import com.indeed.imhotep.ImhotepStatusDump;
//import com.indeed.imhotep.ShardInfo;
//import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
//import com.indeed.imhotep.api.ImhotepSession;
//import com.indeed.imhotep.client.Host;
//import com.indeed.imhotep.client.ImhotepClient;
//import org.apache.log4j.Logger;
//
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.UUID;
//
///**
// * @author jplaisance
// */
//public final class RemoteImhotepServiceCore extends AbstractImhotepServiceCore {
//
//    private static final Logger log = Logger.getLogger(RemoteImhotepServiceCore.class);
//
//    private final RemoteSessionManager sessionManager = new RemoteSessionManager();
//
//    private final ImhotepClient imhotepClient;
//
//    public RemoteImhotepServiceCore(ImhotepClient imhotepClient) {
//        this.imhotepClient = imhotepClient;
//    }
//
//    @Override
//    protected RemoteSessionManager getSessionManager() {
//        return sessionManager;
//    }
//
//    @Override
//    public List<ShardInfo> handleGetShardList() {
//        final Set<ShardInfo> ret = Sets.newHashSet();
//        final Map<Host,List<DatasetInfo>> hostToDatasets = imhotepClient.getShardList();
//        for (Map.Entry<Host, List<DatasetInfo>> entry : hostToDatasets.entrySet()) {
//            final List<DatasetInfo> datasets = entry.getValue();
//            for (DatasetInfo dataset : datasets) {
//                ret.addAll(dataset.getShardList());
//            }
//        }
//        return Lists.newArrayList(ret);
//    }
//
//    @Override
//    public List<DatasetInfo> handleGetDatasetList() {
//        final Map<String, DatasetInfo> datasetToShardList = imhotepClient.getDatasetToShardList();
//        return Lists.newArrayList(datasetToShardList.values());
//    }
//
//    @Override
//    public ImhotepStatusDump handleGetStatusDump() {
//        //the fields in ImhotepStatusDump don't make sense for RemoteImhotepServiceCore
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public List<String> getShardIdsForSession(final String sessionId) {
//        return getSessionManager().getShardIdsForSession(sessionId);
//    }
//
//    @Override
//    public String handleOpenSession(
//            final String dataset,
//            final List<String> shardRequestList,
//            final String username,
//            final String ipAddress,
//            final int clientVersion,
//            final int mergeThreadLimit,
//            final boolean optimizeGroupZeroLookups,
//            String sessionId,
//            long tempFileSizeLimit
//    ) throws ImhotepOutOfMemoryException {
//        if (Strings.isNullOrEmpty(sessionId)) sessionId = UUID.randomUUID().toString();
//        final
//        ImhotepSession
//                session = imhotepClient.sessionBuilder(dataset, null, null)
//                    .shardsOverride(shardRequestList)
//                    .mergeThreadLimit(mergeThreadLimit)
//                    .username(username)
//                    .optimizeGroupZeroLookups(optimizeGroupZeroLookups)
//                    .tempFileSizeLimit(tempFileSizeLimit).build();
//        sessionManager.addSession(sessionId, session, shardRequestList, username, ipAddress, clientVersion, dataset);
//        return sessionId;
//    }
//}
