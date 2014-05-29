package com.indeed.imhotep.tools;

import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.ShardInfo;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class ShardCleanup {
    public static void main(String[] args) throws IOException {
        final int numMachines = 18;
        final int numDrives = 8;
        PrintWriter symlinkDeleteScript = new PrintWriter("/tmp/del_imo_symlinks.sh");
        PrintWriter directoryDeleteScript = new PrintWriter("/tmp/del_imo_dirs.sh");
        Map<String, List<ShardInfo>> datasets = new HashMap<String, List<ShardInfo>>();

        for (int i = 1; i <= numMachines; i++) {
            List<DatasetInfo> shards = ImhotepRemoteSession.getShardInfoList(String.format("aus-imo%02d.indeed.net", i), 12345);
            System.out.println("read "+shards.size()+ " from imo "+i);
            for (DatasetInfo ds : shards) {
                List<ShardInfo> temp = datasets.get(ds.getDataset());
                if (temp == null) {
                    temp = new ArrayList<ShardInfo>();
                    datasets.put(ds.getDataset(), temp);
                }
                temp.addAll(ds.getShardList());
            }
        }
        for (Map.Entry<String, List<ShardInfo>> dataset : datasets.entrySet()) {
            Map<String, Map<Long, Integer>> versionMaps = new HashMap<String, Map<Long, Integer>>();
            for (ShardInfo shard : dataset.getValue()) {
                Map<Long, Integer> versionMap = versionMaps.get(shard.getShardId());
                if (versionMap == null) {
                    versionMap = new HashMap<Long, Integer>();
                    versionMaps.put(shard.getShardId(), versionMap);
                }
                Integer temp = versionMap.get(shard.getVersion());
                if (temp == null) {
                    temp = 0;
                }
                versionMap.put(shard.getVersion(), temp+1);
            }
            for (Map.Entry<String, Map<Long, Integer>> shards : versionMaps.entrySet()) {
                long maxRepresentedVersion = -1;
                int machineCount = 0;
                for (Map.Entry<Long, Integer> svPair : shards.getValue().entrySet()) {
                    if (svPair.getValue() >= 2) {
                        maxRepresentedVersion = Math.max(maxRepresentedVersion, svPair.getKey());
                        machineCount = svPair.getValue();
                    }
                }
                for (Map.Entry<Long, Integer> svPair : shards.getValue().entrySet()) {
                    if (svPair.getKey() < maxRepresentedVersion) {
                        final String filename = dataset.getKey()+"/"+shards.getKey()+"."+svPair.getKey();
                        symlinkDeleteScript.println("# Because we have version "+maxRepresentedVersion+" on "+machineCount+" other machines");
                        symlinkDeleteScript.println("rm /home/imhotep/shards/"+filename);
                        directoryDeleteScript.println("# Because we have version "+maxRepresentedVersion+" on "+machineCount+" other machines");
                        for (int i = 1; i <= numDrives; i++) {
                            directoryDeleteScript.println("rm -r /imhotep/"+String.format("%02d", i)+"/"+filename);
                        }
                    }
                }
            }
        }
        symlinkDeleteScript.close();
        directoryDeleteScript.close();
    }
}
