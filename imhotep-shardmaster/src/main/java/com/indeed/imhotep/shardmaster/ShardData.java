package com.indeed.imhotep.shardmaster;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.shardmaster.utils.IntervalTree;
import org.joda.time.Interval;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author kornerup
 */

public class ShardData {

    final private Map<String, IntervalTree<Long, ShardInfo>> tblShards;
    final private Map<String, TableFields> tblFields;
    final private Map<String, ShardInfo> pathsToShards;


    public boolean hasField(String dataset, String field) {
        return tblFields.containsKey(dataset) && tblFields.get(dataset).lastUpdatedTimestamp.containsKey(field);
    }

    public long getFieldUpdateTime(String dataset, String field) {
        final TableFields tableFields = tblFields.get(dataset);
        return tableFields != null ? tableFields.lastUpdatedTimestamp.get(field) : 0;
    }

    public List<String> getFields(String dataset, FieldType type) {
        final List<String> fields = new ArrayList<>();
        final TableFields tableFields = tblFields.get(dataset);
        if(tableFields == null) {
            return new ArrayList<>();
        }
        tableFields.fieldNameToFieldType.forEach((name, thisType) -> {
            if(thisType == type) {
                fields.add(name);
            }
        });
        return fields;
    }

    public int getNumDocs(String path) {
        if(!pathsToShards.containsKey(path)) {
            return -1;
        }
        return pathsToShards.get(path).numDocs;
    }

    // TODO: use this to figure out if a shard has been deleted
    public Set<String> getCopyOfAllPaths() {
        final ConcurrentHashMap.KeySetView<String, Boolean> set = ConcurrentHashMap.newKeySet(pathsToShards.keySet().size());
        set.addAll(pathsToShards.keySet());
        return set;
    }

    enum FieldType {
        INT(0), STRING(1);

        private int value;

        public int getValue(){
            return value;
        }

        static FieldType getType(int value) {
            switch (value) {
                case 0:
                    return INT;
                case 1:
                    return STRING;
            }
            return null;
        }

        FieldType(int value){
            this.value = value;
        }
    }

    class TableFields {
        Map<String, FieldType> fieldNameToFieldType;
        Map<String, Long> lastUpdatedTimestamp;
        public TableFields(){
            fieldNameToFieldType = new ConcurrentHashMap<>();
            lastUpdatedTimestamp = new ConcurrentHashMap<>();
        }
    }

    ShardData() {
        tblShards = new ConcurrentHashMap<>();
        tblFields = new ConcurrentHashMap<>();
        pathsToShards = new ConcurrentHashMap<>();
    }

    public void addShardFromHDFS(FlamdexMetadata metadata, Path shardPath, ShardDir shardDir) {
        addShardToDatastructure(metadata, shardPath, shardDir);
    }

    public void addTableFieldsRowsFromSQL(ResultSet rows) throws SQLException {
        if (rows.first()) {
            do {
                String dataset = rows.getString("dataset");
                String fieldName = rows.getString("fieldname");
                FieldType type = FieldType.getType(rows.getInt("type"));
                long dateTime = rows.getLong("lastshardstarttime");
                if (!tblFields.containsKey(dataset)) {
                    tblFields.put(dataset, new TableFields());
                }
                tblFields.get(dataset).lastUpdatedTimestamp.put(fieldName, dateTime);
                tblFields.get(dataset).fieldNameToFieldType.put(fieldName, type);
            } while (rows.next());
        }
    }

    public void addTableShardsRowsFromSQL(ResultSet rows) throws SQLException {
        if (rows.first()) {
            do {
                String strPath = rows.getString("path");
                int numDocs = rows.getInt("numDocs");

                if(pathsToShards.containsKey(strPath)) {
                    continue;
                }

                Path path = Paths.get(strPath);
                ShardDir shardDir = new ShardDir(path);
                String dataset = shardDir.getDataset();
                String shardname = shardDir.getId();
                ShardInfo shard = new ShardInfo(shardname, numDocs, shardDir.getVersion());
                final Interval interval = ShardTimeUtils.parseInterval(shardname);

                pathsToShards.put(strPath, shard);

                if (!tblShards.containsKey(dataset)) {
                    tblShards.put(dataset, new IntervalTree<>());
                }

                tblShards.get(dataset).addInterval(interval.getStart().getMillis(), interval.getEnd().getMillis(), shard);
            } while (rows.next());
        }
    }

    private void addShardToDatastructure(FlamdexMetadata metadata, Path shardPath, ShardDir shardDir) {
        ShardInfo info = new ShardInfo(shardDir.getId(), metadata.getNumDocs(), shardDir.getVersion());
        pathsToShards.put(shardDir.getIndexDir().toString(), info);
        String dataset = shardDir.getDataset();
        if(!tblShards.containsKey(dataset)) {
            tblShards.put(dataset, new IntervalTree<>());
            tblFields.put(dataset, new TableFields());
        }
        final Interval interval = ShardTimeUtils.parseInterval(shardDir.getId());
        tblShards.get(dataset).addInterval(interval.getStart().getMillis(), interval.getEnd().getMillis(), info);

        for (String field : metadata.getIntFields()) {
            if (!tblFields.get(dataset).lastUpdatedTimestamp.containsKey(field) || tblFields.get(dataset).lastUpdatedTimestamp.get(field) < interval.getStartMillis()) {
                tblFields.get(dataset).fieldNameToFieldType.put(field, FieldType.INT);
                tblFields.get(dataset).lastUpdatedTimestamp.put(field, interval.getStartMillis());
            }
        }

        for (String field : metadata.getStringFields()) {
            if (!tblFields.get(dataset).lastUpdatedTimestamp.containsKey(field) || tblFields.get(dataset).lastUpdatedTimestamp.get(field) < interval.getStartMillis()) {
                tblFields.get(dataset).fieldNameToFieldType.put(field, FieldType.STRING);
                tblFields.get(dataset).lastUpdatedTimestamp.put(field, interval.getStartMillis());
            }
        }
    }

    public boolean hasShard(String path){
        return pathsToShards.containsKey(path);
    }

    public Collection<String> getDatasets(){
        return tblShards.keySet();
    }


    // NOTE: this is a bit hacky to get a linear runtime
    public Collection<ShardInfo> getShardsForDataset(String dataset) {
        IntervalTree<Long, ShardInfo> tree = tblShards.get(dataset);
        if (tree == null) {
            return new HashSet<>();
        }
        Map<String, ShardInfo> shardToShardWithVersion = new HashMap<>();
        tree.getAllValues().forEach(shard -> {
            if(!shardToShardWithVersion.containsKey(shard.shardId) || shardToShardWithVersion.get(shard.shardId).getVersion() < shard.getVersion()) {
                shardToShardWithVersion.put(shard.shardId, shard);
            }
        });
        return shardToShardWithVersion.values();
    }

    public Collection<ShardInfo> getShardsInTime(String dataset, long start, long end) {
        return tblShards.get(dataset).getValuesInRange(start, end);
    }
}
