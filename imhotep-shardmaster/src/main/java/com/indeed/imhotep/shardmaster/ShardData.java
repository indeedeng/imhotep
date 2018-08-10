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
import java.util.stream.Collectors;

/**
 * @author kornerup
 */

public class ShardData {

    final private Map<String, IntervalTree<Long, ShardInfo>> tblShards;
    final private Map<String, TableFields> tblFields;
    final private Map<String, ShardInfo> pathsToShards;


    public boolean hasField(final String dataset, final String field) {
        return tblFields.containsKey(dataset) && tblFields.get(dataset).lastUpdatedTimestamp.containsKey(field);
    }

    public long getFieldUpdateTime(final String dataset, final String field) {
        final TableFields tableFields = tblFields.get(dataset);
        return tableFields != null ? tableFields.lastUpdatedTimestamp.get(field) : 0;
    }

    public List<String> getFields(final String dataset, final FieldType type) {
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

    public int getNumDocs(final String path) {
        if(!pathsToShards.containsKey(path)) {
            return -1;
        }
        return pathsToShards.get(path).numDocs;
    }

    public Set<String> getCopyOfAllPaths() {
        final ConcurrentHashMap.KeySetView<String, Boolean> set = ConcurrentHashMap.newKeySet(pathsToShards.keySet().size());
        set.addAll(pathsToShards.keySet());
        return set;
    }

    public void deleteShards(final Set<String> allPaths) {
        for(final String path: allPaths) {
            final ShardDir temp = new ShardDir(Paths.get(path));
            final Interval interval = ShardTimeUtils.parseInterval(temp.getId());
            tblShards.get(temp.getDataset()).deleteInterval(interval.getStart().getMillis(), interval.getEnd().getMillis(), pathsToShards.get(path));
            pathsToShards.remove(path);
        }
    }

    public List<String> deleteDatasetsWithoutShards() {
        List<String> datasets = tblFields.keySet().stream().filter(dataset -> (!tblShards.containsKey(dataset)) || tblShards.get(dataset).getAllValues().size() == 0).collect(Collectors.toList());
        for(String dataset: datasets) {
            tblShards.remove(dataset);
            tblFields.remove(dataset);
        }
        return datasets;
    }

    enum FieldType {
        INT, STRING;

        static FieldType getType(final String value) {
            switch (value) {
                case "INT":
                    return INT;
                case "STRING":
                    return STRING;
            }
            return null;
        }

    }

    class TableFields {
        final Map<String, FieldType> fieldNameToFieldType;
        final Map<String, Long> lastUpdatedTimestamp;
        public TableFields(){
            fieldNameToFieldType = new ConcurrentHashMap<>();
            lastUpdatedTimestamp = new ConcurrentHashMap<>();
        }
    }

    public ShardData() {
        tblShards = new ConcurrentHashMap<>();
        tblFields = new ConcurrentHashMap<>();
        pathsToShards = new ConcurrentHashMap<>();
    }

    public void addShardFromHDFS(final FlamdexMetadata metadata, final ShardDir shardDir) {
        addShardToDatastructure(metadata, shardDir);
    }

    public void addTableFieldsRowsFromSQL(final ResultSet rows) throws SQLException {
        if (rows.first()) {
            do {
                final String dataset = rows.getString("dataset");
                final String fieldName = rows.getString("fieldname");
                final FieldType type = FieldType.getType(rows.getString("type"));
                final long dateTime = rows.getLong("lastshardstarttime");
                if (!tblFields.containsKey(dataset)) {
                    tblFields.put(dataset, new TableFields());
                }
                tblFields.get(dataset).lastUpdatedTimestamp.put(fieldName, dateTime);
                tblFields.get(dataset).fieldNameToFieldType.put(fieldName, type);
            } while (rows.next());
        }
    }

    public void updateTableShardsRowsFromSQL(final ResultSet rows, boolean shouldDelete) throws SQLException {
        final Set<String> existingPaths;
        if(shouldDelete) {
            existingPaths = getCopyOfAllPaths();
        } else {
            existingPaths = Collections.emptySet();
        }
        if (rows.first()) {
            do {
                final String strPath = rows.getString("path");
                existingPaths.remove(strPath);

                final int numDocs = rows.getInt("numDocs");

                if(pathsToShards.containsKey(strPath)) {
                    continue;
                }

                final Path path = Paths.get(strPath);
                final ShardDir shardDir = new ShardDir(path);
                final String dataset = shardDir.getDataset();
                final String shardname = shardDir.getId();
                final ShardInfo shard = new ShardInfo(shardname, numDocs, shardDir.getVersion());
                final Interval interval = ShardTimeUtils.parseInterval(shardname);

                pathsToShards.put(strPath, shard);

                if (!tblShards.containsKey(dataset)) {
                    tblShards.put(dataset, new IntervalTree<>());
                }

                tblShards.get(dataset).addInterval(interval.getStart().getMillis(), interval.getEnd().getMillis(), shard);
            } while (rows.next());
        }

        if(shouldDelete) {
            deleteShards(existingPaths);
        }
    }

    private void addShardToDatastructure(final FlamdexMetadata metadata, final ShardDir shardDir) {
        final ShardInfo info = new ShardInfo(shardDir.getId(), metadata.getNumDocs(), shardDir.getVersion());
        final String dataset = shardDir.getDataset();

        pathsToShards.put(shardDir.getIndexDir().toString(), info);

        if(!tblShards.containsKey(dataset)) {
            tblShards.put(dataset, new IntervalTree<>());
            tblFields.put(dataset, new TableFields());
        }
        final Interval interval = ShardTimeUtils.parseInterval(shardDir.getId());
        tblShards.get(dataset).addInterval(interval.getStart().getMillis(), interval.getEnd().getMillis(), info);

        for (final String field : metadata.getIntFields()) {
            if (!tblFields.get(dataset).lastUpdatedTimestamp.containsKey(field) || tblFields.get(dataset).lastUpdatedTimestamp.get(field) < interval.getStartMillis()) {
                tblFields.get(dataset).fieldNameToFieldType.put(field, FieldType.INT);
                tblFields.get(dataset).lastUpdatedTimestamp.put(field, interval.getStartMillis());
            }
        }

        for (final String field : metadata.getStringFields()) {
            if (!tblFields.get(dataset).lastUpdatedTimestamp.containsKey(field) || tblFields.get(dataset).lastUpdatedTimestamp.get(field) < interval.getStartMillis()) {
                tblFields.get(dataset).fieldNameToFieldType.put(field, FieldType.STRING);
                tblFields.get(dataset).lastUpdatedTimestamp.put(field, interval.getStartMillis());
            }
        }
    }

    public boolean hasShard(final String path){
        return pathsToShards.containsKey(path);
    }

    public Collection<String> getDatasets(){
        return tblShards.keySet();
    }

    public Collection<ShardInfo> getShardsForDataset(final String dataset) {
        final IntervalTree<Long, ShardInfo> tree = tblShards.get(dataset);
        if (tree == null) {
            return new HashSet<>();
        }
        return tree.getAllValues();
    }

    public Collection<ShardInfo> getShardsInTime(final String dataset, final long start, final long end) {
        return tblShards.get(dataset).getValuesInRange(start, end);
    }
}
