package com.indeed.imhotep.shardmaster;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.shardmaster.utils.IntervalTree;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author kornerup
 */

public class ShardData {
    private static ShardData ourInstance = new ShardData();

    public static ShardData getInstance() {
        return ourInstance;
    }

    final private Map<String, IntervalTree<DateTime, TableShards>> tblShards;
    final private Map<String, TableFields> tblFields;
    final private Set<String> paths;

    public boolean hasField(String dataset, String field) {
        return tblFields.containsKey(dataset) && tblFields.get(dataset).lastUpdatedTimestamp.containsKey(field);
    }

    public long getFieldUpdateTime(String dataset, String field) {
        if(hasField(dataset, field)) {
            return tblFields.get(dataset).lastUpdatedTimestamp.get(field);
        }
        return 0;
    }

    enum fieldType{
        INT(0), STRING(1);

        private int value;

        public int getValue(){
            return value;
        }

        static fieldType getType(int value) {
            switch (value) {
                case 0:
                    return INT;
                case 1:
                    return STRING;
            }
            return null;
        }

        fieldType(int value){
            this.value = value;
        }
    }

    class TableShards {
        String path;
        int numDocs;
        TableShards(String path, int numDocs) {
            this.path = path;
            this.numDocs = numDocs;
        }
    }

    class TableFields {
        Map<String, fieldType> fieldNameToFieldType;
        Map<String, Long> lastUpdatedTimestamp;
        public TableFields(){
            fieldNameToFieldType = new ConcurrentHashMap<>();
            lastUpdatedTimestamp = new ConcurrentHashMap<>();
        }
    }

    private ShardData() {
        tblShards = new ConcurrentHashMap<>();
        tblFields = new ConcurrentHashMap<>();
        paths = new ConcurrentSkipListSet<>();
    }

    public void addShardFromHDFS(FlamdexMetadata metadata, Path shardPath, ShardDir shardDir) {
        addShardToDatastructure(metadata, shardPath, shardDir);
    }

    public void addTableFieldsRowsFromSQL(ResultSet rows) throws SQLException {
        if (rows.first()) {
            do {
                String dataset = rows.getString("dataset");
                String fieldName = rows.getString("fieldname");
                fieldType type = fieldType.getType(rows.getInt("type"));
                long dateTime = rows.getLong("lastshardstarttime");
                if (!tblFields.containsKey(dataset)) {
                    tblFields.put(dataset, new TableFields());
                }
                tblFields.get(dataset).lastUpdatedTimestamp.put(fieldName, dateTime);
                tblFields.get(dataset).fieldNameToFieldType.put(fieldName, type);
            } while (rows.next());
        }
    }

    public Map<String, List<ShardDir>> addTableShardsRowsFromSQL(ResultSet rows) throws SQLException {
        final Map<String, List<ShardDir>> shardDirs = new HashMap<>();
        if (rows.first()) {
            do {
                String strPath = rows.getString("path");
                if(paths.contains(strPath)) {
                    continue;
                }
                paths.add(strPath);
                Path path = Paths.get(strPath);
                ShardDir shardDir = new ShardDir(path);
                String dataset = shardDir.getIndexDir().getParent().toString();
                String shardname = shardDir.getId();
                if(!shardDirs.containsKey(dataset)){
                    shardDirs.put(dataset, new ArrayList<>());
                }
                shardDirs.get(dataset).add(new ShardDir(Paths.get(dataset, shardname)));

                int numDocs = rows.getInt("numDocs");
                if (!tblShards.containsKey(dataset)) {
                    tblShards.put(dataset, new IntervalTree<>());
                }
                final Interval interval = ShardTimeUtils.parseInterval(shardname);
                tblShards.get(dataset).addInterval(interval.getStart(), interval.getEnd(), new TableShards(path.toString(), numDocs));
               } while (rows.next());
        }
        return shardDirs;
    }

    private void addShardToDatastructure(FlamdexMetadata metadata, Path shardPath, ShardDir shardDir) {
        paths.add(shardDir.getIndexDir().toString());
        String dataset = shardDir.getIndexDir().getParent().toString();
        if(!tblShards.containsKey(dataset)) {
            tblShards.put(dataset, new IntervalTree<>());
            tblFields.put(dataset, new TableFields());
        }
        final Interval interval = ShardTimeUtils.parseInterval(shardDir.getId());
        tblShards.get(dataset).addInterval(interval.getStart(), interval.getEnd(), new TableShards(shardPath.toString(), metadata.getNumDocs()));

        metadata.getIntFields().parallelStream().forEach(field ->
            {
                tblFields.get(dataset).fieldNameToFieldType.put(field, fieldType.INT);
                tblFields.get(dataset).lastUpdatedTimestamp.put(field, DateTime.now().getMillis());
            });
        metadata.getStringFields().parallelStream().forEach(field ->
            {
                tblFields.get(dataset).fieldNameToFieldType.put(field, fieldType.STRING);
                tblFields.get(dataset).lastUpdatedTimestamp.put(field, DateTime.now().getMillis());
            });
    }

    public boolean hasShard(String path){
        return paths.contains(path);
    }

    public Collection<String> getDatasets(){
        return tblShards.keySet();
    }


    // NOTE: this is a bit hacky to get a linear runtime
    public Collection<ShardDir> getShardsForDataset(String dataset) {
        IntervalTree<DateTime, TableShards> tree = tblShards.get(dataset);
        if (tree == null) {
            return new HashSet<>();
        }
        Map<String, ShardDir> shardToShardWithVersion = new HashMap<>();
        tree.getAllValues().stream().map(path -> new ShardDir(Paths.get(path.path))).forEach(shardDir -> {
            if(!shardToShardWithVersion.containsKey(shardDir.getId()) || shardToShardWithVersion.get(shardDir.getId()).getVersion() < shardDir.getVersion()) {
                shardToShardWithVersion.put(shardDir.getId(), shardDir);
            }
        });
        return shardToShardWithVersion.values();
    }
}
