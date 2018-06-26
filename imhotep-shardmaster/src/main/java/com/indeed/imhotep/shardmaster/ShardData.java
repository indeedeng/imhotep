package com.indeed.imhotep.shardmaster;

import com.indeed.imhotep.Shard;
import com.indeed.imhotep.ShardDir;
import org.joda.time.DateTime;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author kornerup
 */

public class ShardData {
    private static ShardData ourInstance = new ShardData();

    public static ShardData getInstance() {
        return ourInstance;
    }

    final private Map<String, TableShards> tblShards;
    final private Map<String, TableFields> tblFields;

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
        Map<String, Integer> shardNameToNumDocs;
        public TableShards(){
            shardNameToNumDocs = new ConcurrentHashMap<>();
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
    }

    public void addShardFromHDFS(FlamdexMetadata metadata, Path shardPath, String dataset, String shardId) {
        addShardToDatastructure(metadata, shardPath, dataset, shardId);
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
                String dataset = rows.getString("dataset");
                String shardname = rows.getString("shardname");
                if(!shardDirs.containsKey(dataset)){
                    shardDirs.put(dataset, new ArrayList<>());
                }
                shardDirs.get(dataset).add(new ShardDir(Paths.get(dataset, shardname)));

                int numDocs = rows.getInt("numDocs");
                if (!tblShards.containsKey(dataset)) {
                    tblShards.put(dataset, new TableShards());
                }
                tblShards.get(dataset).shardNameToNumDocs.put(shardname, numDocs);
               } while (rows.next());
        }
        return shardDirs;
    }

    private void addShardToDatastructure(FlamdexMetadata metadata, Path shardPath, String dataset, String shardId) {
        if(!tblShards.containsKey(dataset)) {
            tblShards.put(dataset, new TableShards());
            tblFields.put(dataset, new TableFields());
        }
        tblShards.get(dataset).shardNameToNumDocs.put(shardId, metadata.getNumDocs());

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

    public boolean hasShard(String dataset, ShardDir shardDir){
        String id = shardDir.getId();
        return tblShards.containsKey(dataset) && tblShards.get(dataset).shardNameToNumDocs.containsKey(id);
    }

    public Collection<String> getDatasets(){
        return tblShards.keySet();
    }

    public Collection<String> getShardsForDataset(String dataset){
        return tblShards.get(dataset).shardNameToNumDocs.keySet();
    }
}
