package com.indeed.imhotep.shardmaster;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.shardmaster.utils.IntervalTree;
import javafx.util.Pair;
import org.joda.time.Interval;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author kornerup
 */

public class ShardData {

    final private Map<String, IntervalTree<Long, ShardInfo>> tblShards;
    private Map<String, Map<String, Pair<FieldType, Long>>> tblFields;
    final private Map<String, ShardInfo> pathsToShards;


    public boolean hasField(final String dataset, final String field) {
        return tblFields.containsKey(dataset) && tblFields.get(dataset).containsKey(field);
    }

    public long getFieldUpdateTime(final String dataset, final String field) {
        final Map<String, Pair<FieldType, Long>> datasetFields = tblFields.get(dataset);
        return datasetFields != null ? datasetFields.get(field).getValue() : 0;
    }

    public List<String> getFields(final String dataset, final FieldType type) {
        final List<String> fields = new ArrayList<>();
        final Map<String, Pair<FieldType, Long>> datasetFields = tblFields.get(dataset);
        if(datasetFields == null) {
            return new ArrayList<>();
        }
        datasetFields.forEach((name, entry) -> {
            if(entry.getKey() == type) {
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
            if(tblShards.get(temp.getDataset()).getAllValues().isEmpty()) {
                tblShards.remove(temp.getDataset());
            }
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
        INT, STRING, CONFLICT;

        static FieldType getType(final String value) {
            switch (value) {
                case "INT":
                    return INT;
                case "STRING":
                    return STRING;
                case "CONFLICT":
                    return CONFLICT;
            }
            return null;
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

    public void updateTableFieldsRowsFromSQL(final ResultSet rows) throws SQLException {
        Map<String, Map<String, Pair<FieldType, Long>>> newTblFields = new HashMap<>();

        if (rows.first()) {
            do {
                final String dataset = rows.getString("dataset");
                final String fieldName = rows.getString("fieldname");
                final FieldType type = FieldType.getType(rows.getString("type"));
                final long dateTime = rows.getLong("lastshardstarttime");
                if (!newTblFields.containsKey(dataset)) {
                    newTblFields.put(dataset, new ConcurrentHashMap<>());
                }

                newTblFields.get(dataset).put(fieldName, new Pair<>(type, dateTime));

            } while (rows.next());
        }

        tblFields = newTblFields;
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


                if(pathsToShards.containsKey(strPath)) {
                    continue;
                }

                final int numDocs = rows.getInt("numDocs");

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
            tblFields.put(dataset, new ConcurrentHashMap<>());
        }
        final Interval interval = ShardTimeUtils.parseInterval(shardDir.getId());
        tblShards.get(dataset).addInterval(interval.getStart().getMillis(), interval.getEnd().getMillis(), info);

        Set<String> stringFields = new HashSet<>(metadata.getStringFields());
        Set<String> intFields = new HashSet<>(metadata.getIntFields());

        Stream.concat(metadata.getIntFields().stream(), metadata.getStringFields().stream())
                .filter(field -> !tblFields.get(dataset).containsKey(field)
                        || tblFields.get(dataset).get(field).getValue() < interval.getStartMillis())
                .forEach(field -> {
                    if(stringFields.contains(field) && !intFields.contains(field)) {
                        tblFields.get(dataset).put(field, new Pair<>(FieldType.STRING, interval.getStartMillis()));
                    } else if(!stringFields.contains(field) && intFields.contains(field)) {
                        tblFields.get(dataset).put(field, new Pair<>(FieldType.INT, interval.getStartMillis()));
                    } else {
                        tblFields.get(dataset).put(field, new Pair<>(FieldType.CONFLICT, interval.getStartMillis()));
                    }
                });
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
