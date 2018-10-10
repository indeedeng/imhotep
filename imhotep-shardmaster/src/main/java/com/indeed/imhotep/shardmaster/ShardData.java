package com.indeed.imhotep.shardmaster;
import com.google.common.base.Joiner;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.shardmaster.utils.IntervalTree;
import javafx.util.Pair;
import org.apache.log4j.Logger;
import org.joda.time.Interval;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author kornerup
 */

public class ShardData {
    private static final Logger LOGGER = Logger.getLogger(ShardData.class);

    private final Map<String, IntervalTree<Long, ShardInfo>> tblShards;
    private final AtomicReference<Map<String, Map<String, Pair<FieldType, Long>>>> tblFields;
    private final Map<String, ShardInfo> pathsToShards;

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
        tblFields = new AtomicReference<>(new ConcurrentHashMap<>());
        pathsToShards = new ConcurrentHashMap<>();
    }

    public void addShardFromFilesystem(final ShardDir shardDir, final FlamdexMetadata metadata) {
        final ShardInfo info = new ShardInfo(shardDir.getId(), metadata.getNumDocs(), shardDir.getVersion());
        final String dataset = shardDir.getDataset();

        pathsToShards.put(shardDir.getIndexDir().toString(), info);

        if(!tblShards.containsKey(dataset)) {
            tblShards.put(dataset, new IntervalTree<>());
            tblFields.get().put(dataset, new ConcurrentHashMap<>());
        }
        final Interval interval = ShardTimeUtils.parseInterval(shardDir.getId());
        tblShards.get(dataset).addInterval(interval.getStart().getMillis(), interval.getEnd().getMillis(), info);

        final Set<String> stringFields = new HashSet<>(metadata.getStringFields());
        final Set<String> intFields = new HashSet<>(metadata.getIntFields());

        Stream.concat(metadata.getIntFields().stream(), metadata.getStringFields().stream())
                .filter(field -> !tblFields.get().get(dataset).containsKey(field)
                        || (tblFields.get().get(dataset).get(field).getValue() < interval.getStartMillis()))
                .forEach(field -> {
                    if(stringFields.contains(field) && !intFields.contains(field)) {
                        tblFields.get().get(dataset).put(field, new Pair<>(FieldType.STRING, interval.getStartMillis()));
                    } else if(!stringFields.contains(field) && intFields.contains(field)) {
                        tblFields.get().get(dataset).put(field, new Pair<>(FieldType.INT, interval.getStartMillis()));
                    } else {
                        tblFields.get().get(dataset).put(field, new Pair<>(FieldType.CONFLICT, interval.getStartMillis()));
                    }
                });
    }

    public void updateTableShardsRowsFromSQL(final ResultSet rows, final boolean shouldDelete, final ShardFilter filter) throws SQLException {
        final Set<String> pathsThatWeMightDelete;
        if(shouldDelete) {
            pathsThatWeMightDelete = getCopyOfAllPaths();
        } else {
            pathsThatWeMightDelete = Collections.emptySet();
        }
        if (rows.first()) {
            do {
                final String strPath = rows.getString("path");
                if (shouldDelete) {
                    pathsThatWeMightDelete.remove(strPath);
                }
                
                if(pathsToShards.containsKey(strPath)) {
                    continue;
                }

                final int numDocs = rows.getInt("numDocs");
                final Path path = Paths.get(strPath);
                final ShardDir shardDir = new ShardDir(path);
                final String dataset = shardDir.getDataset();
                final String shardname = shardDir.getId();
                if(!filter.accept(dataset, shardname)) {
                    continue;
                }
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
            deleteShards(pathsThatWeMightDelete);
        }
    }

    public void updateTableFieldsRowsFromSQL(final ResultSet rows, final ShardFilter filter) throws SQLException {
        final Map<String, Map<String, Pair<FieldType, Long>>> newTblFields = new ConcurrentHashMap<>();

        if (rows.first()) {
            do {
                final String dataset = rows.getString("dataset");
                if(!filter.accept(dataset)) {
                    continue;
                }
                final String fieldName = rows.getString("fieldname");
                final FieldType type = FieldType.getType(rows.getString("type"));
                final long dateTime = rows.getLong("lastshardstarttime");
                if (!newTblFields.containsKey(dataset)) {
                    newTblFields.put(dataset, new ConcurrentHashMap<>());
                }

                newTblFields.get(dataset).put(fieldName, new Pair<>(type, dateTime));

            } while (rows.next());
        }

        tblFields.set(newTblFields);
    }

    public void deleteShards(final Set<String> deleteCandidates) {
        for(final String path: deleteCandidates) {
            final ShardDir temp = new ShardDir(Paths.get(path));
            final Interval interval = ShardTimeUtils.parseInterval(temp.getId());
            tblShards.get(temp.getDataset()).deleteInterval(interval.getStart().getMillis(), interval.getEnd().getMillis(), pathsToShards.get(path));
            if(tblShards.get(temp.getDataset()).isEmpty()) {
                tblShards.remove(temp.getDataset());
            }
            pathsToShards.remove(path);
        }
    }

    public List<String> deleteDatasetsWithoutShards() {
        final List<String> datasets = tblFields.get().keySet().stream().filter(dataset -> (!tblShards.containsKey(dataset)) || tblShards.get(dataset).isEmpty()).collect(Collectors.toList());
        if (!datasets.isEmpty()) {
            LOGGER.info("Deleting in memory data for empty datasets: " + Joiner.on(",").join(datasets));
            for (final String dataset : datasets) {
                tblShards.remove(dataset);
                tblFields.get().remove(dataset);
            }
        }
        return datasets;
    }


    public boolean hasField(final String dataset, final String field) {
        return tblFields.get().containsKey(dataset) && tblFields.get().get(dataset).containsKey(field);
    }

    public List<String> getFields(final String dataset, final FieldType type) {
        final List<String> fields = new ArrayList<>();
        final Map<String, Pair<FieldType, Long>> datasetFields = tblFields.get().get(dataset);
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

    public long getFieldUpdateTime(final String dataset, final String field) {
        final Map<String, Pair<FieldType, Long>> datasetFields = tblFields.get().get(dataset);
        return (datasetFields != null) ? datasetFields.get(field).getValue() : 0;
    }

    public boolean hasShard(final String path){
        return pathsToShards.containsKey(path);
    }

    public Collection<ShardInfo> getAllPaths() {
        return pathsToShards.values();
    }

    public Set<String> getCopyOfAllPaths() {
        final Set<String> set = ConcurrentHashMap.newKeySet(pathsToShards.size());
        set.addAll(pathsToShards.keySet());
        return set;
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
        final IntervalTree<Long, ShardInfo> shards = tblShards.get(dataset);
        if(shards == null) {
            return Collections.emptyList();
        }
        return shards.getValuesInRange(start, end);
    }

    public int getNumDocs(final String path) {
        if(!pathsToShards.containsKey(path)) {
            return -1;
        }
        return pathsToShards.get(path).numDocs;
    }
}
