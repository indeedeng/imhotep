package com.indeed.imhotep;
import java.nio.file.Path;

/**
 * @author kornerup
 */

public class ShardWithPathAndDataset extends Shard {
    private final Path path;
    private final String dataset;

    public ShardWithPathAndDataset(ShardDir shardDir, int numDocs) {
        super(shardDir.getId(), numDocs, shardDir.getVersion());
        this.path = shardDir.getIndexDir();
        this.dataset = shardDir.getDataset();
    }

    public ShardWithPathAndDataset(String shardId, int numDocs, long version, Path path, String datasetName) {
        super(shardId, numDocs, version);
        this.path = path;
        this.dataset = datasetName;
    }

    public Path getPath() {
        return path;
    }

    public String getDataset() {return dataset; }
}
