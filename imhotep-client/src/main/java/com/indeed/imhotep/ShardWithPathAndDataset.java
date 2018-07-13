package com.indeed.imhotep;
import com.indeed.imhotep.client.Host;

import java.nio.file.Path;

/**
 * @author kornerup
 */

public class ShardWithPathAndDataset extends Shard {
    private final Path path;
    private final String dataset;

    public ShardWithPathAndDataset(ShardDir shardDir, int numDocs, Host host) {
        super(shardDir.getId(), numDocs, shardDir.getVersion(), host);
        this.path = shardDir.getIndexDir();
        this.dataset = shardDir.getDataset();
    }

    public ShardWithPathAndDataset(String shardId, int numDocs, long version, Host host, Path path, String datasetName) {
        super(shardId, numDocs, version, host);
        this.path = path;
        this.dataset = datasetName;
    }

    public Path getPath() {
        return path;
    }

    public String getDataset() {return dataset; }
}
