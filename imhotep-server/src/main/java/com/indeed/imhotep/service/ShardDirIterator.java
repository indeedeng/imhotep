package com.indeed.imhotep.service;

import com.indeed.util.core.Pair;

/**
 * @author kenh
 * iterates over shards for a given dataset
 */
interface ShardDirIterator extends Iterable<Pair<String, ShardDir>> {
}
