package com.indeed.flamdex.simple;

/**
 * @author arun.
 */
final class IteratorIdPair {
    final SimpleTermIterator simpleTermIterator;
    final int id;

    IteratorIdPair(SimpleTermIterator simpleTermIterator, int id) {
        this.simpleTermIterator = simpleTermIterator;
        this.id = id;
    }
}
