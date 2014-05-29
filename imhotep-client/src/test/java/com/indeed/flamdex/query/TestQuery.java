package com.indeed.flamdex.query;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author dwahler
 */
public class TestQuery {
    @Test
    public void testEqualsHashCode() {
        @SuppressWarnings("unchecked")
        List<List<Query>> equivalenceClasses = ImmutableList.<List<Query>>of(
                ImmutableList.of(
                        Query.newTermQuery(new Term("abc", true, 123, "foo")),
                        Query.newTermQuery(new Term("abc", true, 123, "bar"))),
                ImmutableList.of(
                        Query.newTermQuery(new Term("abc", false, 0, "foo")),
                        Query.newTermQuery(new Term("abc", false, 123, "foo"))),
                ImmutableList.of(
                        Query.newTermQuery(new Term("abc", false, 123, "bar"))),

                ImmutableList.of(Query.newRangeQuery("foo", 123, 456, true)),
                ImmutableList.of(Query.newRangeQuery("foo", 123, 456, false)),
                ImmutableList.of(Query.newRangeQuery("foo", "123", "456", true)),
                ImmutableList.of(Query.newRangeQuery("foo", "123", "456", false)),

                ImmutableList.of(Query.newBooleanQuery(BooleanOp.AND, ImmutableList.of(
                        Query.newTermQuery(new Term("abc", true, 123, ""))))),
                ImmutableList.of(Query.newBooleanQuery(BooleanOp.AND, ImmutableList.of(
                        Query.newTermQuery(new Term("abc", true, 123, "")),
                        Query.newTermQuery(new Term("abc", true, 456, ""))))),
                ImmutableList.of(Query.newBooleanQuery(BooleanOp.OR, ImmutableList.of(
                        Query.newTermQuery(new Term("abc", true, 123, ""))))),
                ImmutableList.of(Query.newBooleanQuery(BooleanOp.OR, ImmutableList.of(
                        Query.newTermQuery(new Term("abc", true, 123, "")),
                        Query.newTermQuery(new Term("abc", true, 456, ""))))),
                ImmutableList.of(Query.newBooleanQuery(BooleanOp.NOT, ImmutableList.of(
                        Query.newTermQuery(new Term("abc", true, 123, "")))))
        );

        int hashCollisions = 0, hashComparisons = 0;

        for (List<Query> group1 : equivalenceClasses) {
            for (List<Query> group2 : equivalenceClasses) {

                for (Query a : group1) {
                    for (Query b : group2) {

                        if (group1 == group2) {
                            // assert equality within class
                            assertEquals(a, b);
                            assertEquals(a.hashCode(), b.hashCode());
                        } else {
                            // assert non-equivalence with other classes
                            assertFalse(a + " equals " + b, a.equals(b));
                            if (a.hashCode() == b.hashCode()) {
                                hashCollisions++;
                            }
                            hashComparisons++;
                        }

                    }
                }

            }
        }

        System.out.printf("hash collisions occurred for %d out of %d comparisons\n", hashCollisions, hashComparisons);
    }
}
