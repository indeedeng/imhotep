package com.indeed.flamdex.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.indeed.flamdex.lucene.LuceneQueryTranslator;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * @author dwahler
 */
public class TestLuceneQueryTranslator {
    @Test
    public void testTerm() {
        Query q1 = LuceneQueryTranslator.rewrite(new TermQuery(new org.apache.lucene.index.Term("abc", "123")),
                ImmutableSet.<String>of());
        assertEquals(Query.newTermQuery(new Term("abc", false, 0, "123")), q1);

        Query q2 = LuceneQueryTranslator.rewrite(new TermQuery(new org.apache.lucene.index.Term("abc", "123")),
                ImmutableSet.<String>of("abc"));
        assertEquals(Query.newTermQuery(new Term("abc", true, 123, "")), q2);
    }

    @Test
    public void testAnd() {
        final BooleanQuery bq = new BooleanQuery();
        bq.add(new TermQuery(new org.apache.lucene.index.Term("f", "a")), BooleanClause.Occur.MUST);
        bq.add(new TermQuery(new org.apache.lucene.index.Term("f", "b")), BooleanClause.Occur.MUST);

        Query q1 = LuceneQueryTranslator.rewrite(bq, Collections.<String>emptySet());
        assertEquals(Query.newBooleanQuery(BooleanOp.AND, ImmutableList.of(
                Query.newTermQuery(new Term("f", false, 0, "a")),
                Query.newTermQuery(new Term("f", false, 0, "b"))
        )), q1);
    }

    @Test
    public void testOr() {
        final BooleanQuery bq = new BooleanQuery();
        bq.add(new TermQuery(new org.apache.lucene.index.Term("f", "a")), BooleanClause.Occur.SHOULD);
        bq.add(new TermQuery(new org.apache.lucene.index.Term("f", "b")), BooleanClause.Occur.SHOULD);

        Query q1 = LuceneQueryTranslator.rewrite(bq, Collections.<String>emptySet());
        assertEquals(Query.newBooleanQuery(BooleanOp.OR, ImmutableList.of(
                Query.newTermQuery(new Term("f", false, 0, "a")),
                Query.newTermQuery(new Term("f", false, 0, "b"))
        )), q1);
    }

    @Test
    public void testMixed1() {
        final BooleanQuery bq = new BooleanQuery();
        bq.add(new TermQuery(new org.apache.lucene.index.Term("f", "a")), BooleanClause.Occur.MUST);
        bq.add(new TermQuery(new org.apache.lucene.index.Term("f", "b")), BooleanClause.Occur.SHOULD);

        Query q1 = LuceneQueryTranslator.rewrite(bq, Collections.<String>emptySet());
        assertEquals(Query.newBooleanQuery(BooleanOp.AND, ImmutableList.of(
                Query.newTermQuery(new Term("f", false, 0, "a"))
        )), q1);
    }

    @Test
    public void testMixed2() {
        final BooleanQuery bq = new BooleanQuery();
        bq.add(new TermQuery(new org.apache.lucene.index.Term("f", "a")), BooleanClause.Occur.MUST);
        bq.add(new TermQuery(new org.apache.lucene.index.Term("f", "b")), BooleanClause.Occur.MUST_NOT);

        Query q1 = LuceneQueryTranslator.rewrite(bq, Collections.<String>emptySet());
        assertEquals(Query.newBooleanQuery(BooleanOp.AND, ImmutableList.of(
                Query.newBooleanQuery(BooleanOp.AND, ImmutableList.of(
                        Query.newTermQuery(new Term("f", false, 0, "a")))),
                Query.newBooleanQuery(BooleanOp.NOT, ImmutableList.<Query>of(
                        Query.newTermQuery(new Term("f", false, 0, "b"))))
        )), q1);
    }

    @Test
    public void testNor() {
        final BooleanQuery bq = new BooleanQuery();
        bq.add(new TermQuery(new org.apache.lucene.index.Term("f", "a")), BooleanClause.Occur.MUST_NOT);
        bq.add(new TermQuery(new org.apache.lucene.index.Term("f", "b")), BooleanClause.Occur.MUST_NOT);

        Query q1 = LuceneQueryTranslator.rewrite(bq, Collections.<String>emptySet());
        assertEquals(Query.newBooleanQuery(BooleanOp.NOT, ImmutableList.of(
                Query.newBooleanQuery(BooleanOp.OR, ImmutableList.of(
                        Query.newTermQuery(new Term("f", false, 0, "a")),
                        Query.newTermQuery(new Term("f", false, 0, "b"))))
        )), q1);
    }
}
