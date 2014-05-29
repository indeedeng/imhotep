package com.indeed.flamdex.lucene;

import com.google.common.collect.Lists;
import com.indeed.flamdex.query.BooleanOp;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreRangeQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.RangeQuery;
import org.apache.lucene.search.TermQuery;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * @author jsgroth
 */
public final class LuceneQueryTranslator {
    private LuceneQueryTranslator() {}

    public static Query rewrite(org.apache.lucene.search.Query q, Set<String> intFields) {
        if (q instanceof TermQuery) {
            return rewrite((TermQuery)q, intFields);
        } else if (q instanceof BooleanQuery) {
            return rewrite((BooleanQuery)q, intFields);
        } else if (q instanceof RangeQuery) {
            return rewrite((RangeQuery)q, intFields);
        } else if (q instanceof ConstantScoreRangeQuery) {
            return rewrite((ConstantScoreRangeQuery)q, intFields);
        } else if (q instanceof PrefixQuery) {
            return rewrite((PrefixQuery)q, intFields);
        } else if (q instanceof PhraseQuery) {
            return rewrite((PhraseQuery)q, intFields);
        }
        throw new IllegalArgumentException("unsupported lucene query type: " + q.getClass().getSimpleName());
    }

    public static Query rewrite(PrefixQuery pq, Set<String> intFields) {
        if (intFields.contains(pq.getPrefix().field())) {
            // not really sure what to do here, for now just treat it as an inequality query
            return Query.newRangeQuery(pq.getPrefix().field(), Long.parseLong(pq.getPrefix().text()), Long.MAX_VALUE, true);
        } else {
            final String prefix = pq.getPrefix().text();
            final String endTerm = prefix.substring(0, prefix.length() - 1) + ((char)((prefix.charAt(prefix.length() - 1) + 1)));
            return Query.newRangeQuery(pq.getPrefix().field(), prefix, endTerm, false);
        }
    }

    public static Query rewrite(BooleanQuery bq, Set<String> intFields) {
        boolean hasMust = false;
        boolean hasMustNot = false;
        for (final BooleanClause clause : bq.getClauses()) {
            if (clause.getOccur() == BooleanClause.Occur.MUST) {
                hasMust = true;
            } else if (clause.getOccur() == BooleanClause.Occur.MUST_NOT) {
                hasMustNot = true;
            }
        }

        if (hasMustNot) {
            final List<Query> nots = Lists.newArrayList();
            final List<Query> other = Lists.newArrayList();
            for (final BooleanClause clause : bq.getClauses()) {
                if (clause.getOccur() == BooleanClause.Occur.MUST_NOT) {
                    final Query query = rewrite(clause.getQuery(), intFields);
                    nots.add(query);
                } else if (clause.getOccur() == BooleanClause.Occur.MUST ||
                        (clause.getOccur() == BooleanClause.Occur.SHOULD && !hasMust)) {
                    final Query query = rewrite(clause.getQuery(), intFields);
                    other.add(query);
                }
            }

            final Query notQuery = Query.newBooleanQuery(BooleanOp.NOT, nots);
            if (other.isEmpty()) {
                return notQuery;
            } else {
                return Query.newBooleanQuery(BooleanOp.AND, Arrays.asList(Query.newBooleanQuery(hasMust ? BooleanOp.AND : BooleanOp.OR, other), notQuery));
            }
        } else {
            final List<Query> operands = Lists.newArrayList();
            for (final BooleanClause clause : bq.getClauses()) {
                if (clause.getOccur() == BooleanClause.Occur.MUST || (clause.getOccur() == BooleanClause.Occur.SHOULD && !hasMust)) {
                    final Query query = rewrite(clause.getQuery(), intFields);
                    operands.add(query);
                }
            }
            return Query.newBooleanQuery(hasMust ? BooleanOp.AND : BooleanOp.OR, operands);
        }
    }

    public static Query rewrite(TermQuery tq, Set<String> intFields) {
        final Term term = rewriteTerm(tq.getTerm(), intFields);
        return Query.newTermQuery(term);
    }

    private static Term rewriteTerm(org.apache.lucene.index.Term lTerm, Set<String> intFields) {
        final String field = lTerm.field();
        final Term term;
        if (intFields.contains(field)) {
            term = new Term(field, true, Long.parseLong(lTerm.text()), "");
        } else {
            String termText = lTerm.text();
            term = new Term(field, false, 0, termText);
        }
        return term;
    }

    public static Query rewrite(RangeQuery rq, Set<String> intFields) {
        final Term startTerm = rewriteTerm(rq.getLowerTerm(), intFields);
        final Term endTerm = rewriteTerm(rq.getUpperTerm(), intFields);
        return Query.newRangeQuery(startTerm, endTerm, rq.isInclusive());
    }

    public static Query rewrite(ConstantScoreRangeQuery rq, Set<String> intFields) {
        final Term startTerm = rewriteTerm(new org.apache.lucene.index.Term(rq.getField(), rq.getLowerVal()), intFields);
        final Term endTerm = rewriteTerm(new org.apache.lucene.index.Term(rq.getField(), rq.getUpperVal()), intFields);
        return Query.newRangeQuery(startTerm, endTerm, rq.includesUpper());
    }

    public static Query rewrite(PhraseQuery pq, Set<String> intFields) {
        final List<Query> termQueries = Lists.newArrayListWithCapacity(pq.getTerms().length);
        for (final org.apache.lucene.index.Term term : pq.getTerms()) {
            termQueries.add(Query.newTermQuery(rewriteTerm(term, intFields)));
        }
        return Query.newBooleanQuery(BooleanOp.AND, termQueries);
    }
}
