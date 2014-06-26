package org.zenoss.zep.index.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NGramPhraseQuery;
import org.apache.lucene.search.PrefixFilter;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.WildcardQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public class FilterCacheManager {

    private static final Logger logger = LoggerFactory.getLogger(FilterCacheManager.class);

    public static enum FilterType {
        PREFIX, TERMS, WILDCARD, NGRAM
    }

    private final LoadingCache<Term, CachingWrapperFilter> prefixCache;
    private final LoadingCache<Set<Term>, CachingWrapperFilter> termsCache;
    private final LoadingCache<Term, CachingWrapperFilter> wildcardCache;
    private final LoadingCache<List<Term>, CachingWrapperFilter> nGramCache;

    public FilterCacheManager() {

        prefixCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(
                        new CacheLoader<Term, CachingWrapperFilter>() {
                            @Override
                            public CachingWrapperFilter load(Term t) throws Exception {
                                logger.debug("Encountered a new PrefixFilter term: {}", t);
                                return new CachingWrapperFilter(new PrefixFilter(t));
                            }
                        }
                );

        wildcardCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(
                        new CacheLoader<Term, CachingWrapperFilter>() {
                            @Override
                            public CachingWrapperFilter load(Term t) throws Exception {
                                logger.debug("Encountered a new WildcardQuery term: {}", t);
                                return new CachingWrapperFilter(new QueryWrapperFilter(new WildcardQuery(t)));
                            }
                        }
                );

        termsCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(
                        new CacheLoader<Set<Term>, CachingWrapperFilter>() {
                            @Override
                            public CachingWrapperFilter load(Set<Term> t) throws Exception {
                                logger.debug("Encountered a new TermsFilter term: {}", t);
                                TermsFilter termsFilter = new TermsFilter(Lists.newArrayList(t));
                                return new CachingWrapperFilter(termsFilter);
                            }
                        }
                );

        nGramCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(
                        new CacheLoader<List<Term>, CachingWrapperFilter>() {
                            @Override
                            public CachingWrapperFilter load(List<Term> t) throws Exception {
                                logger.debug("Encountered a new NGramPhaseQuery term: {}", t);
                                NGramPhraseQuery pq = new NGramPhraseQuery(IdentifierAnalyzer.MIN_NGRAM_SIZE);
                                for (Term term : t) {
                                    pq.add(term);
                                }
                                return new CachingWrapperFilter(new QueryWrapperFilter(pq));
                            }
                        }
                );
    }

    public Filter get(FilterType name, Term... key) {
        switch (name) {
            case NGRAM:
                return nGramCache.getUnchecked(ImmutableList.copyOf(key));
            case PREFIX:
                return prefixCache.getUnchecked(key[0]);
            case WILDCARD:
                return wildcardCache.getUnchecked(key[0]);
            case TERMS:
            default:
                return termsCache.getUnchecked(ImmutableSet.copyOf(key));
        }
    }

}
