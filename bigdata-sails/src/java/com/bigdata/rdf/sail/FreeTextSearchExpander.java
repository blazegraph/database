package com.bigdata.rdf.sail;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.query.algebra.StatementPattern;

import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.XSDDoubleIV;
import com.bigdata.rdf.lexicon.ITextIndexer;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.IAccessPathExpander;
import com.bigdata.search.Hiterator;
import com.bigdata.search.IHit;
import com.bigdata.striterator.ChunkedConvertingIterator;
import com.bigdata.striterator.ChunkedOrderedStriterator;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.Filter;
import com.bigdata.striterator.IChunkConverter;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

/**
 * Class used to expand a {@link StatementPattern} involving a
 * {@link BD#SEARCH} magic predicate into the set of subjects having any of the
 * tokens in the query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FreeTextSearchExpander implements IAccessPathExpander<ISPO> {
    
    private static final transient Logger log = Logger
            .getLogger(FreeTextSearchExpander.class);
    
    private static final long serialVersionUID = 1L;
    
    private static final transient ISPO[] EMPTY = new ISPO[0];

    /**
     * FIXME This reference is NOT {@link Serializable}, but the expander is.
     */
    private final AbstractTripleStore database;
    
    private final Literal query, maxHits, minRelevance;
    
    /** Note: volatile for visibility (or use AtomicReference). */
    private volatile Set<URI> graphs;
    
    public FreeTextSearchExpander(final AbstractTripleStore database,
            final Literal query) {

        this(database, query, null, null);
        
    }
    
    public FreeTextSearchExpander(final AbstractTripleStore database,
            final Literal query, final Literal maxHits, 
            final Literal minRelevance) {

        if (database == null)
            throw new IllegalArgumentException();
       
        if (query == null)
            throw new IllegalArgumentException();
        
        this.database = database;
        
        this.query = query;
        
        this.maxHits = maxHits;
        
        this.minRelevance = minRelevance;
        
    }
    
    public boolean backchain() {
        return false;
    }
    
    public boolean runFirst() {
        return true;
    }
    
    public IAccessPath<ISPO> getAccessPath(
            final IAccessPath<ISPO> accessPath) {

        return new FreeTextSearchAccessPath(accessPath);
        
    }
    
    /**
     * Add a set of named graphs to use to filter free text search results. We 
     * are checking to see for each search hit whether that hit is used in a 
     * statement in any of the named graphs. If not, we need to filter this hit
     * out, otherwise it creates a security hole.  We only check the
     * object position for now, because only literals can be hits
     * (for now).
     * 
     * @todo fix if we ever start free text indexing URIs.
     * 
     * @param graphs
     *          The set of named graphs to use in the filtering process.
     */
    public void addNamedGraphsFilter(final Set<URI> graphs) {
        
        this.graphs = graphs;
        
    }
    
    private class FreeTextSearchAccessPath implements IAccessPath<ISPO> {

        private IAccessPath<ISPO> accessPath;
        
        private Hiterator<IHit> hiterator;
        
        public FreeTextSearchAccessPath(final IAccessPath<ISPO> accessPath) {
//            final SPOPredicate pred = (SPOPredicate) accessPath.getPredicate();
//            IVariableOrConstant<IV> p = pred.p();
//            IVariableOrConstant<IV> o = pred.o();
//            if (p.isConstant() == false || o.isConstant() == false) {
//                throw new IllegalArgumentException("query not well formed");
//            }
            this.accessPath = accessPath;
        }
        
        private Hiterator<IHit> getHiterator() {

            if (hiterator == null) {
                
                @SuppressWarnings("unchecked")
                final ITextIndexer<IHit> textNdx = (ITextIndexer) 
                	database.getLexiconRelation().getSearchEngine();
                
                if (textNdx == null)
                    throw new UnsupportedOperationException(
                            "No free text index?");
                
                String s = query.getLabel();
                final boolean prefixMatch;
                if (s.indexOf('*') >= 0) {
                	prefixMatch = true;
                	s = s.replaceAll("\\*", "");
                } else {
                	prefixMatch = false;
                }

                /*
                 * FIXME This is using a constant (1000ms) for the timeout on
                 * the free text search. That needs to be passed down from the
                 * SAIL.
                 * 
                 * @todo Rather than explicitly passing in all of these as
                 * parameters to the constructor, why not pass them through as
                 * annotations on the magic predicate?
                 */
                hiterator = textNdx.search(s,
                                query.getLanguage(), 
                                prefixMatch,
                                minRelevance == null ? 0d : minRelevance.doubleValue()/* minCosine */, 
                                maxHits == null ? 10000 : maxHits.intValue()+1/* maxRank */,
                                1000L/* timeout */, TimeUnit.MILLISECONDS);
            }

            return hiterator;
            
        }
        
        public IIndex getIndex() {

            return accessPath.getIndex();
            
        }

        /**
         * The results are in decreasing cosine (aka relevance) order.
         * 
         * @return <code>null</code> since the results are not in any defined
         *         {@link SPOKeyOrder}.
         */
        public IKeyOrder<ISPO> getKeyOrder() {
            
           return null;
            
//            return accessPath.getKeyOrder();
            
        }

        public IPredicate<ISPO> getPredicate() {

            return accessPath.getPredicate();
            
        }

        public boolean isEmpty() {

            return rangeCount(false/* exact */) > 0;
            
        }

        public IChunkedOrderedIterator<ISPO> iterator() {
            
            final IChunkedOrderedIterator<IHit> itr2 = 
                new ChunkedWrappedIterator<IHit>(getHiterator());
            
            final IChunkedOrderedIterator<ISPO> itr3 = 
                new ChunkedConvertingIterator<IHit,ISPO>
                ( itr2, new HitConverter(accessPath)
                  );
            
            // if graphs is null we don't need to filter results for named graphs
            if (graphs == null) {
                return itr3;
            }
            
            /* 
             * Here we filter results for named graphs.
             */
            final IChunkedOrderedIterator<ISPO> itr4 = 
                new ChunkedOrderedStriterator<IChunkedOrderedIterator<ISPO>, ISPO>(itr3).
                addFilter(new Filter<IChunkedOrderedIterator<ISPO>, ISPO>() {
                    protected boolean isValid(ISPO e) {
                        final BigdataValue val = database.getTerm(e.s());
                        for (URI c : graphs) {
                            if (database.getAccessPath(null, null, val, c).rangeCount(true) > 0) {
                                return true;
                            }
                        }
                        return false;
                    }
                });
            
            return itr4;

        }

        //@todo ignores limit.
        public IChunkedOrderedIterator<ISPO> iterator(int limit, int capacity) {

            return iterator();
            
        }

        //@todo ignores offset & limit.
        public IChunkedOrderedIterator<ISPO> iterator(long offset, long limit, int capacity) {

            return iterator();
            
        }

        public long rangeCount(final boolean exactIsIgnored) {

            final long rangeCount = getHiterator().size();

            if (log.isInfoEnabled())
                log.info("range count: " + rangeCount);

            return rangeCount;
            
        }

        public ITupleIterator<ISPO> rangeIterator() {
            
            throw new UnsupportedOperationException();
            
        }

        public long removeAll() {
            
            throw new UnsupportedOperationException();
            
        }
        
    }
    
    static private class HitConverter implements IChunkConverter<IHit,ISPO> {
        
        private final boolean isBound;
        
        private final IV boundVal;
        
        public HitConverter(final IAccessPath<ISPO> accessPath) {
            final SPOPredicate pred = (SPOPredicate) accessPath.getPredicate();
            final IVariableOrConstant<IV> s = pred.s();
            this.isBound = s.isConstant();
            this.boundVal = isBound ? s.get() : null;
            if (log.isInfoEnabled())
                log.info("isBound=" + isBound + ", boundVal: " + boundVal);
        }

        public ISPO[] convert(final IChunkedOrderedIterator<IHit> src) {
            final IHit[] hits = src.nextChunk();
            if (isBound) {
                return convertWhenBound(hits);
            }
            final ISPO[] spos = new ISPO[hits.length];
            for (int i = 0; i < hits.length; i++) {
                final IV s = new TermId(VTE.LITERAL, hits[i].getDocId());
                final IV p = new XSDDoubleIV(hits[i].getCosine());
                final IV o = null; // reserved
                final IV c = null; // reserved
                spos[i] = new SPO(s, p, o, c);
                if (log.isInfoEnabled())
                    log.info("hit: " + spos[i]);
            }
//            Arrays.sort(spos, SPOKeyOrder.SPO.getComparator());
            return spos;
        }
        
        private ISPO[] convertWhenBound(final IHit[] hits) {
            ISPO[] result = EMPTY;
            for (IHit hit : hits) {
                final IV s = new TermId(VTE.LITERAL, hit.getDocId());
                if (s == boundVal) {
                    final IV p = new XSDDoubleIV(hit.getCosine());
                    final IV o = null; // reserved
                    final IV c = null; // reserved
                    result = new ISPO[] { new SPO(s, p, o, c) };
                    break;
                }
            }
            if (log.isInfoEnabled())
                log.info("# of results: " + result.length);
            return result;
        }

    }

}
