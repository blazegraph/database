package com.bigdata.rdf.sail;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.query.algebra.StatementPattern;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.ISolutionExpander;
import com.bigdata.relation.rule.IVariableOrConstant;
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
public class FreeTextSearchExpander implements ISolutionExpander<ISPO> {
    
    protected static final Logger log = Logger.getLogger(FreeTextSearchExpander.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    private static final long serialVersionUID = 1L;
    
    private final AbstractTripleStore database;
    
    private static final long NULL = IRawTripleStore.NULL;
    
    private final Literal query;
    
    private Set<URI> graphs;
    
    public FreeTextSearchExpander(final AbstractTripleStore database,
            final Literal query) {

        if (database == null)
            throw new IllegalArgumentException();
       
        if (query == null)
            throw new IllegalArgumentException();
        
        this.database = database;
        
        this.query = query;
        
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
    public void addNamedGraphsFilter(Set<URI> graphs) {
        
        this.graphs = graphs;
        
    }
    
    private class FreeTextSearchAccessPath implements IAccessPath<ISPO> {

        private IAccessPath<ISPO> accessPath;
        
        private Hiterator<IHit> hiterator;
        
        public FreeTextSearchAccessPath(IAccessPath<ISPO> accessPath) {
            SPOPredicate pred = (SPOPredicate) accessPath.getPredicate();
            IVariableOrConstant<Long> p = pred.p();
            IVariableOrConstant<Long> o = pred.o();
            if (p.isConstant() == false || o.isConstant() == false) {
                throw new IllegalArgumentException("query not well formed");
            }
            this.accessPath = accessPath;
        }
        
        private Hiterator<IHit> getHiterator() {
            if (hiterator == null) {
                assert database!=null;
                assert query != null;
                if (database.getLexiconRelation().getSearchEngine() == null)
                    throw new UnsupportedOperationException(
                            "No free text index?");
//                final long begin = System.nanoTime();
                hiterator = database.getLexiconRelation()
                        .getSearchEngine().search(query.getLabel(),
                                query.getLanguage(), false/* prefixMatch */,
                                0d/* minCosine */, 10000/* maxRank */,
                                1000L/* timeout */, TimeUnit.MILLISECONDS);
//                hiterator = database.getSearchEngine().search
//                    ( query.getLabel(),
//                      query.getLanguage(), 
//                      0d, // @todo param for minCosine,
//                      10000 // @todo param for maxRank,
////                      timeout,
////                      unit
//                      );
//                final long elapsed = System.nanoTime() - begin;
//                log.warn("search time="
//                        + TimeUnit.MILLISECONDS.convert(elapsed,
//                                TimeUnit.NANOSECONDS)+", query="+query+", nhits="+hiterator.size());
            }
            return hiterator;
        }
        
        public IIndex getIndex() {

            return accessPath.getIndex();
            
        }

        /**
         * The results are in decreasing cosine (aka relevance) order.
         * 
         * @return <code>null</code> since the results are not in any
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

//            /*
//             * FIXME remove. times the search hit converter but has side effect.
//             */
//            {
//                final IChunkedOrderedIterator<IHit> itr2 = new ChunkedWrappedIterator<IHit>(
//                        getHiterator());
//
//                final IChunkedOrderedIterator<ISPO> itr3 = new ChunkedConvertingIterator<IHit, ISPO>(
//                        itr2, new HitConverter(accessPath));
//
//                final long begin = System.nanoTime();
//                while (itr3.hasNext()) {
//                    itr3.next();
//                }
//                final long elapsed = System.nanoTime() - begin;
//                log.error("search converting iterator time="
//                        + TimeUnit.MILLISECONDS.convert(elapsed,
//                                TimeUnit.NANOSECONDS) + ", query=" + query
//                        + ", nhits=" + hiterator.size());
//                hiterator = null; // clear reference since we will need to reobtain the hiterator.
//            }
            
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
                        BigdataValue val = database.getTerm(e.s());
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

        public long rangeCount(boolean exact) {

            final long rangeCount = getHiterator().size();

            if (INFO)
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
    
    private class HitConverter implements IChunkConverter<IHit,ISPO> {
        
        private final boolean isBound;
        
        private final long boundVal;
        
        public HitConverter(IAccessPath<ISPO> accessPath) {
            SPOPredicate pred = (SPOPredicate) accessPath.getPredicate();
            IVariableOrConstant<Long> s = pred.s();
            this.isBound = s.isConstant();
            if (INFO) log.info("isBound: " + isBound);
            this.boundVal = isBound ? s.get() : NULL;
            if (INFO) log.info("boundVal: " + boundVal);
        }

        public ISPO[] convert(IChunkedOrderedIterator<IHit> src) {
            if (DEBUG) log.debug("converting chunk");
            IHit[] hits = src.nextChunk();
            if (isBound) {
                return convertWhenBound(hits);
            }
            ISPO[] spos = new ISPO[hits.length];
            for (int i = 0; i < hits.length; i++) {
                long s = hits[i].getDocId();
                if (INFO) log.info("hit: " + s);
                spos[i] = new SPO(s, NULL, NULL);
            }
//            Arrays.sort(spos, SPOKeyOrder.SPO.getComparator());
            return spos;
        }
        
        private ISPO[] convertWhenBound(IHit[] hits) {
            ISPO[] result = new ISPO[0];
            for (IHit hit : hits) {
                long s = hit.getDocId();
                if (s == boundVal) {
                    result = new ISPO[] { new SPO(s, NULL, NULL) };
                    break;
                }
            }
            if (INFO) log.info("# of results: " + result.length);
            return result;
        }

    }

}
