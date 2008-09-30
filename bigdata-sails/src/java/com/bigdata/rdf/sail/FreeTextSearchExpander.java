package com.bigdata.rdf.sail;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.ISolutionExpander;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.search.Hiterator;
import com.bigdata.search.IHit;
import com.bigdata.striterator.ChunkedConvertingIterator;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.IChunkConverter;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

public class FreeTextSearchExpander implements ISolutionExpander<ISPO> {
    
    protected static final Logger log = Logger.getLogger(FreeTextSearchExpander.class);
    
    
    private static final long serialVersionUID = 1L;
    
    private final AbstractTripleStore database;
    
    private final Literal query;
    
    public FreeTextSearchExpander(AbstractTripleStore database, Literal query) {
        this.database = database;
        this.query = query;
        log.info("creating free text search expander for: " + query);
    }
    
    public IAccessPath<ISPO> getAccessPath(
            final IAccessPath<ISPO> accessPath) {
        return new FreeTextSearchAccessPath(accessPath);
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
                hiterator = database.getSearchEngine().search
                    ( query.getLabel(), query.getLanguage(), 
                      0d/* minCosine */, 10000/* maxRank */
                      );                
            }
            return hiterator;
        }
        
        public IIndex getIndex() {
            return accessPath.getIndex();
        }

        public IKeyOrder<ISPO> getKeyOrder() {
            return accessPath.getKeyOrder();
        }

        public IPredicate<ISPO> getPredicate() {
            return accessPath.getPredicate();
        }

        public boolean isEmpty() {
            return rangeCount(true) > 0;
        }

        public IChunkedOrderedIterator<ISPO> iterator() {
            log.info("iterator");
            final IChunkedOrderedIterator<IHit> itr2 = 
                new ChunkedWrappedIterator<IHit>(getHiterator());
            final IChunkedOrderedIterator<ISPO> itr3 = 
                new ChunkedConvertingIterator<IHit,ISPO>
                ( itr2, new HitConverter(accessPath)
                  );
            return itr3;
        }

        public IChunkedOrderedIterator<ISPO> iterator(int limit, int capacity) {
            return iterator();
        }

        public IChunkedOrderedIterator<ISPO> iterator(long offset, long limit, int capacity) {
            return iterator();
        }

        public long rangeCount(boolean exact) {
            long rangeCount = 1;
            rangeCount = getHiterator().size();
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
        private final long p, o;
        
        public HitConverter(IAccessPath<ISPO> accessPath) {
            SPOPredicate pred = (SPOPredicate) accessPath.getPredicate();
            this.p = ((IConstant<Long>) pred.p()).get();
            this.o = ((IConstant<Long>) pred.o()).get();
        }

        public ISPO[] convert(IChunkedOrderedIterator<IHit> src) {
            IHit[] hits = src.nextChunk();
            ISPO[] spos = new ISPO[hits.length];
            for (int i = 0; i < hits.length; i++) {
                long s = hits[i].getDocId();
                spos[i] = new SPO(s, database.NULL, database.NULL);
            }
            Arrays.sort(spos, SPOKeyOrder.SPO.getComparator());
            return spos;
        }
    }
};