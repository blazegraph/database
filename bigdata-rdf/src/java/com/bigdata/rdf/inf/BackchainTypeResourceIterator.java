/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Oct 30, 2007
 */

package com.bigdata.rdf.inf;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.accesspath.ClosableEmptyIterator;
import com.bigdata.relation.accesspath.ClosableSingleItemIterator;
import com.bigdata.relation.accesspath.IChunkedIterator;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.accesspath.IClosableIterator;
import com.bigdata.relation.accesspath.IKeyOrder;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.Striterator;

/**
 * Provides backward chaining for (x rdf:type rdfs:Resource).
 * <p>
 * Note: You only need to do this on read from a high level query language since
 * the rest of the RDFS rules will run correctly without the (x rdf:type
 * rdfs:Resource) entailments being present. Further, you only need to do this
 * when the {@link InferenceEngine} was instructed to NOT store the (x rdf:type
 * rdfs:Resource) entailments.
 * <p>
 * Note: This iterator will NOT generate an inferred (x rdf:type rdfs:Resource)
 * entailment iff there is an explicit statement (x rdf:type rdfs:Resource) in
 * the database.
 * 
 * @see InferenceEngine
 * @see InferenceEngine.Options
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */ 
public class BackchainTypeResourceIterator implements IChunkedOrderedIterator<SPO> {
    
    protected final static transient long NULL = IRawTripleStore.NULL;
    
    private final IChunkedOrderedIterator<SPO> _src;
    private final Iterator<SPO> src;
//    private final long s;
//    private final AbstractTripleStore db;
    private final long rdfType, rdfsResource;
    private final IKeyOrder<SPO> keyOrder;

    /**
     * The subject(s) whose (s rdf:type rdfs:Resource) entailments will be
     * visited.
     */
    private IClosableIterator<Long> resourceIds;
    
    /**
     * An iterator reading on the {@link SPOKeyOrder#POS} index. The predicate is
     * bound to <code>rdf:type</code> and the object is bound to
     * <code>rdfs:Resource</code>. If the subject was given to the ctor, then
     * it will also be bound.
     */
    private IChunkedOrderedIterator<SPO> posItr;
    
    private boolean sourceExhausted = false;
    
    private boolean open = true;

    /**
     * This is set each time by {@link #nextChunk()} and inspected by
     * {@link #nextChunk(IKeyOrder)} in order to decide whether the chunk needs
     * to be sorted.
     */
    private IKeyOrder<SPO> chunkKeyOrder = null; 

    /**
     * The last {@link SPO} visited by {@link #next()}.
     */
    private SPO current = null;
    
    /**
     * Create an iterator that will visit all statements in the source iterator
     * and also backchain any entailments of the form (x rdf:type rdfs:Resource)
     * which are valid for the given triple pattern.
     * 
     * @param src
     *            The source iterator. {@link #nextChunk()} will sort statements
     *            into the {@link IKeyOrder} reported by this iterator (as long
     *            as the {@link IKeyOrder} is non-<code>null</code>).
     * @param s
     *            The subject of the triple pattern.
     * @param p
     *            The predicate of the triple pattern.
     * @param o
     *            The object of the triple pattern.
     * @param db
     *            The database from which we will read the distinct subject
     *            identifiers (iff this is an all unbound triple pattern).
     * @param rdfType
     *            The term identifier that corresponds to rdf:Type for the
     *            database.
     * @param rdfsResource
     *            The term identifier that corresponds to rdf:Resource for the
     *            database.
     */
    public BackchainTypeResourceIterator(IChunkedOrderedIterator<SPO> src,
            long s, long p, long o, AbstractTripleStore db, final long rdfType,
            final long rdfsResource) {
        
        if (src == null)
            throw new IllegalArgumentException();
        
        if (db == null)
            throw new IllegalArgumentException();
        
        this._src = src;
        
        // filters out (x rdf:Type rdfs:Resource).
        this.src = new Striterator(src).addFilter(new Filter(){

            private static final long serialVersionUID = 1L;

            protected boolean isValid(Object arg0) {

                SPO o = (SPO) arg0;

                if (o.p == rdfType && o.o == rdfsResource) {
                    
                    return false;
                    
                }
                
                return true;
            }});
        
        this.keyOrder = src.getKeyOrder(); // MAY be null.

//        this.s = s;
        
        if (s == NULL
                && (p == NULL || p == rdfType)
                && (o == NULL || o == rdfsResource)
                ) {

            /*
             * Backchain will generate one statement for each distinct subject
             * or object in the store.
             * 
             * @todo This is Ok as long as you are forward chaining all of the
             * rules that put a predicate or an object into the subject position
             * since it will then have all resources. If you backward chain some
             * of those rules, e.g., rdf1, then you MUST change this to read on
             * the ids index and skip anything that is marked as a literal using
             * the low bit of the term identifier but you will overgenerate for
             * resources that are no longer in use by the KB.
             */

            resourceIds = new MergedOrderedIterator(
                    db.getSPORelation().distinctTermScan(SPOKeyOrder.SPO),
                    db.getSPORelation().distinctTermScan(SPOKeyOrder.OSP)
                    );

            /*
             * Reading (? rdf:Type rdfs:Resource) using the POS index.
             */

            posItr = db.getAccessPath(NULL, rdfType, rdfsResource,
                    ExplicitSPOFilter.INSTANCE).iterator();

        } else if ((p == NULL || p == rdfType)
                && (o == NULL || o == rdfsResource)) {

            /*
             * Backchain will generate exactly one statement: (s rdf:type
             * rdfs:Resource).
             */

            resourceIds = new ClosableSingleItemIterator<Long>(s);

            /*
             * Reading a single point (s type resource), so this will actually
             * use the SPO index.
             */

            posItr = db.getAccessPath(s, rdfType, rdfsResource,
                    ExplicitSPOFilter.INSTANCE).iterator();

        } else {

            /*
             * Backchain will not generate any statements.
             */

            resourceIds = new ClosableEmptyIterator<Long>();

            posItr = null;
            
        }

//        this.db = db;
        
        this.rdfType = rdfType;
        
        this.rdfsResource = rdfsResource;
        
    }

    public IKeyOrder<SPO> getKeyOrder() {

        return keyOrder;
        
    }

    public void close() {

        if(!open) return;
        
        // release any resources here.
        
        open = false;

        _src.close();

        resourceIds.close();
        
        resourceIds = null;
        
        if (posItr != null) {

            posItr.close();
            
        }
        
    }

    public boolean hasNext() {
        
        if (!open) {

            // the iterator has been closed.
            
            return false;
            
        }

        if (!sourceExhausted) {

            if (src.hasNext()) {

                // still consuming the source iterator.

                return true;

            }

            // the source iterator is now exhausted.

            sourceExhausted = true;

            _src.close();

        }

        if (resourceIds.hasNext()) {

            // still consuming the subjects iterator.
            
            return true;
            
        }
        
        // the subjects iterator is also exhausted so we are done.
        
        return false;
        
    }

    /**
     * Visits all {@link SPO}s visited by the source iterator and then begins
     * to backchain ( x rdf:type: rdfs:Resource ) statements.
     * <p>
     * The "backchain" scans two iterators: an {@link IChunkedOrderedIterator}
     * on <code>( ? rdf:type
     * rdfs:Resource )</code> that reads on the database
     * (this tells us whether we have an explicit
     * <code>(x rdf:type rdfs:Resource)</code> in the database for a given
     * subject) and iterator that reads on the term identifiers for the distinct
     * resources in the database (this bounds the #of backchained statements
     * that we will emit).
     * <p>
     * For each value visited by the {@link #resourceIds} iterator we examine
     * the statement iterator. If the next value that would be visited by the
     * statement iterator is an explicit statement for the current subject, then
     * we emit the explicit statement. Otherwise we emit an inferred statement.
     */
    public SPO next() {

        if (!hasNext()) {

            throw new NoSuchElementException();
            
        }

        if (src.hasNext()) {

            current = src.next();

        } else {

            final long s = resourceIds.next();
            /*
             * FIXME should have member field of last visiting posItr and scan
             * from that until GTE [s]. if GT then emit inference else emit
             * explicit. and write unit test for this.
             */
            final SPO tmp = (posItr.hasNext() ? posItr.next() : null);
            
            if (tmp != null) {
                
                current = tmp;
                
            } else {
            
                current = new SPO(s, rdfType, rdfsResource,
                        StatementEnum.Inferred);
                
            }

        }
    
        return current;
        
    }

    /**
     * Note: This method preserves the {@link IKeyOrder} of the source iterator
     * iff it is reported by {@link #getKeyOrder()}. Otherwise chunks read from
     * the source iterator will be in whatever order that iterator is using
     * while chunks containing backchained entailments will be in
     * {@link SPOKeyOrder#POS} order.
     * <p>
     * Note: In order to ensure that a consistent ordering is always used within
     * a chunk the backchained entailments will always begin on a chunk
     * boundary.
     */
    public SPO[] nextChunk() {

        final int chunkSize = 10000;
        
        if (!hasNext())
            throw new NoSuchElementException();
        
        if(!sourceExhausted) {
            
            /*
             * Return a chunk from the source iterator.
             * 
             * Note: The chunk will be in the order used by the source iterator.
             * If the source iterator does not report that order then
             * [chunkKeyOrder] will be null.
             */
            
            chunkKeyOrder = keyOrder;

            SPO[] s = new SPO[chunkSize];

            int n = 0;
            
            while(src.hasNext() && n < chunkSize ) {
                
                s[n++] = src.next();
                
            }
            
            SPO[] stmts = new SPO[n];
            
            // copy so that stmts[] is dense.
            System.arraycopy(s, 0, stmts, 0, n);
            
            return stmts;
            
        }

        /*
         * Create a "chunk" of entailments.
         * 
         * Note: This chunk will be in natural POS order since that is the index
         * that we scan to decide whether or not there was an explicit ( x
         * rdf:type rdfs:Resource ) while we consume the [subjects] in termId
         * order.
         */
        
        long[] s = new long[chunkSize];
        
        int n = 0;
        
        while(resourceIds.hasNext() && n < chunkSize ) {
            
            s[n++] = resourceIds.next();
            
        }
        
        SPO[] stmts = new SPO[n];
        
        for(int i=0; i<n; i++) {
            
            stmts[i] = new SPO(s[i], rdfType, rdfsResource,
                    StatementEnum.Inferred);
            
        }
                
        if (keyOrder != null && keyOrder != SPOKeyOrder.POS) {

            /*
             * Sort into the same order as the source iterator.
             * 
             * Note: We have to sort explicitly since we are scanning the POS
             * index
             */

            Arrays.sort(stmts, 0, stmts.length, keyOrder.getComparator());

        }

        /*
         * The chunk will be in POS order since that is how we are scanning the
         * indices.
         */
        
        chunkKeyOrder = SPOKeyOrder.POS;
        
        return stmts;
        
    }

    public SPO[] nextChunk(IKeyOrder<SPO> keyOrder) {
        
        if (keyOrder == null)
            throw new IllegalArgumentException();

        final SPO[] stmts = nextChunk();
        
        if (chunkKeyOrder != keyOrder) {

            // sort into the required order.

            Arrays.sort(stmts, 0, stmts.length, keyOrder.getComparator());

        }

        return stmts;
        
    }

    /**
     * Note: You can not "remove" the backchained entailments. If the last
     * statement visited by {@link #next()} is "explicit" then the request is
     * delegated to the source iterator.
     */
    public void remove() {

        if (!open)
            throw new IllegalStateException();
        
        if (current == null)
            throw new IllegalStateException();
        
        if(current.isExplicit()) {
            
            /*
             * Delegate the request to the source iterator.
             */
            
            src.remove();
            
        }
        
        current = null;
        
    }
    
    /**
     * Reads on two iterators visiting elements in some natural order and visits
     * their order preserving merge (no duplicates).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <T>
     */
    private static class MergedOrderedIterator<T extends Long> implements IChunkedIterator<T> {
       
        private final IChunkedIterator<T> src1;
        private final IChunkedIterator<T> src2;
        
        public MergedOrderedIterator(IChunkedIterator<T> src1,IChunkedIterator<T> src2) {

            this.src1 = src1;
            
            this.src2 = src2;
            
        }
        
        public void close() {
            
            src1.close();
            
            src2.close();
            
        }

        /**
         * Note: Not implemented since not used above and this class is private.
         */
        public T[] nextChunk() {
            throw new UnsupportedOperationException();
        }

        public boolean hasNext() {

            return src1.hasNext() || src2.hasNext();
            
        }
        
        private T tmp1;
        private T tmp2;
        
        public T next() {

            if(!hasNext()) throw new NoSuchElementException();
            
            if (tmp1 == null && src1.hasNext()) {

                tmp1 = src1.next();

            }
 
            if (tmp2 == null && src2.hasNext()) {

                tmp2 = src2.next();

            }
            
            if (tmp1 == null) {

                final T tmp = tmp2;

                tmp2 = null;

                return tmp;

            }
            
            if (tmp2 == null) {

                final T tmp = tmp1;

                tmp1 = null;

                return tmp;

            }

            final int cmp = tmp1.compareTo(tmp2);

            if (cmp == 0) {

                final T tmp = tmp1;

                tmp1 = tmp2 = null;

                return tmp;

            } else if (tmp1.compareTo(tmp2) < 0) {

                final T tmp = tmp1;

                tmp1 = null;

                return tmp;

            } else {

                final T tmp = tmp2;

                tmp2 = null;

                return tmp;

            }
            
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }
    
}
