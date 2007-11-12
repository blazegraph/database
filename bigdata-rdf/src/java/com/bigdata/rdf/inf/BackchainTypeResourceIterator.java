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
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.util.KeyOrder;

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
public class BackchainTypeResourceIterator implements ISPOIterator {
    
    private final ISPOIterator _src;
    private final Iterator<SPO> src;
//    private final long s;
//    private final AbstractTripleStore db;
    private final long rdfType, rdfsResource;
    private final KeyOrder keyOrder;

    /**
     * The subject(s) whose (s rdf:type rdfs:Resource) entailments will be
     * visited.
     */
    private Iterator<Long> resourceIds;
    
    /**
     * An iterator reading on the {@link KeyOrder#POS} index. The predicate is
     * bound to <code>rdf:type</code> and the object is bound to
     * <code>rdfs:Resource</code>. If the subject was given to the ctor, then
     * it will also be bound.
     */
    private ISPOIterator posItr;
    
    private boolean sourceExhausted = false;
    
    private boolean open = true;

    /**
     * This is set each time by {@link #nextChunk()} and inspected by
     * {@link #nextChunk(KeyOrder)} in order to decide whether the chunk needs
     * to be sorted.
     */
    private KeyOrder chunkKeyOrder = null; 

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
     *            into the {@link KeyOrder} reported by this iterator (as long
     *            as the {@link KeyOrder} is non-<code>null</code>).
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
    public BackchainTypeResourceIterator(ISPOIterator src, long s, long p,
            long o, AbstractTripleStore db, final long rdfType, final long rdfsResource) {
        
        if (src == null)
            throw new IllegalArgumentException();
        
        if (db == null)
            throw new IllegalArgumentException();
        
        this._src = src;
        
        // filters out (x rdf:Type rdfs:Resource).
        this.src = new Striterator(src).addFilter(new Filter(){

            private static final long serialVersionUID = 1L;

            protected boolean isValid(Object arg0) {
                
                SPO o = (SPO)arg0;
                
                if(o.p==rdfType && o.o==rdfsResource) {
                    
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
             * in the store.
             * 
             * @todo This is Ok as long as you are forward chaining all of the
             * rules that put a predicate or an object into the subject position
             * since it will then have all resources. If you backward chain some
             * of those rules, e.g., rdf1, then you MUST change this to read on
             * the ids index and skip anything that is marked as a literal using
             * the low bit of the term identifier.
             */

            resourceIds = db.getAccessPath(KeyOrder.SPO).distinctTermScan();

            /*
             * Reading (? rdf:Type rdfs:Resource) using the POS index.
             */
            
            posItr = db.getAccessPath(NULL, rdfType, rdfsResource).iterator(ExplicitSPOFilter.INSTANCE);

        } else if ((p == NULL || p == rdfType)
                && (o == NULL || o == rdfsResource)) {

            /*
             * Backchain will generate exactly one statement: (s rdf:type
             * rdfs:Resource).
             */

            resourceIds = Arrays.asList(new Long[] { s }).iterator();
            
            /*
             * Reading a single point (s type resource), so this will actually
             * use the SPO index.
             */
            
            posItr = db.getAccessPath(s, rdfType, rdfsResource).iterator(ExplicitSPOFilter.INSTANCE);

        } else {

            /*
             * Backchain will not generate any statements.
             */

            resourceIds = Arrays.asList(new Long[] {}).iterator();

            posItr = null;
            
        }

//        this.db = db;
        
        this.rdfType = rdfType;
        
        this.rdfsResource = rdfsResource;
        
    }

    public KeyOrder getKeyOrder() {

        return keyOrder;
        
    }

    public void close() {

        if(!open) return;
        
        // release any resources here.
        
        open = false;

        _src.close();

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
     * The "backchain" scans two iterators: an ISPOIterator on ( ? rdf:type
     * rdfs:Resource ) that reads on the database (this tells us whether we have
     * an explicit (x rdf:type rdfs:Resource) in the database for a given
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
     * Note: This method preserves the {@link KeyOrder} of the source iterator
     * iff it is reported by {@link ISPOIterator#getKeyOrder()}. Otherwise
     * chunks read from the source iterator will be in whatever order that
     * iterator is using while chunks containing backchained entailments will be
     * in {@link KeyOrder#POS} order.
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
                
        if (keyOrder != null && keyOrder != KeyOrder.POS) {

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
        
        chunkKeyOrder = KeyOrder.POS;
        
        return stmts;
        
    }

    public SPO[] nextChunk(KeyOrder keyOrder) {
        
        if (keyOrder == null)
            throw new IllegalArgumentException();

        SPO[] stmts = nextChunk();
        
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
    
}
