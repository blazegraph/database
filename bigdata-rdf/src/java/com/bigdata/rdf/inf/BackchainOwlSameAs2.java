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
 * Created on Nov 9, 2007
 */

package com.bigdata.rdf.inf;

import it.unimi.dsi.mg4j.util.BloomFilter;

import com.bigdata.btree.BTree;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Backchains {@link RuleOwlSameAs2}.
 * <p>
 * Approach:
 * 
 * <p>
 * Stage 1: we deliver all results from the source iterator collecting up the
 * set of distinct subjects in a {@link BTree} backed by a
 * {@link TemporaryRawStore} and updating a {@link BloomFilter} whose key is the
 * statement's fully bound term identifiers in SPO order.
 * <p>
 * Stage 2: we do an ordered read on the {@link BTree}. for each distinct
 * subject <code>s</code>, we issue a query <code>(s owl:sameAs y)</code>,
 * collecting up the set <code>Y</code>, where <code>y != s</code> of
 * resource that are the same as that subject. (We do not actually need to
 * collect that set - its members can be visited incrementally.)
 * <p>
 * Stage 3: For each <code>y</code> in <code>Y</code> we obtain an iterator
 * reading on <code>(y p o)</code> where <code>p</code> and <code>o</code>
 * are the predicate and object specified to the constructor and MAY be unbound.
 * For each statement visited by that iterator we test the bloom filter. If it
 * reports NO then this statement does NOT duplicate a statement visited in
 * Stage 1 and we deliver it via {@link #next()}. If it reports YES then this
 * statement MAY duplicate a statement delivered in Stage 1 and we add it into
 * an {@link ISPOBuffer}.
 * <p>
 * When the {@link ISPOBuffer} overflows we sort it and perform an ordered
 * existence test on the database. If the statement is found in the database
 * then it is discarded from the buffer. The remaining statements in the buffer
 * are then delivered via {@link #next()}.
 * <p>
 * Note: This assumes that backchaining can only discover duplicates of
 * statements delivered by the source iterator and NOT duplicates of statements
 * discovered by backchaining itself. If this is NOT true then you need to add
 * each backchained statement to the {@link BloomFilter} as well.
 * 
 * @todo if predicate is sameAs then semantics must be an empty iterator.
 * 
 * @todo must avoid duplicates.
 * @todo must use ordered reads.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BackchainOwlSameAs2 implements ISPOIterator {

    private final ISPOIterator src;
    private final long owlSameAs;
    private final KeyOrder keyOrder;

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
     * 
     * @param src
     * @param s
     * @param p
     * @param o
     * @param db
     * @param owlSameAs
     */
    public BackchainOwlSameAs2(ISPOIterator src, long s, long p, long o,
            AbstractTripleStore db, final long owlSameAs) {

        if (src == null)
            throw new IllegalArgumentException();
        
        if (db == null)
            throw new IllegalArgumentException();
        
        this.src = src;
        
        this.keyOrder = src.getKeyOrder(); // MAY be null.
                
        this.owlSameAs = owlSameAs;
        
    }

    public KeyOrder getKeyOrder() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean hasNext() {
        // TODO Auto-generated method stub
        return false;
    }

    public SPO next() {
        // TODO Auto-generated method stub
        return null;
    }

    public SPO[] nextChunk() {
        // TODO Auto-generated method stub
        return null;
    }

    public SPO[] nextChunk(KeyOrder keyOrder) {
        // TODO Auto-generated method stub
        return null;
    }

    public void close() {
        // TODO Auto-generated method stub

    }

    public void remove() {
        // TODO Auto-generated method stub

    }

}
