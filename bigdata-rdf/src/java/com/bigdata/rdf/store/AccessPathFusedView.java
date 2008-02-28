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
 * Created on Oct 31, 2007
 */

package com.bigdata.rdf.store;

import java.util.Iterator;

import com.bigdata.btree.FusedEntryIterator;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.rdf.spo.ISPOFilter;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPOArrayIterator;
import com.bigdata.rdf.util.KeyOrder;

import cutthecrap.utils.striterators.Striterator;

/**
 * A read-only fused view of two access paths obtained for the same statement
 * index in two different databases.
 * 
 * @todo write tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AccessPathFusedView implements IAccessPath {

    private final IAccessPath path1, path2;
    
    /**
     * 
     */
    public AccessPathFusedView(IAccessPath path1, IAccessPath path2) {
        
        if (path1 == null)
            throw new IllegalArgumentException();
        
        if (path2 == null)
            throw new IllegalArgumentException();
        
        if (path1 == path2)
            throw new IllegalArgumentException();

        if(path1.getKeyOrder() != path2.getKeyOrder())
            throw new IllegalArgumentException();

        long[] triplePattern1 = path1.getTriplePattern();
        
        long[] triplePattern2 = path2.getTriplePattern();
        
        for(int i=0; i<IRawTripleStore.N; i++) {
            
            if (triplePattern1[i] != triplePattern2[i])
                throw new IllegalArgumentException();
            
        }
        
        this.path1 = path1;
        
        this.path2 = path2;
        
    }

    public long[] getTriplePattern() {

        return path1.getTriplePattern();
        
    }

    public KeyOrder getKeyOrder() {

        return path1.getKeyOrder();
        
    }

    public boolean isEmpty() {

        return path1.isEmpty() && path2.isEmpty();
        
    }

    public long rangeCount() {

        // Note: this is the upper bound.
        
        // @todo check for overflow on Long#Max_value
        
        return path1.rangeCount() + path2.rangeCount();
        
    }

    public ITupleIterator rangeQuery() {

        /*
         * @todo The modification to drive the range iterator capacity, flags
         * and filter through everywhere might have broken FusedEntryIterator
         * for this call in the case where the source iterators are not
         * requesting either the keys or the values.
         * 
         * FIXME This MUST specify ALLVERSIONS for the source iterators in order
         * for the fused view to be able to recognize a deleted index entry and
         * discard a historical undeleted entry later in the predence order for
         * the view.
         */
        return new FusedEntryIterator(
                false, // ALLVERSIOSN
                new ITupleIterator[] {
                path1.rangeQuery(),//
                path2.rangeQuery()//
                }
        );
                
    }

    /**
     * @todo This fully buffers everything.  It should be modified to incrementally
     * read from both of the underlying {@link ISPOIterator}s.
     */
    public ISPOIterator iterator() {

        return iterator(null/*filter*/);

    }

    public ISPOIterator iterator(ISPOFilter filter) {

        return new SPOArrayIterator(null/* db */, this, 0/* no limit */, filter);

    }

    /**
     * @todo This fully buffers everything.  It should be modified to incrementally
     * read from both of the underlying {@link ISPOIterator}s.
     */
    public ISPOIterator iterator(int limit, int capacity) {

        return iterator(limit,capacity,null/*filter*/);
        
    }

    public ISPOIterator iterator(int limit, int capacity, ISPOFilter filter) {

        return new SPOArrayIterator(null/* db */, this, limit, filter);
        
    }

    public Iterator<Long> distinctTermScan() {

        return new Striterator(path1.distinctTermScan()).append(path2
                .distinctTermScan());

    }

    public int removeAll() {
        
        throw new UnsupportedOperationException();
        
    }

    public int removeAll(ISPOFilter filter) {
        
        throw new UnsupportedOperationException();
        
    }

}
