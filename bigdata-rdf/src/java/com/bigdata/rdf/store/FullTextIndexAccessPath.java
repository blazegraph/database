/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Sep 25, 2008
 */

package com.bigdata.rdf.store;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.search.FullTextIndex;
import com.bigdata.search.IHit;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.striterator.IKeyOrder;

/**
 * A custom {@link IAccessPath} supporting full text search for the
 * {@link BNS#SEARCH} magic predicate.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FullTextIndexAccessPath implements IAccessPath<IHit> {

    private final FullTextIndex textIndex;
//    private final IPredicate<IHit> predicate;
    private final String query;
    private final String languageCode;
    private final double minCosine;
    private final int maxRank;
    
    public FullTextIndexAccessPath(FullTextIndex textIndex,
//            IPredicate<IHit> predicate,
            String query, String languageCode,
            double minCosine, int maxRank) {

        if (textIndex == null)
            throw new IllegalArgumentException();

        if (query == null)
            throw new IllegalArgumentException();

        this.textIndex = textIndex;

//        this.predicate = predicate;

        this.query = query;
        
        this.languageCode = languageCode;
        
        this.minCosine = minCosine;
        
        this.maxRank = maxRank;
        
    }
    
    public IIndex getIndex() {
   
        return textIndex.getIndex();
        
    }

    public IKeyOrder<IHit> getKeyOrder() {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * FIXME This can't readily be implemented, at least not if we type the
     * {@link IAccessPath}, since the generic type of the predicate will
     * typically differ from the generic type of the visited elements.
     * <p>
     * In fact, we probably want to evaluate against specified variables in an
     * {@link IBindingSet} rather than an {@link IPredicate} <em>as bound</em>.
     * <p>
     * That is rather more general and would make it possible to write joins
     * that cross {@link IRelation}s of differing generic types.
     */
    public IPredicate<IHit> getPredicate() {
//        return predicate;
        throw new UnsupportedOperationException();
    }

    /**
     * <code>true</code> iff no hits satisify the constraints.
     * 
     * @todo cache the result iff an historical read.
     */
    public boolean isEmpty() {

        final ICloseableIterator itr = iterator(1, 0);

        try {
        
            return itr.hasNext();
            
        } finally {
            
            itr.close();
            
        }
        
    }

    final public IChunkedOrderedIterator<IHit> iterator() {
        
        return iterator(0, 0);
        
    }

    public IChunkedOrderedIterator<IHit> iterator(int limit, int capacity) {

        if (limit != 0) {

            // [limit] imposes an additional constraint.
            limit = Math.min(limit, maxRank);

        }

        return new ChunkedWrappedIterator<IHit>(textIndex.search(query,
                languageCode, minCosine, maxRank));
        
    }

    /**
     * Returns a constant for the range count unless an exact range count is
     * requested. This is because the only want to estimate the range count is
     * to evaluate the {@link #iterator()}.
     * 
     * @todo cache iff historical, including the answers since we will doubtless
     *       need them shortly.
     */
    public long rangeCount(boolean exact) {

        if (exact) {
        
            long n = 0;
            
            final ICloseableIterator itr = iterator();
            
            try {
            
                while (itr.hasNext()) {
                    
                    itr.next();
                    
                    n++;
                    
                }
                
            } finally {
                
                itr.close();
                
            }
        
        }
    
        // This is a SWAG.
        return 500;
    
    }

    /** Not supported. */
    public ITupleIterator<IHit> rangeIterator() {
        
        throw new UnsupportedOperationException();
        
    }

    /** Not supported. */
    public long removeAll() {

        throw new UnsupportedOperationException();
        
    }

}
