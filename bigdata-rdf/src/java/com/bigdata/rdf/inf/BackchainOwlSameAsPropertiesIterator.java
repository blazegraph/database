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
 * Created on March 11, 2008
 */

package com.bigdata.rdf.inf;

import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Provides backward chaining for property collection and reverse property
 * collection on owl:sameAs for all access paths.
 * <p>
 * Note:
 * 
 * @see BackchainOwlSameAsPropertiesSPOIterator
 * @see BackchainOwlSameAsPropertiesSPIterator
 * @see BackchainOwlSameAsPropertiesPOIterator
 * @see BackchainOwlSameAsPropertiesPIterator
 
 * @see InferenceEngine
 * @see InferenceEngine.Options
 *
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */ 
public class BackchainOwlSameAsPropertiesIterator implements ISPOIterator {
    
    private ISPOIterator delegate;
    
    /**
     * Create an iterator that will visit all statements in the source iterator
     * and also backchain any entailments that would have resulted from
     * owl:sameAs {2,3}.
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
     * @param sameAs
     *            The term identifier that corresponds to owl:sameAs for the
     *            database.
     */
    public BackchainOwlSameAsPropertiesIterator(ISPOIterator src, long s, long p,
            long o, AbstractTripleStore db, final long sameAs) {
        
        if (src == null)
            throw new IllegalArgumentException();
        
        if (db == null)
            throw new IllegalArgumentException();
        
        if ( s != NULL && o != NULL ) {
            this.delegate = new BackchainOwlSameAsPropertiesSPOIterator(src,s,p,o,db,sameAs);
        }
        else
        if ( s != NULL && o == NULL ) {
            this.delegate = new BackchainOwlSameAsPropertiesSPIterator(src,s,p,db,sameAs);
        }
        else
        if ( s == NULL && o != NULL ) {
            this.delegate = new BackchainOwlSameAsPropertiesPOIterator(src,p,o,db,sameAs);
        }
        else
        if ( s == NULL && o == NULL ) {
            this.delegate = new BackchainOwlSameAsPropertiesPIterator(src,p,db,sameAs);
        }
        
    }
    
    public KeyOrder getKeyOrder() 
    {
        return delegate.getKeyOrder();
    }

    public boolean hasNext() 
    {
        return delegate.hasNext();
    }

    public SPO next() 
    {
        return delegate.next();
    }

    public SPO[] nextChunk() 
    {
        return delegate.nextChunk();
    }

    public SPO[] nextChunk(KeyOrder keyOrder) 
    {
        return delegate.nextChunk(keyOrder);
    }

    public void close() 
    {
        delegate.close();
    }

    public void remove() 
    {
        delegate.remove();
    }

}
