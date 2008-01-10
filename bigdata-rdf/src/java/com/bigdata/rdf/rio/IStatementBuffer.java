/*

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
 * Created on Nov 2, 2007
 */

package com.bigdata.rdf.rio;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Abstraction for buffering statements.
 * 
 * @todo review javadoc here and on implementations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IStatementBuffer {

    /**
     * The optional store into which statements will be inserted when non-<code>null</code>.
     */
    public AbstractTripleStore getStatementStore();

    /**
     * The database that will be used to resolve terms.  When {@link #getStatementStore()}
     * is <code>null</code>, statements will be written into this store as well.
     */
    public AbstractTripleStore getDatabase();

    /**
     * True iff the buffer is empty.
     */
    public boolean isEmpty();

    /**
     * The #of statements in the buffer.
     */
    public int size();
    
    /**
     * Resets the state of the buffer (any pending writes are discarded).
     */
    public void clear();

    /**
     * Batch insert buffered data (terms and statements) into the store.
     */
    public void flush();

    /**
     * Add an "explicit" statement to the buffer with a "null" context.
     * 
     * @param s
     * @param p
     * @param o
     */
    public void add(Resource s, URI p, Value o);
    
    /**
     * Add an "explicit" statement to the buffer.
     * 
     * @param s
     * @param p
     * @param o
     * @param c
     */
    public void add(Resource s, URI p, Value o, Resource c);

}
