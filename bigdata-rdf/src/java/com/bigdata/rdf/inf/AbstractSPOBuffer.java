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
 * Created on Nov 5, 2007
 */

package com.bigdata.rdf.inf;

import org.openrdf.model.Value;

import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.ISPOBuffer;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.AbstractArrayBuffer;
import com.bigdata.relation.accesspath.IElementFilter;

/**
 * Abtract base class for buffering {@link SPO}s for some batch api operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated by {@link AbstractArrayBuffer}, but this class is more tightly
 * coupled to the {@link AbstractTripleStore}.
 */
abstract public class AbstractSPOBuffer implements ISPOBuffer {

    /**
     * The array in which the statements are stored.
     */
    final protected ISPO[] stmts;
    
    /**
     * The #of statements currently in {@link #stmts}
     */
    protected int numStmts;

    public int size() {
        
        return numStmts;
        
    }
    
    public boolean isEmpty() {
        
        return numStmts == 0;
        
    }
    
    /**
     * The {@link SPO} at the given index (used by some unit tests).
     * 
     * @param i
     * 
     * @return
     */
    public ISPO get(int i) {
        
        if (i > numStmts) {

            throw new IndexOutOfBoundsException();
            
        }
        
        return stmts[i];
        
    }

    /**
     * The value provided to the constructor.  This is only used to
     * resolve term identifers for log messages.
     */
    private final AbstractTripleStore _db;
    
    /**
     * The database in which the term identifiers are defined - this is
     * exposed ONLY for use in logging messages.
     */
    protected AbstractTripleStore getTermDatabase() {
        
        return _db;
        
    }
    
    /**
     * An optional filter. When present, statements matched by the filter are
     * NOT retained by the {@link SPOAssertionBuffer}.
     */
    protected final IElementFilter<ISPO> filter;
    
    /**
     * The buffer capacity.
     */
    protected final int capacity;

    /**
     * Create a buffer.
     * 
     * @param store
     *            The database used to resolve term identifiers in log
     *            statements (optional).
     * @param filter
     *            Option filter. When present statements matched by the filter
     *            are NOT retained by the {@link SPOAssertionBuffer} and will
     *            NOT be added to the <i>store</i>.
     * @param capacity
     *            The maximum #of Statements, URIs, Literals, or BNodes that the
     *            buffer can hold.
     */
    protected AbstractSPOBuffer(AbstractTripleStore store, IElementFilter<ISPO> filter,
            int capacity) {

        if (capacity <= 0)
            throw new IllegalArgumentException();
        
        this._db = store;

        this.filter = filter;
        
        this.capacity = capacity;

        stmts = new ISPO[capacity];
        
    }
        
    /**
     * Returns true iff there is no more space remaining in the buffer. Under
     * those conditions adding another statement to the buffer could cause an
     * overflow.
     * 
     * @return True if the buffer might overflow if another statement were
     *         added.
     */
    protected boolean nearCapacity() {
        
        if (numStmts < capacity) {

            return false;

        }
        
        // would overflow the statement[].
            
        return true;
        
    }
    
    public String toString() {
        
        return super.toString()+":#stmts="+numStmts;
        
    }
    
    abstract public int flush();

    /**
     * Cumulative counter of the #of statements actually written on the database
     * by {@link #flush()}. This is reset by {@link #flush(boolean)} when
     * <code>reset := true</code>
     */
    private int nwritten = 0;
    
    public boolean add( final ISPO stmt ) {
        
        assert stmt != null;

        if (filter != null && filter.accept(stmt)) {
            
            /*
             * Note: Do not store statements (or justifications) matched by the
             * filter.
             */

            if(DEBUG) {
                
                log.debug("filter rejects: "+stmt);
                
            }
            
            return false;
            
        }
                
        if(nearCapacity()) {

            flush();
            
        }
        
        stmts[numStmts++] = stmt;
         
        if (DEBUG) {
            
            /*
             * Note: If [store] is a TempTripleStore then this will NOT be able
             * to resolve the terms from the ids (since the lexicon is only in
             * the database).
             */

            log.debug(stmt.toString(_db));
        
        }

        return true;
        
    }
    
    /**
     * Dumps the state of the buffer on {@link System#err}.
     * 
     * @param store
     *            Used to resolve the term identifiers to {@link Value}s.
     */
    public void dump(AbstractTripleStore store) {
        
        System.err.println("capacity=" + capacity + ", numStmts=" + numStmts);
                
        for (int i = 0; i < numStmts; i++) {
            
            final ISPO stmt = stmts[i];

            System.err.println("#" + (i+1) + "\t" + stmt.toString(store));
            
        }
        
    }
    
}
