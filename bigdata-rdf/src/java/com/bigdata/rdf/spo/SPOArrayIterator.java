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
 * Created on Oct 24, 2007
 */

package com.bigdata.rdf.spo;

import java.util.Arrays;
import java.util.NoSuchElementException;

import com.bigdata.btree.IEntryIterator;
import com.bigdata.rdf.inf.SPOAssertionBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IAccessPath;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Iterator visits {@link SPO}s and supports removal (fully buffered).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOArrayIterator implements ISPOIterator {

    private boolean open = true;
    
    /**
     * The database (optional).
     */
    private AbstractTripleStore db;

    /**
     * The {@link KeyOrder} in which statements are being visited and
     * <code>null</code> if not known.
     */
    private final KeyOrder keyOrder;

//    /**
//     * Optional filter. When non-<code>null</code>, only matching statements
//     * will be vistied.
//     */
//    private final ISPOFilter filter;
    
    /** SPO buffer. */
    private SPO[] stmts;

    /** #of valid entries in {@link #stmts}. */
    private int numStmts;

    /**
     * The index of the next entry in {@link #stmts} that will be returned by
     * {@link #next()}.
     */
    private int i = 0;

    /**
     * The {@link SPO} most recently returned by {@link #next()}.
     */
    private SPO current = null;
    
    /**
     * The #of statements that this iterator buffered.
     */
    public int getStatementCount() {

        return numStmts;
        
    }

    public KeyOrder getKeyOrder() {
        
        return keyOrder;
        
    }
    
    /**
     * An iterator that visits the {@link SPO}s in the given array whose
     * {@link KeyOrder} is NOT known.
     * <p>
     * Note: This constructor variant results in an iterator that does NOT
     * support {@link #remove()} and whose {@link #getKeyOrder()} method returns
     * <code>null</code>.
     * 
     * @param stmts
     *            The statements.
     * @param numStmts
     *            The #of entries in <i>stmts</i> that are valid.
     */
    public SPOArrayIterator(SPO[] stmts, int numStmts) {

        this.db = null; // not available (remove is disabled).

        this.keyOrder = null; // not known.
        
        this.stmts = stmts;
        
        this.numStmts = numStmts;

//        this.filter = null;
        
    }

    /**
     * An iterator that visits the {@link SPO}s in the given array and
     * optionally supports {@link #remove()}.
     * 
     * @param db
     *            The database - when non-<code>null</code> the iterator will
     *            support {@link #remove()}.
     * @param keyOrder
     *            The order of the data in <i>stmts</i> and <code>null</code>
     *            IFF the order is NOT known.
     * @param stmts
     *            The statements.
     * @param numStmts
     *            The #of entries in <i>stmts</i> that are valid.
     * @param filter
     *            An optional filter. When non-<code>null</code>, only
     *            matching statements will be visited.
     */
    public SPOArrayIterator(AbstractTripleStore db, KeyOrder keyOrder,
            SPO[] stmts, int numStmts, ISPOFilter filter) {

        this.db = db; // MAY be null (remove() will be disabled).

        this.keyOrder = keyOrder; // MAY be null (implies not known).
        
        this.stmts = stmts;
        
        this.numStmts = numStmts;
        
//        this.filter = filter;

    }

    /**
     * Fully buffers all statements selected by the {@link IAccessPath}.
     * <p>
     * Note: This constructor variant supports {@link #remove()}.
     * <p>
     * Note: This constructor is much lighter weight than the
     * {@link SPOIterator} when you are trying to do an existance test (limit of
     * 1) or read only a few 100 {@link SPO}s.
     * 
     * @param db
     *            The database (MAY be null, but then {@link #remove()} is not
     *            supported).
     * 
     * @param accessPath
     *            The access path (including the triple pattern).
     * 
     * @param limit
     *            When non-zero, this is the maximum #of {@link SPO}s that will
     *            be read. When zero, all {@link SPO}s for that access path
     *            will be read and buffered.
     * 
     * @param filter
     *            An optional filter. When non-<code>null</code>, only
     *            matching statements will be visited.
     */
    public SPOArrayIterator(AbstractTripleStore db, IAccessPath accessPath,
            int limit, ISPOFilter filter) {

        if (accessPath == null)
            throw new IllegalArgumentException();

        if (limit < 0)
            throw new IllegalArgumentException();
        
        this.db = db; // MAY be null.
        
        this.keyOrder = accessPath.getKeyOrder();

//        this.filter = filter;
        
        final long rangeCount = accessPath.rangeCount();

        if (rangeCount > 10000000) {
            
            /*
             * Note: This is a relatively high limit (10M statements). You are
             * much better off processing smaller chunks!
             */
            
            throw new RuntimeException("Too many statements to read into memory: "+rangeCount);
            
        }
        
        final int n = (int) (limit > 0 ? Math.min(rangeCount, limit)
                : rangeCount);
        
        this.stmts = new SPO[ n ];

        /*
         * Materialize the matching statements.
         * 
         * @todo for scale-out, pass the filter to the data service.
         */
        
        IEntryIterator itr = accessPath.rangeQuery();

        int i = 0;

        while (itr.hasNext() && i < n) {

            SPO spo = new SPO(keyOrder, itr);
            
            if (filter != null && !filter.isMatch(spo)) {

                continue;
                
            }
            
            stmts[i++] = spo;

        }

        this.numStmts = i;
        
    }
    
    public boolean hasNext() {

        if(!open) return false;
        
        assert i <= numStmts;
        
        if (i == numStmts) {

            return false;
            
        }

        return true;
        
    }

    public SPO next() {
        
        if (!hasNext()) {

            throw new NoSuchElementException();
        
        }
        
        current = stmts[i++];
        
        return current;
        
    }

    /**
     * Removes the last statement visited from the database (non-batch API).
     * <p>
     * Note If you are trying to remove a set of statements then you are MUCH
     * better off collecting up a set of SPOs to be removed and then batching
     * the remove requests on all indices in parallel.
     * 
     * @throws UnsupportedOperationException
     *             if a ctor variant was used that did not convey the database
     *             reference.
     * 
     * FIXME modify this to collect up statements to be removed and to batch
     * remove them no later than when the iterator is exhausted (rather than
     * closed since we close the iterator in finally clauses and we only need to
     * insure removal if the itr completes normally). Do the same for
     * {@link SPOIterator}. However, note that this method is only useful if
     * you are using the iterator in its one-at-a-time mode. If you are using
     * the chunked mode of the iterator then we need a remove(SPO) method here.
     * It should just write on an interal {@link SPOAssertionBuffer} which removes the
     * statements from the database when it is flushed. The buffer should be
     * flushed when the iterator is exhausted.
     */
    public void remove() {

        assertOpen();

        if (db == null) {

            throw new UnsupportedOperationException();

        }

        if (current == null) {
            
            throw new IllegalStateException();
            
        }
       
        db.getAccessPath(current.s, current.p, current.o).removeAll();
        
        // clear the reference.
        current = null;
        
    }

    /**
     * Return the backing array.
     * 
     * @see #getStatementCount()
     */
    public SPO[] array() {

        assertOpen();

        return stmts;
        
    }

    /**
     * Returns the remaining statements.
     * 
     * @throws NoSuchElementException
     *             if {@link #hasNext()} returns false.
     */
    public SPO[] nextChunk() {
       
        if (!hasNext()) {

            throw new NoSuchElementException();
            
        }

        final SPO[] ret;
        
        if (i == 0 && numStmts == stmts.length) {
            
            /*
             * The SPO[] does not have any unused elements and nothing has been
             * returned to the caller by next() so we can just return the
             * backing array in this case.
             */
            
            ret = stmts;
            
        } else {

            /*
             * Create and return a new SPO[] containing only the statements
             * remaining in the iterator.
             */
            
            final int remaining = numStmts - i;
            
            ret = new SPO[remaining];
            
            System.arraycopy(stmts, i, ret, 0, remaining);
            
        }
        
        // indicate that all statements have been consumed.
        
        i = numStmts;
        
        return ret;
        
    }
    
    public SPO[] nextChunk(KeyOrder keyOrder) {

        if (keyOrder == null)
            throw new IllegalArgumentException();

        SPO[] stmts = nextChunk();

        if (keyOrder != this.keyOrder) {

            // sort into the required order.

            Arrays.sort(stmts, 0, stmts.length, keyOrder.getComparator());

        }

        return stmts;

    }
    
    /*
     * Note: Do NOT eagerly close the iterator since the makes it impossible to
     * implement {@link #remove()}.
     */
    public void close() {

        if (!open) {
            
            // already closed.
            
            return;
            
        }
        
        open = false;
        
        db = null;
        
        stmts = null;
        
        current = null;
        
        i = numStmts = 0;
        
    }

    private final void assertOpen() {
        
        if (!open) {

            throw new IllegalStateException();
            
        }
        
    }
    
}
