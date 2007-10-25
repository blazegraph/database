/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Oct 24, 2007
 */

package com.bigdata.rdf.spo;

import java.util.Arrays;
import java.util.NoSuchElementException;

import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IAccessPath;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.rdf.util.RdfKeyBuilder;

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
     * Lazily allocated by {@link #remove()}.
     */
    private RdfKeyBuilder keyBuilder = null;
    
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
     */
    public SPOArrayIterator(AbstractTripleStore db, KeyOrder keyOrder, SPO[] stmts, int numStmts) {

        this.db = db; // MAY be null (remove() will be disabled).

        this.keyOrder = keyOrder; // MAY be null (implies not known).
        
        this.stmts = stmts;
        
        this.numStmts = numStmts;

    }

    /**
     * Fully buffers all statements selected by the {@link IAccessPath}.
     * <p>
     * Note: This constructor variant supports {@link #remove()}.
     * 
     * @param accessPath
     *            The access path (including the triple pattern).
     */
    public SPOArrayIterator(IAccessPath accessPath) {
        
        keyOrder = accessPath.getKeyOrder();

        final int rangeCount = accessPath.rangeCount();
        
        this.stmts = new SPO[rangeCount];

        // materialize the matching statements.
        IEntryIterator itr = accessPath.rangeQuery();

        int i = 0;

        while (itr.hasNext()) {

            Object val = itr.next();

            stmts[i++] = new SPO(keyOrder, itr.getKey(), val);

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
        
        assertOpen();

        if (!hasNext()) {

            throw new NoSuchElementException();
        
        }
        
        return stmts[i++];
        
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
     */
    public void remove() {

        assertOpen();

        if(db==null) {
            
            throw new UnsupportedOperationException();
            
        }
        
        if(current==null) {
            
            throw new IllegalStateException();
            
        }
        
        if(keyBuilder == null) {
            
            keyBuilder = new RdfKeyBuilder(new KeyBuilder(3 * Bytes.SIZEOF_LONG));
            
        }
        
        long s = current.s;
        long p = current.p;
        long o = current.o;
        
        // @todo Do this in parallel threads with AbstractTripleStore#indexWriteService
        
        db.getSPOIndex().remove(keyBuilder.statement2Key(s, p, o));
        db.getPOSIndex().remove(keyBuilder.statement2Key(p, o, s));
        db.getOSPIndex().remove(keyBuilder.statement2Key(o, s, p));
        
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
     * Returns the remaining statements and {@link #close() closes} the
     * iterator.
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
        
        close();
        
        return ret;
        
    }
    
    public SPO[] nextChunk(KeyOrder keyOrder) {

        if (keyOrder == null)
            throw new IllegalArgumentException();

        SPO[] stmts = nextChunk();

        if (keyOrder != this.keyOrder) {

            // sort into the required order.

            Arrays.sort(stmts, 0, stmts.length, keyOrder.getSPOComparator());

        }

        return stmts;

    }
    
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
        
        keyBuilder = null;
        
    }

    private final void assertOpen() {
        
        if (!open) {

            throw new IllegalStateException();
            
        }
        
    }
    
}
