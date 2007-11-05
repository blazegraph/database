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
 * Created on Nov 5, 2007
 */

package com.bigdata.rdf.inf;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.Value;

import com.bigdata.rdf.spo.ISPOFilter;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Abtract base class for buffering {@link SPO}s for some batch api operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractSPOBuffer implements ISPOBuffer {

    final public Logger log = Logger.getLogger(ISPOBuffer.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final public boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final public boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * The array in which the statements are stored.
     */
    final protected SPO[] stmts;
    
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
    public SPO get(int i) {
        
        if (i > numStmts) {

            throw new IndexOutOfBoundsException();
            
        }
        
        return stmts[i];
        
    }

    /**
     * The backing store for the batch api operation.
     */
    public final AbstractTripleStore store;
    
    /**
     * An optional filter. When present, statements matched by the filter are
     * NOT retained by the {@link SPOAssertionBuffer}.
     */
    protected final ISPOFilter filter;
    
    /**
     * The buffer capacity.
     */
    protected final int capacity;

    /**
     * Create a buffer.
     * 
     * @param store
     *            The database into which the terms and statements will be
     *            inserted.
     * @param filter
     *            Option filter. When present statements matched by the filter
     *            are NOT retained by the {@link SPOAssertionBuffer} and will NOT be
     *            added to the <i>store</i>.
     * @param capacity
     *            The maximum #of Statements, URIs, Literals, or BNodes that the
     *            buffer can hold.
     */
    protected AbstractSPOBuffer(AbstractTripleStore store, ISPOFilter filter,
            int capacity) {

        assert store != null;
        assert capacity > 0;
        
        this.store = store;

        this.filter = filter;
        
        this.capacity = capacity;

        stmts = new SPO[capacity];
        
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
        
        if (numStmts + 1 > capacity) {

            // would overflow the statement[].
            
            return true;
            
        }
        return false;
        
    }
    
    public String toString() {
        
        return super.toString()+":#stmts="+numStmts;
        
    }
    
    abstract public int flush();
    
    public boolean add( SPO stmt ) {
        
        assert stmt != null;

        if (filter != null && filter.isMatch(stmt)) {
            
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

            log.debug(stmt.toString(store));
        
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

            SPO stmt = stmts[i];

            System.err.println("#" + (i+1) + "\t" + stmt.toString(store));
            
        }
        
    }
    
}
