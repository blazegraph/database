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
 * Created on Apr 13, 2007
 */

package com.bigdata.rdf.spo;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.rdf.rio.Buffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.store.TempTripleStore;

/**
 * A buffer for {@link SPO}s that are flushed on overflow into a backing
 * {@link TempTripleStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOBuffer {

    final public Logger log = Logger.getLogger(SPOBuffer.class);

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
    final private SPO[] stmts;
    
    /**
     * The #of statements currently in the buffer.
     */
    private int numStmts;

    /**
     * The #of statements currently in the buffer (if duplicates are not being
     * filtered then this count will include any duplicate statements).
     */
    public int size() {
        
        return numStmts;
        
    }
    
    /**
     * True iff there are no statements in the buffer.
     */
    public boolean isEmpty() {
        
        return numStmts == 0;
        
    }
    
    /**
     * The {@link SPO} at the given index.
     * @param i
     * @return
     */
    public SPO get(int i) {
        
        if (i > numStmts)
            throw new IndexOutOfBoundsException();
        
        return stmts[i];
        
    }
    
    /**
     * Map used to filter out duplicate statements. 
     */
    final private Map<SPO,SPO> distinctStmtMap;

    /**
     * The backing store into which the statements are added when the buffer
     * overflows.
     */
    protected final AbstractTripleStore store;
    
    /**
     * The backing store into which the statements are added when the buffer
     * overflows.
     */
    public AbstractTripleStore getBackingStore() {
        
        return store;
        
    }
    
    /**
     * An optional filter. When present, statements matched by the filter are
     * NOT retained by the {@link SPOBuffer} and are NOT added to the database.
     */
    protected final ISPOFilter filter;
    
    /**
     * The buffer capacity -or- <code>-1</code> if the {@link Buffer} object
     * is signaling that no more buffers will be placed onto the queue by the
     * producer and that the consumer should therefore terminate.
     */
    protected final int capacity;

    /**
     * When true only distinct statements are stored in the buffer.
     */
    protected final boolean distinct;
    
    /**
     * Create a buffer.
     * 
     * @param store
     *            The database into which the terms and statements will be
     *            inserted.
     * @param capacity
     *            The maximum #of Statements, URIs, Literals, or BNodes that the
     *            buffer can hold.
     * @param distinct
     *            When true only distinct terms and statements are stored in the
     *            buffer.
     */
    public SPOBuffer(AbstractTripleStore store, int capacity, boolean distinct) {
     
        this(store,null,capacity,distinct);
        
    }

    /**
     * Create a buffer.
     * 
     * @param store
     *            The database into which the terms and statements will be
     *            inserted.
     * @param filter
     *            Option filter. When present statements matched by the filter
     *            are NOT retained by the {@link SPOBuffer} and are NOT added to
     *            the <i>store</i>.
     * @param capacity
     *            The maximum #of Statements, URIs, Literals, or BNodes that the
     *            buffer can hold.
     * @param distinct
     *            When true only distinct terms and statements are stored in the
     *            buffer.
     */
    public SPOBuffer(AbstractTripleStore store, ISPOFilter filter, int capacity, boolean distinct) {
    
        assert store != null;
        assert capacity > 0;
        
        this.store = store;

        this.filter = filter;
        
        this.capacity = capacity;

        this.distinct = distinct;

        stmts = new SPO[capacity];

        if (distinct) {

            distinctStmtMap = new HashMap<SPO, SPO>(capacity);

        } else {

            distinctStmtMap = null;

        }

    }
        
    /**
     * Returns true there are no slots remaining in the statements array. Under
     * those conditions adding another statement to the buffer could cause an
     * overflow.
     * 
     * @return True if the buffer might overflow if another statement were
     *         added.
     */
    final private boolean nearCapacity() {
                
        if (numStmts + 1 > capacity)
            return true;
        
        return false;
        
    }
    
    /**
     * Uniquify a statement.
     * 
     * @param stmt
     * 
     * @return Either the statement or the pre-existing statement in the buffer
     *         with the same data.
     */
    protected SPO getDistinctStatement(SPO stmt) {

        assert distinct == true;
        
        SPO existingStmt = distinctStmtMap.get(stmt);
        
        if (existingStmt != null) {
            
            // return the pre-existing statement.
            
            return existingStmt;
            
        } else {

            // put the new statement in the map.
            
            if (distinctStmtMap.put(stmt, stmt) != null) {
                
                throw new AssertionError();
                
            }

            // return the new statement.
            return stmt;
            
        }
        
    }
    
    /**
     * Flush any buffer statements to the backing store.
     */
    public void flush() {

        if (numStmts > 0) {

            log.info("numStmts=" + numStmts);

            /*
             * batch insert statements into the store.
             */
            store.addStatements(stmts, numStmts);

            /*
             * reset the buffer.
             */

            numStmts = 0;

            if (distinctStmtMap != null) {

                distinctStmtMap.clear();

            }

        }

    }
    
    /**
     * Adds the statement into the buffer. When the buffer is
     * {@link #nearCapacity()} the statements in the buffer will be flushed into
     * the backing store.
     * 
     * @param stmt
     *            The statement.
     * 
     * @see #nearCapacity()
     */
    public void add( SPO stmt ) {

        if(filter!=null && filter.isMatch(stmt)) {
        
            // Do not store statements matched by the filter.
            return;
            
        }
        
        if(nearCapacity()) {

            flush();
            
        }
        
        if(distinct) {

            SPO tmp = getDistinctStatement(stmt);

            if(tmp.count++ == 0){
           
                stmts[numStmts++] = tmp;

            }
          
        } else {

            stmts[numStmts++] = stmt;

        }

        if (DEBUG) {
            
            /*
             * Note: If [store] is a TempTripleStore then this will NOT be able
             * to resolve the terms from the ids (since the lexicon is only in
             * the database).
             */
            log.debug("add " + stmt.toString(store));
        
        }

    }

    /**
     * Dumps the state of the buffer on {@link System#err}.
     * 
     * @param store
     *            The terms in the statements are resolved against this store.
     */
    public void dump(ITripleStore store) {
        
        System.err.println("capacity="+capacity);
        
        System.err.println("numStmts="+numStmts);
        
        if(distinct) {
            
            System.err.println("#distinct="+distinctStmtMap.size());
            
        }
        
        for (int i = 0; i < numStmts; i++) {

            SPO stmt = stmts[i];

            System.err.println("#" + i + "\t"
                    + store.toString(stmt.s, stmt.p, stmt.o));
            
        }
        
    }
    
}
