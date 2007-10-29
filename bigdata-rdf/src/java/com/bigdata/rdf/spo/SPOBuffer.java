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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.Value;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.util.concurrent.DaemonThreadFactory;

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
     * The array in which the optional {@link Justification}s are stored.
     */
    final private Justification[] justifications;
    
    /**
     * The #of statements currently in {@link #stmts}
     */
    private int numStmts;

    /**
     * The #of justifications currently in {@link #justifications}
     */
    private int numJustifications;

    /**
     * The #of statements currently in the buffer (if duplicates are not being
     * filtered then this count will include any duplicate statements).
     */
    public int size() {
        
        return numStmts;
        
    }

    /**
     * The #of justifications currently in the buffer.
     */
    public int getJustificationCount() {
        
        return numJustifications;
        
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
        
        if (i > numStmts) {

            throw new IndexOutOfBoundsException();
            
        }
        
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
     * The buffer capacity -or- <code>-1</code> if the {@link StatementBuffer} object
     * is signaling that no more buffers will be placed onto the queue by the
     * producer and that the consumer should therefore terminate.
     */
    protected final int capacity;

    /**
     * When true only distinct statements are stored in the buffer.
     */
    protected final boolean distinct;
    
    /**
     * true iff the Truth Maintenance strategy requires that we store
     * {@link Justification}s for entailments.
     */
    protected final boolean justify;
    
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
     * @param justified
     *            true iff the Truth Maintenance strategy requires that we store
     *            {@link Justification}s for entailments.
     */
    public SPOBuffer(AbstractTripleStore store, ISPOFilter filter,
            int capacity, boolean distinct, boolean justified) {

        assert store != null;
        assert capacity > 0;
        
        this.store = store;

        this.filter = filter;
        
        this.capacity = capacity;

        this.distinct = distinct;

        this.justify = justified;
        
        stmts = new SPO[capacity];

        justifications = justified ? new Justification[capacity] : null;
        
        if (distinct) {

            distinctStmtMap = new HashMap<SPO, SPO>(capacity);

        } else {

            distinctStmtMap = null;

        }

    }
        
    /**
     * Returns true iff there is no more space remaining in the buffer. Under
     * those conditions adding another statement to the buffer could cause an
     * overflow.
     * 
     * @return True if the buffer might overflow if another statement were
     *         added.
     */
    final private boolean nearCapacity() {
        
        if (numStmts + 1 > capacity) {

            // would overflow the statement[].
            
            return true;
            
        }
        
        if (numJustifications + 1 > capacity) {

            // would overflow the justification[].
            
            return true;
            
        }
        
        return false;
        
    }
     
    /**
     * Uniquify a statement.
     * <p>
     * Note: this will treat statements that are inferred vs explicit vs axioms
     * as distinct since {@link SPO#equals(SPO)} recognizes the
     * {@link StatementEnum} as a distinction. However, the {@link SPOBuffer} is
     * generally used to buffer either explicit statements or entailments but
     * not both at once.
     * <p>
     * Note: If we are storing justifications, then we want to store all
     * justifications so we CAN NOT make them unique. There for uniqueness is
     * defined solely in terms of the {@link SPO} and we ALWAYS store the
     * justification. This means that we will flush the buffer as soon as either
     * array is at capacity.
     * 
     * @param stmt
     * 
     * @return Either the given statement or the pre-existing statement with the
     *         same data.
     */
    protected SPO getDistinctStatement(SPO stmt) {

        assert distinct == true;

        SPO existingStmt = distinctStmtMap.get(stmt);

        if (existingStmt != null) {

            // return the pre-existing statement.

            return existingStmt;

        } else {

            // put the new statement in the map.

            if (distinctStmtMap.put(stmt,stmt) != null) {

                throw new AssertionError();

            }

            // return the new statement.
            return stmt;

        }

    }
    
    /**
     * A service used to write statements and justifications at the same time.
     */
    private ExecutorService indexWriteService = Executors.newFixedThreadPool(2,
            DaemonThreadFactory.defaultThreadFactory());

    /**
     * Flush any buffer statements to the backing store.
     */
    public void flush() {

        if (numStmts > 0 || numJustifications > 0) {

            log.info("numStmts=" + numStmts+", numJustifications="+numJustifications);

            final long begin = System.currentTimeMillis();
            
            if (numJustifications == 0) {
                
                // batch insert statements into the store.
                store.addStatements(stmts, numStmts);

            } else {
                
                /*
                 * Use a thread pool to write out the statement and the
                 * justifications concurrently. This drammatically reduces the
                 * latency when also writing justifications.
                 */

                List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(2);
                
                /*
                 * Note: we reject using the filter before stmts or
                 * justifications make it into the buffer so we do not need to
                 * apply the filter again here.
                 */
                
                tasks.add(new StatementWriter());
                
                tasks.add(new JustificationWriter());
                
                final List<Future<Long>> futures;
                final long elapsed_SPO;
                final long elapsed_JST;
                
                try {

                    futures = indexWriteService.invokeAll( tasks );

                    elapsed_SPO = futures.get(0).get();
                    elapsed_JST = futures.get(1).get();

                } catch(InterruptedException ex) {
                    
                    throw new RuntimeException(ex);
                    
                } catch(ExecutionException ex) {
                
                    throw new RuntimeException(ex);
                
                }

                System.err.print(" stmts="+elapsed_SPO+"ms");
                System.err.print(" justs="+elapsed_JST+"ms");
                
            }
            
            /*
             * reset the buffer.
             */

            numStmts = numJustifications = 0;

            if (distinctStmtMap != null) {

                distinctStmtMap.clear();

            }

            long elapsed = System.currentTimeMillis() - begin;

            System.err.println(" elapsed=" + elapsed + "ms");

        }

    }
    
    private class StatementWriter implements Callable<Long>{

        public Long call() throws Exception {
            
            final long begin = System.currentTimeMillis();
            
            store.addStatements(stmts, numStmts);
            
            final long elapsed = System.currentTimeMillis() - begin;
            
            return elapsed;

        }
        
    }
    
    private class JustificationWriter implements Callable<Long>{

        public Long call() throws Exception {
            
            final long begin = System.currentTimeMillis();
            
            store.addJustifications(justifications, numJustifications);
            
            final long elapsed = System.currentTimeMillis() - begin;
            
            return elapsed;

        }
        
    }
    
    /**
     * Adds an entailment together with its bindings.
     * <p>
     * When the buffer is {@link #nearCapacity()} the statements in the buffer
     * will be flushed into the backing store.
     * 
     * @param stmt
     *            The entailment.
     * @param justification
     *            The justification for that entailment (optional, depending on
     *            the TM strategy).
     * 
     * @return true if the buffer will store the statement. The buffer will
     *         reject the statement if it matches the {@link #filter}.
     * 
     * @see #nearCapacity()
     * @see #flush()
     */
    public boolean add( SPO stmt, Justification justification ) {
        
        assert stmt != null;
        
        /*
         * When justification is required the rule MUST supply a justification.
         * 
         * When it is NOT required then the rule SHOULD NOT supply a
         * justification.
         */

        assert justify ? justification != null : justification == null;
        
        if (filter != null && filter.isMatch(stmt)) {
            
            /*
             * Note: Do not store statements (or justifications) matched by the
             * filter.
             */

            if(DEBUG) {
                
                log.debug("(filtered out)"
                        + "\n"
                        + (justification == null ? stmt.toString(store)
                                : justification.toString(store)));
                
            }
            
            return false;
            
        }
        
//        if(DEBUG) {
//
//            /*
//             * This checks constraints on the RDF data model in case one of the
//             * rules is using the wrong bindings and trying to emit an
//             * entailment that is invalid per the data model. E.g., a literal in
//             * the subject position.
//             */
//            
//            if( ! (store.getTerm(stmt.s) instanceof _Resource) ) {
//                
//                throw new AssertionError(stmt.toString(store));
//                
//            }
//            
//            if( ! (store.getTerm(stmt.p) instanceof _URI) ) {
//                
//                throw new AssertionError(stmt.toString(store));
//                
//            }
//            
//            if( ! (store.getTerm(stmt.o) instanceof _Value) ) {
//                
//                throw new AssertionError(stmt.toString(store));
//                
//            }
//            
//        }
        
        if(nearCapacity()) {

            flush();
            
        }
        
        final boolean newStmt;
        
        if(distinct) {

            SPO tmp = getDistinctStatement(stmt);
            
            if(tmp==stmt) {
                
                stmts[numStmts++] = stmt;
                
                newStmt = true;
                
            } else {
                
                newStmt = false;
                
            }

            if(justify) {

                justifications[numJustifications++] = justification;
                    
            }
          
        } else {

            stmts[numStmts++] = stmt;

            newStmt = true;
            
            if(justify) { 
                
                justifications[numJustifications++] = justification;
                
            }
                
        }
        
        if (DEBUG) {
            
            /*
             * Note: If [store] is a TempTripleStore then this will NOT be able
             * to resolve the terms from the ids (since the lexicon is only in
             * the database).
             */

            log.debug((newStmt ? "(new)" : "(duplicate)")
                    + "\n"
                    + (justification == null ? stmt.toString(store)
                            : justification.toString(store)));
        
        }

        return true;
        
    }
    
    /**
     * Dumps the state of the buffer on {@link System#err}.
     * 
     * @param store
     *            Used to resolve the term identifiers to {@link Value}s.
     * 
     * @todo also dump the justifications.
     */
    public void dump(ITripleStore store) {
        
        System.err.println("capacity=" + capacity//
                + ", numStmts=" + numStmts//
                + ", numJusts=" + numJustifications//
                + (distinct ? "#distinct=" + distinctStmtMap.size() : "")//
        );
                
        for (int i = 0; i < numStmts; i++) {

            SPO stmt = stmts[i];

            System.err.println("#" + (i+1) + "\t" + stmt.toString(store));
            
        }
        
    }
    
}
