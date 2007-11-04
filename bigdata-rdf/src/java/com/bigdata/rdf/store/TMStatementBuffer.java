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
 * Created on Nov 1, 2007
 */

package com.bigdata.rdf.store;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.inf.ClosureStats;
import com.bigdata.rdf.inf.InferenceEngine;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.util.RdfKeyBuilder;

/**
 * An class that facilitates maintaining the RDF(S)+ closure on a database as
 * statements are asserted or retracted. One instance of this class should be
 * created for statements to be asserted against the database. Another instance
 * should be created for statements to be retracted from the database. In each
 * case, the caller writes on the {@link IStatementBuffer} interface, which will
 * overflow into a {@link TempTripleStore} maintained by this class. When all
 * data have been buffered, the caller invokes {@link #doClosure()}, which will
 * either assert or retract the statements from the database while maintaining
 * the RDF(S)+ closure over the database. Whether statements are asserted or
 * retracted depends on a constructor parameter.
 * 
 * @todo for concurrent data writers, this class should probably allocate an
 *       {@link RdfKeyBuilder} provisioned according to the target database and
 *       attach it to the {@link StatementBuffer}. Alternatively, have the
 *       {@link StatementBuffer} do that. In either case, the batch API on the
 *       {@link AbstractTripleStore} should then use the {@link RdfKeyBuilder}
 *       attached to the {@link StatementBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TMStatementBuffer implements IStatementBuffer {

    /**
     * Logger.
     */
    public static final Logger log = Logger.getLogger(TMStatementBuffer.class);
    
    private final int bufferCapacity;
    
    /**
     * The target database.
     */
    protected final AbstractTripleStore database;
    
    /**
     * The object used to compute entailments for that database.
     */
    protected final InferenceEngine inferenceEngine;

    /**
     * The {@link StatementBuffer}.
     */
    private IStatementBuffer buffer;

    /**
     * Returns the {@link StatementBuffer} used to buffer statements.
     */
    public IStatementBuffer getStatementBuffer() {

        if (buffer == null) {

            buffer = new StatementBuffer(getTempStore(), database,
                    bufferCapacity);

        }

        return buffer;

    }
    
    private TempTripleStore tempStore;

    /**
     * Return the {@link TempTripleStore} that will be used to buffer
     * assertions. The {@link TempTripleStore} is lazily allocated since it may
     * have been released by {@link #clear()}.
     */
    protected TempTripleStore getTempStore() {

        if (tempStore == null) {

            tempStore = new TempTripleStore(database.getProperties());
            
        }
        
        return tempStore;
        
    }

    /**
     * Adds a statement to the buffer.
     * 
     * @param s
     * @param p
     * @param o
     */
    public void add(Resource s, URI  p, Value o) {
        
        if(buffer==null) {
            
            buffer = getStatementBuffer();
            
        }
        
        buffer.add(s, p, o);
        
    }
    
    /**
     * 
     * 
     * @param inferenceEngine
     *            The inference engine for the database.
     * @param bufferCapacity
     *            The capacity of the buffer.
     * @param assertionBuffer
     *            When <code>true</code> the buffer contains statements that
     *            are being asserted against the database. When
     *            <code>false</code> it contains statements that are being
     *            retracted from the database. This flag determines how truth
     *            maintenance is performed when the buffer is {@link #flush()}.
     * 
     * @todo max in memory size for the temporary store?
     * @todo delete the temporary store on close if it is larger than X?
     */
    public TMStatementBuffer(InferenceEngine inferenceEngine, int bufferCapacity, boolean assertionBuffer) {
                
        log.info("bufferCapacity="+bufferCapacity);

        this.database = inferenceEngine.database;

        this.inferenceEngine = inferenceEngine;

        this.bufferCapacity = bufferCapacity;
        
        this.assertionBuffer = assertionBuffer;

    }

    private final boolean assertionBuffer;

    /**
     * When <code>true</code> the buffer contains statements that are being
     * asserted against the database. When <code>false</code> it contains
     * statements that are being retracted from the database. This flag
     * determines how truth maintenance is performed by {@link #flush()}.
     */
    public boolean isAssertionBuffer() {
        
        return assertionBuffer;
        
    }
    
    /**
     * Flushes statements to the {@link TempTripleStore}. 
     */
    public void flush() {

        if (buffer != null) {

            buffer.flush();
            
        }
        
    }
    
    /**
     * The buffered statements are asserted on (retracted from) the database
     * along with their entailments (truth maintenance). Whether statements and
     * their entailments are asserted or retracted depends on
     * {@link #isAssertionBuffer()}.
     */
    public ClosureStats doClosure() {

        if(isAssertionBuffer()) {
            
            return assertAll();
            
        } else {
            
            return retractAll();
            
        }

    }
    
    /**
     * Discards all buffered statements.
     */
    public void clear() {

        if (tempStore != null && tempStore.getBackingStore().isOpen()) {

            tempStore.close();

        }

        tempStore = null;

        buffer = null;

//        /*
//         * clear the temporary store (drops the indices).
//         */
//
//        log.info("Clearing the temporary store");
//
//        tempStore.clear();
//
//        /*
//         * If the temporary store has grown "too large" then delete it and
//         * create a new one.
//         * 
//         * Note: The backing store is a WORM (wrote once, read many).
//         * Therefore it never shrinks in size. By periodically deleting the
//         * backing store we avoid having it consume too much space on the
//         * disk.
//         */
//
//        TemporaryStore backingStore = tempStore.getBackingStore();
//
//        if (backingStore.getBufferStrategy().getNextOffset() > 200 * Bytes.megabyte) {
//
//            log.info("Closing the temporary store");
//
//            // delete the backing file.
//            tempStore.close();
//
//            // clear the reference.
//            tempStore = null;
//
//        }

    }
    
    /**
     * Wipes out the entailments from the database (this requires a full scan on
     * all statement indices).
     * 
     * @todo implement and move to {@link AbstractTripleStore}.
     */
    public void removeEntailments() {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Closes the temporary store against the database, writing entailments into
     * the temporary store and then copying the entire temporary store into the
     * database.
     */    
    public ClosureStats assertAll() {
        
        final ClosureStats stats;

        if(isEmpty()) {
            
            // nothing to assert.
            
            return new ClosureStats();
            
        }
        
        // flush anything to the temporary store.
        flush();
        
        if (tempStore == null) {

            // Should exist since flushed and not empty.
         
            throw new AssertionError();
            
        }

        final int nbeforeClosure = tempStore.getStatementCount();

        log.info("Computing closure of the temporary store with "
                + nbeforeClosure + " statements");

        stats = inferenceEngine.computeClosure(tempStore);

        final int nafterClosure = tempStore.getStatementCount();

        log.info("There are " + nafterClosure
                + " statements in the temporary store after closure");

        // measure time for these other operations as well.

        final long begin = System.currentTimeMillis();

        /*
         * copy statements from the temporary store to the database.
         */

        log.info("Copying statements from the temporary store to the database");

        int ncopied = tempStore.copyStatements(database, null/*filter*/);

        // note: this is the number that are _new_ to the database.
        log.info("Copied " + ncopied
                + " statements that were new to the database.");

        // discard everything.
        clear();

        stats.elapsed += System.currentTimeMillis() - begin;

        return stats;

    }

    /**
     * FIXME Implement TM for statement retraction.
     * 
     * @todo return statistics.
     */
    public ClosureStats retractAll() {

        final ClosureStats stats;
        
        if(isEmpty()) {
            
            // nothing to retract.
            
            return new ClosureStats();
            
        }
        
        // flush anything to the temporary store.
        flush();
        
        if (tempStore == null) {

            // Should exist since flushed and not empty.
         
            throw new AssertionError();
            
        }

        /*
         * FIXME do closure
         * 
         * Only explicit statements were added to the tempStore.
         * 
         * For each statement in the tempStore, determine whether or not it is
         * has a grounded justification chain. If yes, then change the statement
         * type to inferred and we are done with that statement.
         * 
         * If there is no grounded justification chain for a statement then it
         * gets put into a buffer, writing on yet another tempStore.
         * 
         * Once all statement in this tempStore have been processed, we delete
         * it using clear().
         * 
         * We then compute the closure of the new tempStore against the database
         * in order to discover additional statements that may no longer be
         * supported and hence will have to be retracted. This closure operation
         * is similar to the incremental load closure but with two twists: (1)
         * we do NOT generate justification chains; and (2) we do NOT copy the
         * result onto the database - instead we leave it in the new tempStore.
         * Once we have the new set of statements to consider for retraction we
         * simply invoke retractAll() again on that tempStore.
         */
        
        clear();

        throw new UnsupportedOperationException();

        // return stats;

    }

    public AbstractTripleStore getStatementStore() {
        
        return getTempStore();
        
    }

    public AbstractTripleStore getDatabase() {

        return database;
        
    }

    public boolean isEmpty() {

        if (buffer != null && !buffer.isEmpty())
            return false;

        if (tempStore != null && tempStore.getStatementCount() > 0)
            return false;

        return true;
        
    }

    public int size() {

        int n = 0;
        
        if (buffer != null) n += buffer.size();

        if (tempStore != null) n+=tempStore.getStatementCount();
        
        return n;
        
    }
    
}
