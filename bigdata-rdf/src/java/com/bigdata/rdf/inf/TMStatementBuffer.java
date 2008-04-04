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
 * Created on Nov 1, 2007
 */

package com.bigdata.rdf.inf;

import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.ISPOFilter;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOArrayIterator;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IAccessPath;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.rdf.util.KeyOrder;
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
 * @todo refactor to TMSPOBuffer with only a wrapper for converting from the
 *       Sesame object model into the SPO model.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TMStatementBuffer implements IStatementBuffer {

    /**
     * Logger.
     */
    public static final Logger log = Logger.getLogger(TMStatementBuffer.class);
    
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

            Properties properties = database.getProperties();
            
//            // turn off justifications for the tempStore.
//            properties.setProperty(Options.JUSTIFY, "false");
        
            // turn off the lexicon since we will only use the statement indices.
            properties.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.LEXICON, "false");
            
            tempStore = new TempTripleStore(properties);
            
        }
        
        return tempStore;
        
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

    /**
     * Adds a statement to the buffer.
     * <p>
     * Note: You MUST NOT submit a statement that is not an explicit statement
     * in the database to the retraction buffer! This error is NOT checked.
     * Correctness here is easy to achieve when the statements to be removed are
     * drawn from a triple pattern. Simply add an {@link ExplicitSPOFilter} to
     * the {@link IAccessPath#iterator(ISPOFilter)} reading on the triple
     * pattern and you will NOT see anything except explicit statements. Those
     * statements can then be added to the {@link BufferEnum#RetractionBuffer}
     * safely using this method.
     * 
     * @param s
     * @param p
     * @param o
     */
    public void add(Resource s, URI  p, Value o) {
    
        add(s,p,o,null);
        
    }

    public void add(Resource s, URI  p, Value o,Resource c) {
        
        if(buffer==null) {
            
            buffer = getStatementBuffer();
            
        }
        
        buffer.add(s, p, o, c);
        
    }
    
    public static enum BufferEnum {
        
        /**
         * A buffer that is used to support incremental truth maintenance of the
         * database as new statements are asserted. The buffer contains explicit
         * statements.  When the closure is computed, the entailments of those
         * statements are computed and are stored in the buffer.  Finally, the
         * contents of the buffer are copied into the database, thereby adding
         * both the explicitly asserted statements and their entailments.
         */
        AssertionBuffer,
        
        /**
         * A buffer that is used to support incremental truth maintenance of the
         * database as statements are retracted. Explicit statements that are to
         * be retracted from the database are written onto this buffer NOT
         * directly deleted on the database. When the closure is computed, each
         * statement is examined to determine whether or not it is still
         * entailed by the data remaining in the buffer NOT considering the
         * statements that were explicitly retracted. Statements that are still
         * provable are converted to inferences.
         * <p>
         * Statements which can no longer be proven are (a) retracted from the
         * database along with their {@link Justification}s and (b) inserted
         * into a {@link #TruthMaintenanceBuffer} for further processing.
         */
        RetractionBuffer;
        
    }
    
    /**
     * 
     * 
     * @param inferenceEngine
     *            The inference engine for the database.
     * @param bufferCapacity
     *            The capacity of the buffer.
     * @param bufferType
     *            When <code>true</code> the buffer contains statements that
     *            are being asserted against the database. When
     *            <code>false</code> it contains statements that are being
     *            retracted from the database. This flag determines how truth
     *            maintenance is performed when the buffer is {@link #flush()}.
     * 
     * @todo max in memory size for the temporary store?
     */
    public TMStatementBuffer(InferenceEngine inferenceEngine, int bufferCapacity, BufferEnum bufferType) {
                
        log.info("bufferCapacity="+bufferCapacity);

        this.database = inferenceEngine.database;

        this.inferenceEngine = inferenceEngine;

        this.bufferCapacity = bufferCapacity;
        
        this.bufferType = bufferType;

    }

    private final BufferEnum bufferType;

    /**
     * When <code>true</code> the buffer contains statements that are being
     * asserted against the database. When <code>false</code> it contains
     * statements that are being retracted from the database. This flag
     * determines how truth maintenance is performed by {@link #doClosure()}.
     */
    public boolean isAssertionBuffer() {
        
        return bufferType == BufferEnum.AssertionBuffer;
        
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

        if (tempStore != null && tempStore.getStore().isOpen()) {

            try {

                tempStore.close();
                
            } catch (Throwable t) {
             
                log.warn("Problem closing temp store: " + t);
                
            }

        }

        tempStore = null;

        buffer = null;

    }
    
    /**
     * Any statements in the <i>fousStore</i> that are already in the database
     * are converted to explicit statements (iff they are not already explicit)
     * and <strong>removed</strong> from the <i>focusStore</i> as a
     * side-effect. This prevents the application of the rules to data that is
     * already known to the database.
     * 
     * @param focusStore
     *            The store whose closure is being computed.
     * @param database
     *            The database.
     * @param filter
     *            An optional filter. Statements matching the filter are NOT
     *            written on the database, but they are still removed from the
     *            focusStore.
     * 
     * @return The #of statements that were removed from the focusStore.
     * 
     * @todo this uses some techniques that are not scaleable if the focusStore
     *       is extremely large.
     */
    static public int applyExistingStatements(AbstractTripleStore focusStore,
            AbstractTripleStore database, ISPOFilter filter) {
        
        log.info("Filtering statements already known to the database");

        final long begin = System.currentTimeMillis();
        
        /*
         * Visit explicit statements in the focusStore (they should all be
         * explicit).
         */

        final ISPOIterator itr = focusStore.getAccessPath(KeyOrder.SPO)
                .iterator(ExplicitSPOFilter.INSTANCE);

        int nremoved = 0;
        
        int nupgraded = 0;
        
        try {

            final long focusStoreSize = focusStore.getStatementCount();
            
            final int capacity = (int) Math.min(focusStoreSize, 1000000);
            
            /*
             * This buffer will write on the database causing any statement that
             * is found in the focusStore and already known to the database to
             * be made into an explicit statement in the database.
             */

            final SPOAssertionBuffer assertionBuffer = new SPOAssertionBuffer(
                    database, database, filter, capacity, false/* justified */);

            /*
             * This buffer will retract statements from the tempStore that are
             * already present as explicit statements in the database.
             */
            
            final SPORetractionBuffer retractionBuffer = new SPORetractionBuffer(
                    focusStore, capacity);


            while (itr.hasNext()) {

                SPO[] chunk = itr.nextChunk();

                for (SPO spo : chunk) {

                    // Lookup the statement in the database.
                    SPO tmp = database.getStatement(spo.s, spo.p, spo.o);
                    
                    if (tmp != null) {

                        // The statement is known to the database.

                        if (tmp.type == StatementEnum.Explicit) {
                            
                            /*
                             * Since the statement is already explicit in the
                             * database we just delete it from the tempStore.
                             */
                            
                            retractionBuffer.add(spo);

                            nremoved++;
                            
                        } else {

                            /*
                             * The statement was not explicit in the database so
                             * we buffer it. When the buffer is flushed, the
                             * statement will be written onto the database and
                             * made explicit.
                             */
                                
                            assertionBuffer.add(spo);
                            
                        }
                        
                    }

                }

            }

            // flush buffers.
            
            assertionBuffer.flush();
            
            retractionBuffer.flush();

        } finally {

            itr.close();

        }

        final long elapsed = System.currentTimeMillis() - begin;

        log.info("Removed " + nremoved + " statements from the focusStore"
                + " and upgraded " + nupgraded
                + " statements in the database in " + elapsed + " ms.");
        
        return nremoved;
        
    }
    
    /**
     * Perform truth maintenance for incremental data load.
     * <p>
     * This method computes the closure of the temporary store against the
     * database, writing entailments into the temporary store. Once all
     * entailments have been computed, it then copies the all statements in the
     * temporary store into the database and deletes the temporary store.
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

        final long nbeforeClosure = tempStore.getStatementCount();

        log.info("Computing closure of the temporary store with "
                + nbeforeClosure + " statements");

        /*
         * For each statement in the tempStore that is already in the database,
         * we convert the statement to an explicit statement (if it is not
         * already explicit) and REMOVE the statement from the from the
         * tempStore as a side-effect. This prevents the application of the
         * rules to data that is already known to the database.
         * 
         * Note: the efficiency of this step depends greatly on the nature of
         * the data that are being loaded. If someone loads data that contains a
         * copy of an ontology already in the database, then filtering avoids
         * the cost of the needless reclosure of that ontology against the
         * database. On the other hand, filtering when "data" (vs "schema") is
         * loaded provides little benefit.
         * 
         * Note: we pass along the DoNotAddFilter. This will let in all explicit
         * statements as presently configured so it is here only to keep things
         * consistent if we change our mind about that practice.
         */

        applyExistingStatements(tempStore, database, inferenceEngine.doNotAddFilter);

        stats = inferenceEngine.computeClosure(tempStore);

        final long nafterClosure = tempStore.getStatementCount();

        log.info("There are " + nafterClosure
                + " statements in the temporary store after closure");

        // measure time for these other operations as well.

        final long begin = System.currentTimeMillis();

        /*
         * copy statements from the temporary store to the database.
         */

        log.info("Copying statements from the temporary store to the database");

//        tempStore.dumpStore(database,true,true,false,true);
        int ncopied = tempStore.copyStatements(database, null/*filter*/, true /*copyJustifications*/);
//        database.dumpStore(database,true,true,false,true);

        // note: this is the number that are _new_ to the database.
        log.info("Copied " + ncopied
                + " statements that were new to the database.");

        // discard everything.
        clear();

        final long elapsed = System.currentTimeMillis() - begin;
        
        stats.elapsed += elapsed;

        log.info("Computed closure in "+elapsed+"ms");
        
        return stats;

    }

    /**
     * Perform truth maintenance for statement retraction.
     * 
     * @return statistics about the closure operation.
     */
    public ClosureStats retractAll() {

        final long begin = System.currentTimeMillis();
        
        final ClosureStats stats = new ClosureStats();
        
        if(isEmpty()) {
            
            // nothing to retract.
            
            return stats;
            
        }
        
        // flush anything to the temporary store.
        flush();
        
        if (tempStore == null) {

            // Should exist since flushed and not empty.
         
            throw new AssertionError();
            
        }

        // #of given statements to retract.
        final long ngiven = tempStore.getStatementCount();
        
        log.info("Computing closure of the temporary store with "
                + ngiven+ " statements");

        // do truth maintenance.
        retractAll(stats,tempStore,0);
        
        MDC.remove("depth");
        
        assert ! tempStore.getStore().isOpen();
        
        /*
         * The tempStore will have been closed, but also release our hard
         * references.
         */
        
        clear();

        final long elapsed = System.currentTimeMillis() - begin;
        
        log.info("Retracted " + ngiven
                + " given and updated closure on the database in " + elapsed
                + " ms");
        
        return stats;
        
    }
    
    /**
     * <p>
     * Do recursive truth maintenance.
     * </p>
     * <p>
     * Note: When this is first called, the tempStore SHOULD contain only those
     * statements that were known to be explicit statements in the database.
     * However, when this is invoked recursively the tempStore will contain only
     * inferred statements -- these are statements that are part of the closure
     * of the original statements that were retracted and whose grounded
     * justifications we are now seeking. Regardless of whether this is invoke
     * with the explicitly given set of statements to retracted or (recursively)
     * with inferences that may need to be retracted, the statements in the
     * tempStore MUST still in the database (they should have been copied to the
     * tempStore, not removed from the database).
     * </p>
     * <p>
     * The steps are:
     * <ol>
     * 
     * <li> Create a focusStore into which we will write all statements that
     * will not be grounded once the statements in the tempStore have been
     * deleted from the database. In the first pass this will contain the
     * original explicit statements which are no longer provable. In subsequent
     * (recursive) passes it will contain any inferences drawn from those
     * statements which are themselves no longer provable.</li>
     * 
     * <li> For each statement in the tempStore, determine whether or not it is
     * has a grounded justification chain. If yes, then change the statement
     * type to inferred in the database and we are done with that statement.
     * </li>
     * 
     * <li> If there is no grounded justification chain for a statement then it
     * gets put into a {@link SPOAssertionBuffer} writing on the focusStore and
     * into an {@link SPORetractionBuffer} writing on the database (so that the
     * statement and its {@link Justification}s get deleted from the database).
     * </li>
     * 
     * <li> Once all statements in the tempStore have been processed we flush
     * the various buffers and {@link TempTripleStore#closeAndDelete()} the
     * tempStore. </li>
     * 
     * <li> We then compute the closure of the focusStore against the database
     * in order to discover additional statements that may no longer be
     * supported and hence will have to be retracted. This closure operation is
     * similar to the incremental load closure but with two twists: (1) we do
     * NOT generate {@link Justification} chains; and (2) we do NOT copy the
     * result onto the database - instead we leave it in the focusStore. </li>
     * 
     * <li> Once we have the new set of statements to consider for retraction we
     * simply invoke retractAll() again on that focusStore. </li>
     * 
     * </ol>
     * </p>
     * 
     * @param stats
     * @param tempStore
     * @param depth
     *            Recursive depth - this is ZERO(0) the first time the method is
     *            called. At depth ZERO(0) the tempStore MUST contain only the
     *            explicit statements to be retracted.
     */
    private void retractAll(ClosureStats stats, TempTripleStore tempStore, int depth) {

        MDC.put("depth", "depth="+depth);
        
        final long tempStoreCount = tempStore.getStatementCount();

        log.info("Doing truth maintenance with " + tempStoreCount
                + " statements : depth="+depth);
        
        /*
         * Temp store used to absorb statements for which no grounded
         * justification chain could be discovered.
         */

        TempTripleStore focusStore = new TempTripleStore(database
                .getProperties());
        
        // consider each statement in the tempStore.
        ISPOIterator itr = tempStore.getAccessPath(KeyOrder.SPO).iterator();

        final long nretracted;
        try {

            /*
             * Buffer writing on the [focusStore] removes any statements that are no longer grounded.
             */

            SPOAssertionBuffer ungroundedBuffer = new SPOAssertionBuffer( 
                    focusStore, database, null/* filter */, bufferCapacity, false/* justified */);

            /*
             * Buffer used to downgrade explicit statements that are still
             * entailed by the database to inferred statements.
             * 
             * Note: If the statement is already present AND it is marked as
             * inferred then this will NOT write on the statement index.
             */

            SPOAssertionBuffer downgradeBuffer = new SPOAssertionBuffer( 
                    focusStore, database, inferenceEngine.doNotAddFilter,
                    10000/* capacity */, false/* justify */);

            /*
             * Buffer used to retract statements from the database after we have
             * determined that those statements are no longer provable from the
             * database without relying on the statements that the caller
             * originally submitted for retraction.
             */

            SPORetractionBuffer retractionBuffer = new SPORetractionBuffer(
                    database, 10000/* capacity */);

            /*
             * Note: when we enter this method recursively statements in the
             * tempStore are entailments of statements that have been retracted
             * but they MAY correspond to explicit statements in the database.
             * We set this constant so that isGrounded will test for that.
             */
            final boolean testHead = depth > 0;
            
            /*
             * When we enter this method for the 1st time the tempStore will
             * contain only explicit statements that exist in the database. In
             * this case we can not allow the presence of the statement in the
             * database to serve as grounds for itself -- or for anything else
             * that we are going to be retracting in that 1st round. Therefore
             * we instruct isGrounded to test for the presence of the statement
             * in the focusStore. if it is there then it is NOT considered
             * support since we are in the processing of retracting that
             * statement.
             */
            final boolean testFocusStore = depth == 0;

            while (itr.hasNext()) {

                SPO[] chunk = itr.nextChunk();

                for (SPO spo : chunk) {

                    assert spo.isFullyBound();
                    
                    if (depth == 0) {

                        /*
                         * At depth zero the statements to be retracted should
                         * be fully bound and explicit.
                         */
                        
                        assert spo.isExplicit();
                        
                    }

                    if (DEBUG) {

                        SPO tmp = database.getStatement(spo.s, spo.p, spo.o);

                        if (tmp == null) {

                            throw new IllegalArgumentException(
                                    "Given statement not in database: "
                                            + spo.toString(database));

                        }

                        if(depth==0) {

                            if (tmp.type != StatementEnum.Explicit) {

                                throw new IllegalArgumentException(
                                    "Given statement not explicit in database: "
                                            + tmp.toString(database));
                            
                            }
                            
                        }

                    }
                    
                    if( spo.type==StatementEnum.Axiom ) {

                        /*
                         * Ignore.
                         */
                        log.info("Ignoring axiom in the tempStore: "+spo);
                        
                    } else if( depth>0 && spo.type==StatementEnum.Explicit ) {
                        
                        /*
                         * Ignore. If an explicit statement is discover by
                         * closure then we do nothing.
                         * 
                         * @todo since closure produces inferences (rather than
                         * explicit statements) this block is probably never
                         * executed.
                         */
                        log.info("Ignoring explicit statement in the tempStore at depth="+depth+", "+spo);
                        throw new AssertionError(); // verify that we never execute this block.
                        
                    } else if (inferenceEngine.isAxiom(spo.s, spo.p, spo.o)) {
                        
                        /*
                         * Convert back to an axiom. We need this in case an
                         * explicit statement is being retracted that is also an
                         * axiom.
                         * 
                         * @todo this block should only run at depth zero. at
                         * depth greater than zero we only see inferences and we
                         * are keeping axioms from being converted to inferences
                         * in the DoNotAddFilter.
                         */
                        
                        SPO tmp = new SPO(spo.s, spo.p, spo.o,
                                StatementEnum.Axiom);

                        tmp.override = true;

                        downgradeBuffer.add(tmp, null);                        

                        log.info("Downgrading to axiom: "+spo.toString(database));

                    } else if (depth == 0
                            && Justification.isGrounded(inferenceEngine, tempStore, database, spo, testHead, testFocusStore)) {

                        /*
                         * Add a variant of the statement that is marked as
                         * "inferred" rather than as "explicit" to the
                         * buffer. When the buffer is flushed the statement
                         * will be written onto the database.
                         * 
                         * @todo consider returning the grounded
                         * justification and then writing it onto the
                         * database where. This will essentially "memoize"
                         * grounded justifications. Of course, you still
                         * have to verify that there is support for the
                         * justification (the statements in the tail of the
                         * justification still exist in the database).
                         */

                        SPO tmp = new SPO(spo.s, spo.p, spo.o,
                                StatementEnum.Inferred);

                        tmp.override = true;

                        downgradeBuffer.add(tmp, null);

                        log.info("Downgrading to inferred: "
                                + spo.toString(database));
                        
                    } else if (depth > 0
                            && Justification.isGrounded(inferenceEngine, tempStore, database, spo, testHead, testFocusStore)) {
                        
                        /*
                         * Ignore.
                         */
                        
                    } else {

                        /*
                         * The statement (and its justifications) will be
                         * removed from the database when the buffer is
                         * flushed.
                         */

                        retractionBuffer.add(spo);
                        
                        log.info("Retracting: "+spo.toString(database));

                        /*
                         * The ungrounded statement will be added to the
                         * focusStore. Once all such ungrounded statements
                         * have been collected we will compute their closure
                         * against the database.
                         * 
                         * That closure (less the statements that we
                         * explicitly wrote into the focusStore) will be
                         * used to search recursively for additional
                         * statements which may no longer be grounded.
                         */

                        ungroundedBuffer.add(spo);

                    }

                }

            }

            // flush buffers, logging counters.

            log.info("#downgraded="+downgradeBuffer.flush());

            nretracted = retractionBuffer.flush();
            
            log.info("#retracted="+nretracted);

            log.info("#ungrounded="+ungroundedBuffer.flush());

        } finally {

            itr.close();

        }

        // drop the tempStore.
        tempStore.closeAndDelete();

        if(nretracted==0) {
            
            log.info("Done - nothing was retracted from the database");
            
            return;
            
        }
        
        if(DEBUG && database.getStatementCount()<200) {
            
            System.err.println("dumping database after retraction: depth="+depth);

            database.dumpStore(true, true, false);
            
        }
        
        long focusStoreCount = focusStore.getStatementCount();

        if (focusStoreCount == 0) {

            // Done.

            log.info("Done - focus store is empty after retraction.");

            return;

        }

        /*
         * Compute the closure of the focusStore against the database.
         * 
         * Note: We subtract out the statements that we put into the
         * [focusStore] after we compute its closure since they have already
         * been deleted from the database.
         */
        {

            /*
             * Suck everything in the focusStore into an SPO[].
             */
            
            SPOArrayIterator tmp = new SPOArrayIterator(focusStore, focusStore
                    .getAccessPath(KeyOrder.SPO), 0/* limit */, null/* filter */);
            
            if(DEBUG && database.getStatementCount()<200) {
                
                System.err.println("focusStore before closure: depth="+depth);
                
                focusStore.dumpStore(database,true,true,false);
            
            }

            // compute closure of the focus store.

            stats.add( inferenceEngine.computeClosure(focusStore,false/*justify*/) );

            if(DEBUG && database.getStatementCount()<200) {
                
                System.err.println("focusStore after closure: depth="+depth);
                
                focusStore.dumpStore(database,true,true,false);
            
            }
            
            // subtract out the statements we used to start the closure.
            int nremoved = focusStore.removeStatements(tmp);
            
            if(DEBUG && database.getStatementCount()<200) {
                
                System.err.println("focusStore after subtracting out tmp: depth="+depth);
                
                focusStore.dumpStore(database,true,true,false);
            
            }
            
            log.info("removed "+nremoved+" from focusStore");
            
        }

        if( focusStore.getAccessPath(KeyOrder.SPO).isEmpty()) {

            log.info("Done - closure of focusStore produced no entailments to consider.");
            
            return;
            
        }
        
        /*
         * Recursive processing.
         */
        
        retractAll(stats, focusStore, depth + 1);

    }

}
