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
 * Created on Apr 13, 2008
 */

package com.bigdata.rdf.inf;

import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOArrayIterator;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.accesspath.IElementFilter;

/**
 * The {@link TruthMaintenance} class facilitates maintaining the RDF(S)+
 * closure on a database as {@link SPO}s are asserted or retracted. This is a
 * flyweight class that accepts a reference to the database and provides a
 * factory for a {@link TempTripleStore} on which {@link SPO}s may be written.
 * The caller writes {@link SPO}s on the {@link TempTripleStore} and then
 * invokes either {@link #assertAll(TempTripleStore)} or
 * {@link #retractAll(TempTripleStore)} as appropriate to update the closure of
 * the database, at which point the {@link TempTripleStore} is discarded (closed
 * and deleted). An instance of this class may be reused, but you need to obtain
 * a new {@link TempTripleStore} using {@link #newTempTripleStore()} each time you
 * want to buffer more {@link SPO}s after updating the closure.
 * <p>
 * Note: Neither this class nor updating closure is thread-safe. In particular,
 * clients MUST NOT write on either the database or the {@link TempTripleStore}
 * when the closure is being updated. The truth maintenance algorithm assumes
 * that the database and tempStore are unchanging outside of the actions taken
 * by the algorithm itself. Concurrent writers would violate this assumption and
 * lead to incorrect truth maintenance at the best.
 * 
 * @todo make sure that we always closeAndDelete the tempStore, even on error?
 * 
 * @todo Provide a means for creating a with-tx tempStore.
 * 
 * @todo handle update of closure for scale-out.
 * 
 * FIXME integrate with <a href="http://iris-reasoner.org/">IRIS</a> (magic
 * sets) to avoid the necessity for truth maintenance and recover the space and
 * time lost to the justification chains as well. See
 * http://sourceforge.net/projects/iris-reasoner/ as well.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TruthMaintenance {

    /**
     * Logger.
     */
    public static final Logger log = Logger.getLogger(TruthMaintenance.class);
    
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
     * The target database.
     */
    protected final AbstractTripleStore database;
    
    /**
     * The object used to compute entailments for that database.
     */
    protected final InferenceEngine inferenceEngine;

    /**
     * Return a new {@link TempTripleStore} that may be used to buffer
     * {@link SPO}s to be either asserted or retracted from the database. It is
     * recommended to use this factory method to provision the tempStore as it
     * disables the lexicon (it will not be used) but leaves other features
     * enabled (such as all access paths) which support truth maintenance.
     * <p>
     * You can wrap this with an {@link IStatementBuffer} using:
     * 
     * <pre>
     * new StatementBuffer(getTempStore(), database, bufferCapacity);
     * </pre>
     * 
     * and then write on the {@link IStatementBuffer}, which will periodically
     * perform batch writes on the tempStore.
     * <p>
     * Likewise, you can use
     * {@link IRawTripleStore#addStatements(IChunkedOrderedIterator, IElementFilter)}
     * and it will automatically perform batch writes.
     * <p>
     * Regardless, when you have written all data on the tempStore, use
     * {@link #assertAll(TempTripleStore)} or
     * {@link #retractAll(TempTripleStore)} to update the closure of the
     * database.
     * 
     * @todo max in memory size for the temporary store?
     */
    public TempTripleStore newTempTripleStore() {

        final Properties properties = database.getProperties();

        // // turn off justifications for the tempStore.
        // properties.setProperty(Options.JUSTIFY, "false");

        // turn off the lexicon since we will only use the statement indices.
        properties.setProperty(
                com.bigdata.rdf.store.AbstractTripleStore.Options.LEXICON,
                "false");

        final TempTripleStore tempStore = new TempTripleStore(properties,
                database);

        return tempStore;
        
    }

    /**
     * The database whose closure will be updated.
     */
    public AbstractTripleStore getDatabase() {

        return database;
        
    }
    
    /**
     * 
     * @param inferenceEngine
     *            The inference engine for the database.
     */
    public TruthMaintenance(InferenceEngine inferenceEngine) {
        
        if (inferenceEngine == null)
            throw new IllegalArgumentException();

        this.database = inferenceEngine.database;

        this.inferenceEngine = inferenceEngine;

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
            AbstractTripleStore database, IElementFilter<SPO> filter) {
        
        log.info("Filtering statements already known to the database");

        final long begin = System.currentTimeMillis();
        
        /*
         * Visit explicit statements in the focusStore (they should all be
         * explicit).
         */

        final IChunkedOrderedIterator<SPO> itr = focusStore.getAccessPath(
                SPOKeyOrder.SPO, ExplicitSPOFilter.INSTANCE).iterator();

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
             * 
             * Note: The retraction buffer will not compute the closure over the
             * statement identifiers since we are just reducing the amount of
             * data to which we have to apply the rules, not really removing
             * statements. If the focusStore contains statements about
             * statements that are already in the database then we MUST NOT
             * remove those metadata statements from the focus store here!
             */
            
            final SPORetractionBuffer retractionBuffer = new SPORetractionBuffer(
                    focusStore, capacity, false/* computeClosureForStatementIdentifiers */);

            while (itr.hasNext()) {

                SPO[] chunk = itr.nextChunk();

                for (SPO spo : chunk) {

                    // Lookup the statement in the database.
                    SPO tmp = database.getStatement(spo.s, spo.p, spo.o);
                    
                    if (tmp != null) {

                        // The statement is known to the database.

                        if (tmp.getType() == StatementEnum.Explicit) {
                            
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

        if (log.isInfoEnabled())
            log.info("Removed " + nremoved + " statements from the focusStore"
                    + " and upgraded " + nupgraded
                    + " statements in the database in " + elapsed + " ms.");
        
        return nremoved;
        
    }
    
    /**
     * Perform truth maintenance for statement assertion.
     * <p>
     * This method computes the closure of the temporary store against the
     * database, writing entailments into the temporary store. Once all
     * entailments have been computed, it then copies the all statements in the
     * temporary store into the database and deletes the temporary store.
     * 
     * @param tempStore
     *            A temporary store containing statements to be asserted. The
     *            tempStore will be closed and deleted as a post-condition.
     */
    public ClosureStats assertAll(TempTripleStore tempStore) {

        if (tempStore == null) {

            throw new IllegalArgumentException();

        }

        final long begin = System.currentTimeMillis();
        
        // #of given statements to retract.
        final long ngiven = tempStore.getStatementCount();
        
        if (ngiven == 0) {
            
            // nothing to assert.
            
            return new ClosureStats();
            
        }
        
        final long nbeforeClosure = tempStore.getStatementCount();

        if (log.isInfoEnabled())
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

        final ClosureStats stats = inferenceEngine.computeClosure(tempStore);

        final long nafterClosure = tempStore.getStatementCount();

        log.info("There are " + nafterClosure
                + " statements in the temporary store after closure");

        /*
         * copy statements from the temporary store to the database.
         */

        log.info("Copying statements from the temporary store to the database");

//        tempStore.dumpStore(database,true,true,false,true);

        final long ncopied = tempStore.copyStatements(database,
                null/* filter */, true /* copyJustifications */);
        
//        database.dumpStore(database,true,true,false,true);

        // note: this is the number that are _new_ to the database.
        log.info("Copied " + ncopied
                + " statements that were new to the database.");

        final long elapsed = System.currentTimeMillis() - begin;
        
        stats.elapsed += elapsed;

        log.info("Computed closure in "+elapsed+"ms");
        
        tempStore.closeAndDelete();
        
        return stats;

    }

    /**
     * Perform truth maintenance for statement retraction.
     * <p>
     * When the closure is computed, each statement to be retracted is examined
     * to determine whether or not it is still entailed by the database without
     * the support of the statements that were explicitly retracted. Statements
     * that were explicit in the database that are still provable are converted
     * to inferences. Statements which can no longer be proven (i.e., that are
     * not supported by a grounded {@link Justification} chain) are retracted
     * from the database and added into another temporary store and their
     * justifications are deleted from the database. This process repeats with
     * the new temporary store until no fixed point (no more ungrounded
     * statements are identified).
     * 
     * @param tempStore
     *            A temporary store containing explicit statements to be
     *            retracted from the database. The tempStore will be closed and
     *            deleted as a post-condition.
     * 
     * @return statistics about the closure operation.
     */
    public ClosureStats retractAll(TempTripleStore tempStore) {

        final long begin = System.currentTimeMillis();
        
        final ClosureStats stats = new ClosureStats();
        
        if (tempStore == null) {
            
            throw new IllegalArgumentException();
            
        }

        // #of given statements to retract.
        final long ngiven = tempStore.getStatementCount();
        
        if (ngiven == 0) {
            
            // nothing to assert.
            
            return stats;
            
        }
        
        if(log.isInfoEnabled()) log.info("Computing closure of the temporary store with "
                + ngiven+ " statements");

        if(database.getStatementIdentifiers()) {
            
            AbstractTripleStore.fixPointStatementIdentifiers(database, tempStore);

            if(log.isInfoEnabled()) log.info("Computing closure of the temporary store with " + ngiven
                    + " statements (after fix point of statement identifiers)");
            
        }

        // do truth maintenance.
        retractAll(stats, tempStore, 0);
        
        MDC.remove("depth");
        
        assert ! tempStore.isOpen();
       
        final long elapsed = System.currentTimeMillis() - begin;
        
        if(log.isInfoEnabled()) log.info("Retracted " + ngiven
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
     * justifications we are now seeking. Regardless of whether this is invoked
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
    private void retractAll(final ClosureStats stats, final TempTripleStore tempStore, int depth) {

        MDC.put("depth", "depth="+depth);
        
        final long tempStoreCount = tempStore.getStatementCount();

        /*
         * Note: The buffer capacity does not need to be any larger than the #of
         * triples in the temp store.
         */
        final int capacity = (int) Math.min(10000L, tempStoreCount);

        if (INFO)
            log.info("Doing truth maintenance with " + tempStoreCount
                    + " statements : depth=" + depth);
        
        /*
         * Temp store used to absorb statements for which no grounded
         * justification chain could be discovered.
         * 
         * FIXME There should be no lexicon for this tempStore, right? Should
         * there be only one access path? How is the tempStore being used?
         */

        final TempTripleStore focusStore = new TempTripleStore(database
                .getProperties(), database);
        
        // consider each statement in the tempStore.
        final IChunkedOrderedIterator<SPO> itr = tempStore.getAccessPath(SPOKeyOrder.SPO).iterator();

        final long nretracted;
        try {

            /*
             * Buffer writing on the [focusStore] adds any statements that are
             * no longer grounded.
             */
            final SPOAssertionBuffer ungroundedBuffer = new SPOAssertionBuffer(
                    focusStore, database, null/* filter */, capacity, false/* justified */);

            /*
             * Buffer used to downgrade explicit statements that are still
             * entailed by the database to inferred statements.
             * 
             * Note: If the statement is already present AND it is marked as
             * inferred then this will NOT write on the statement index.
             */
            final SPOAssertionBuffer downgradeBuffer = new SPOAssertionBuffer(
                    focusStore, database, inferenceEngine.doNotAddFilter,
                    capacity, false/* justify */);

            /*
             * Buffer used to retract statements from the database after we have
             * determined that those statements are no longer provable from the
             * database without relying on the statements that the caller
             * originally submitted for retraction.
             * 
             * Note: [computeClosureForStatementIdentifiers] is false because we
             * handle this before we begin to compute the closure of the
             * explicit statements handed to us by the caller. Truth maintenance
             * will never identify more explicit statements to be deleted so we
             * do not need to re-compute the closure over the statement
             * identifiers.
             */
            final SPORetractionBuffer retractionBuffer = new SPORetractionBuffer(
                    database, capacity, false/* computeClosureForStatementIdentifiers */);

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

                final SPO[] chunk = itr.nextChunk();

                for (SPO spo : chunk) {

                    assert spo.isFullyBound();
                    
                    if (depth == 0) {

                        /*
                         * At depth zero the statements to be retracted should
                         * be fully bound and explicit.
                         */
                        
                        assert spo.isExplicit();
                        
                    }

                    if (spo.getType() == StatementEnum.Axiom) {

                        /*
                         * Ignore.
                         */
                        if (INFO)
                            log.info("Ignoring axiom in the tempStore: "+spo);
                        
                    } else if( depth>0 && spo.getType()==StatementEnum.Explicit ) {
                        
                        /*
                         * Closure produces inferences (rather than explicit
                         * statements) so this block should never be executed.
                         */

                        throw new AssertionError(
                                "Explicit statement in the tempStore at depth="
                                        + depth + ", " + spo.toString(database));
                        
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
                        
                        final SPO tmp = new SPO(spo.s, spo.p, spo.o,
                                StatementEnum.Axiom);

                        tmp.override = true;

                        downgradeBuffer.add(tmp, null);                        

                        if (INFO)
                            log.info("Downgrading to axiom: "
                                    + spo.toString(database));

                    } else if (depth == 0
                            && Justification.isGrounded(inferenceEngine,
                                    tempStore, database, spo, testHead,
                                    testFocusStore)) {

                        /*
                         * Add a variant of the statement that is marked as
                         * "inferred" rather than as "explicit" to the buffer.
                         * When the buffer is flushed the statement will be
                         * written onto the database.
                         * 
                         * @todo consider returning the grounded justification
                         * and then writing it onto the database where. This
                         * will essentially "memoize" grounded justifications.
                         * Of course, you still have to verify that there is
                         * support for the justification (the statements in the
                         * tail of the justification still exist in the
                         * database).
                         */

                        final SPO tmp = new SPO(spo.s, spo.p, spo.o,
                                StatementEnum.Inferred);

                        tmp.override = true;

                        downgradeBuffer.add(tmp, null);

                        if (INFO)
                            log.info("Downgrading to inferred: "
                                    + spo.toString(database));
                        
                    } else if (depth > 0
                            && Justification.isGrounded(inferenceEngine,
                                    tempStore, database, spo, testHead,
                                    testFocusStore)) {

                        /*
                         * Ignore.
                         */

                        if (INFO)
                            log.info(spo.toString(database) + " is grounded");

                    } else if (!database.hasStatement(spo.s, spo.p, spo.o)) {

                        /*
                         * Ignore.
                         * 
                         * @todo this should be done as a bulk filter on the
                         * focusStore below rather than a set of point tests in
                         * this if-then-else block.
                         */

                        if (INFO) {

                            log.info("Statement not in database: "
                                    + spo.toString(database));
                            
                        }

                    } else {

                        /*
                         * The statement (and its justifications) will be
                         * removed from the database when the buffer is
                         * flushed.
                         */

                        retractionBuffer.add(spo);
                        
                        if (INFO)
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
            final int ndowngraded = downgradeBuffer.flush();
                       nretracted = retractionBuffer.flush();
            final int nungrounded = ungroundedBuffer.flush();

            if (INFO)
                log.info("#downgraded=" + ndowngraded + ", #retracted="
                        + nretracted + ", #ungrounded=" + nungrounded);

        } finally {

            itr.close();

        }

        // drop the tempStore.
        tempStore.closeAndDelete();

        if (nretracted == 0) {
            
            log.info("Done - nothing was retracted from the database");
            
            return;
            
        }
        
        if(DEBUG && database.getStatementCount()<200) {
            
            log.debug("dumping database after retraction: depth="
                            + depth
                            + "\n"
                            + database
                                    .dumpStore(database, true/* explicit */,
                                            true/* inferred */,
                                            false/* axioms */, true/* just */));
            
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
            
            final SPOArrayIterator tmp = new SPOArrayIterator(focusStore, focusStore
                    .getAccessPath(SPOKeyOrder.SPO), 0/* limit */, null/* filter */);
            
            if(DEBUG && database.getStatementCount()<200) {
                
                log.debug("focusStore before closure: depth="
                        + depth
                        + "\n"
                        + focusStore
                                .dumpStore(database, true/*explicit*/,
                                        true/*inferred*/, false/*axioms*/,
                                        true/*just*/));
            
            }

            // compute closure of the focus store.

            stats.add( inferenceEngine.computeClosure(focusStore,false/*justify*/) );

            if(DEBUG && database.getStatementCount()<200) {
                
                log.debug("focusStore after closure: depth="
                        + depth
                        + "\n"
                        + focusStore
                                .dumpStore(database, true/*explicit*/,
                                        true/*inferred*/, false/*axioms*/,
                                        true/*just*/));
            
            }
            
            // subtract out the statements we used to start the closure.
            final long nremoved = focusStore
                    .removeStatements(tmp, false/* computeClosureForStatementIdentifiers */);
            
            if(DEBUG && database.getStatementCount()<200) {
                
                log.debug("focusStore after subtracting out tmp: depth="
                        + depth
                        + "\n"
                        + focusStore
                                .dumpStore(database, true/*explicit*/,
                                        true/*inferred*/, false/*axioms*/,
                                        true/*just*/));
            
            }
            
            if (INFO)
                log.info("removed "+nremoved+" from focusStore");
            
        }

        if( focusStore.getAccessPath(SPOKeyOrder.SPO).isEmpty()) {

            log.info("Done - closure of focusStore produced no entailments to consider.");
            
            return;
            
        }
        
        /*
         * Recursive processing.
         */
        
        retractAll(stats, focusStore, depth + 1);

    }

}
