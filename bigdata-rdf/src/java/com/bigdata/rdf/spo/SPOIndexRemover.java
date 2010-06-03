package com.bigdata.rdf.spo;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.proc.LongAggregator;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBitBuffer;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBitBufferHandler;
import com.bigdata.btree.proc.BatchRemove.BatchRemoveConstructor;
import com.bigdata.rdf.inf.Justification;

/**
 * Class writes on a statement index, removing the specified statements (batch
 * API, no truth maintenance). If the database uses {@link Justification}s for
 * truth maintenance then the caller is responsible for also removing the
 * {@link Justification}s for the statements being deleted using a
 * {@link JustificationRemover}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOIndexRemover implements Callable<Long> {

    protected static final Logger log = Logger.getLogger(SPOIndexRemover.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static protected boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static protected boolean DEBUG = log.isDebugEnabled();

//    final AbstractTripleStore db;

    final private SPORelation spoRelation;
    
    final private SPOKeyOrder keyOrder;

	final private boolean primaryIndex;

    final private ISPO[] a;

    final private int numStmts;

    final private AtomicLong sortTime;

    final private AtomicLong writeTime;

    final private AtomicLong mutationCount;
    
    final private boolean reportMutations;
    
    public SPOIndexRemover(final SPORelation spoRelation, final ISPO[] stmts,
            final int numStmts, final SPOKeyOrder keyOrder,
			final boolean primaryIndex,
            final boolean clone, final AtomicLong sortTime,
            final AtomicLong writeTime,
            final AtomicLong mutationCount, 
            final boolean reportMutations) {

        if (spoRelation == null)
            throw new IllegalArgumentException();

        if (stmts == null)
            throw new IllegalArgumentException();

        if (numStmts <= 0)
            throw new IllegalArgumentException();

        if (keyOrder == null)
            throw new IllegalArgumentException();

        this.spoRelation = spoRelation;

        this.keyOrder = keyOrder;
		this.primaryIndex = primaryIndex;
        if (clone) {

            this.a = new ISPO[numStmts];

            System.arraycopy(stmts, 0, a, 0, numStmts);

        } else {

            this.a = stmts;

        }

        this.numStmts = numStmts;

        this.sortTime = sortTime;

        this.writeTime = writeTime;

        this.mutationCount = mutationCount;
        
        this.reportMutations = reportMutations;

    }

    /**
     * Remove the statements specified by the called from the statement indices.
     * 
     * @return The elapsed time.
     */
    public Long call() throws Exception {

        final long begin = System.currentTimeMillis();

//        // thread-local key builder.
//        final RdfKeyBuilder keyBuilder = db.getKeyBuilder();

        final IIndex ndx = spoRelation.getIndex(keyOrder);

        final SPOTupleSerializer tupleSer = (SPOTupleSerializer) ndx
                .getIndexMetadata().getTupleSerializer();
        
        // Place statements in index order.
        Arrays.sort(a, 0, numStmts, keyOrder.getComparator());

        final long beginWrite = System.currentTimeMillis();

        sortTime.addAndGet(beginWrite - begin);

        /*
         * Generate keys for batch operation.
         */

        final byte[][] keys = new byte[numStmts][];

        for (int i = 0; i < numStmts; i++) {

            final ISPO spo = a[i];

            if (DEBUG) {

                /*
                 * Note: the externalized terms will be NOT FOUND when removing
                 * a statement from a temp store since the term identifiers for
                 * the temp store are generally only stored in the database.
                 */

                log.debug("Removing " + spo.toString(/*db*/) + " from " + keyOrder);

            }

            keys[i] = tupleSer.serializeKey(spo);

        }

        final long writeCount;
        if (reportMutations) {
            
            /*
             * The IResultHandler obtains from the RPC an indication of each
             * statement whose state was changed by this operation. We use that
             * information to set the metadata on the corresponding ISPO in the
             * caller's array.
             */

            final ResultBitBufferHandler aggregator = new ResultBitBufferHandler(
                    numStmts);

            // batch remove.
            ndx.submit(//
                    0,// fromIndex,
                    numStmts, // toIndex,
                    keys,//
                    null, // vals
                    BatchRemoveConstructor.RETURN_BIT_MASK,//
                    aggregator // handler
                    );

            final ResultBitBuffer modified = aggregator.getResult();

            final boolean[] bits = modified.getResult();
            
            writeCount = modified.getOnCount();

            for (int i = 0; i < numStmts; i++) {

                if (bits[i]) {

                    /*
                     * Note: This only turns on the modified flag. It will not
                     * clear it if it is already set. The caller has to take
                     * responsibility for that. This way if the statement is
                     * written twice and the 2nd time the indices are not
                     * updated we still report the statement as modified since
                     * its flag has not been cleared (unless the caller
                     * explicitly cleared it in between those writes).
                     */
                    
                    a[i].setModified(bits[i]);

                }

            }

        } else {
            
            final LongAggregator aggregator = new LongAggregator();

            // batch remove.
            ndx.submit(//
                    0,// fromIndex,
                    numStmts, // toIndex,
                    keys,//
                    null, // vals
                    BatchRemoveConstructor.RETURN_MUTATION_COUNT,//
                    aggregator // handler
                    );

            writeCount = aggregator.getResult();

        }
        
		if (primaryIndex) {

            /*
             * Note: Only the task writing on the primary index takes
             * responsibility for reporting the #of statements that were removed
             * from the indices. This avoids double counting.
             */

            mutationCount.addAndGet(writeCount);

        }
        
        final long elapsed = System.currentTimeMillis() - beginWrite;

        writeTime.addAndGet(elapsed);

        return elapsed;

    }

}
