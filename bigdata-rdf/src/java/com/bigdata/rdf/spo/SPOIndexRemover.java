package com.bigdata.rdf.spo;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
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

    final SPORelation spoRelation;
    
    final SPOKeyOrder keyOrder;

    final ISPO[] a;

    final int numStmts;

    final AtomicLong sortTime;

    final AtomicLong writeTime;

    public SPOIndexRemover(SPORelation spoRelation, ISPO[] stmts, int numStmts,
            SPOKeyOrder keyOrder, boolean clone, AtomicLong sortTime,
            AtomicLong writeTime) {

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

        if (clone) {

            this.a = new SPO[numStmts];

            System.arraycopy(stmts, 0, a, 0, numStmts);

        } else {

            this.a = stmts;

        }

        this.numStmts = numStmts;

        this.sortTime = sortTime;

        this.writeTime = writeTime;

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

            final byte[] key = tupleSer.serializeKey(spo);

            // if (ndx.remove(key) == null) {
            //
            // throw new AssertionError("Missing statement: keyOrder="
            // + keyOrder + ", spo=" + spo + ", key="
            // + Arrays.toString(key));
            //
            // }

            keys[i] = key;

        }

        // batch remove.
        ndx.submit(//
                0,// fromIndex,
                numStmts, // toIndex,
                keys,//
                null, // vals
                BatchRemoveConstructor.RETURN_NO_VALUES,//
                null // handler
                );

        final long elapsed = System.currentTimeMillis() - beginWrite;

        writeTime.addAndGet(elapsed);

        return elapsed;

    }

}
