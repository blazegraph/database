package com.bigdata.rdf.spo;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.inf.Justification;

/**
 * Class writes on the justification index, removing all {@link Justification}s
 * for each statement specified by the caller.
 * <p>
 * Note: There is only one index for {@link Justification}s. The keys all use
 * the {s,p,o} of the entailed statement as their prefix, so given a statement
 * it is trivial to do a range scan for its justifications.
 * 
 * @todo Since this task accepts a "chunk" of statements, it should flood range
 *       delete requests for each of the statements to the justifications index
 *       using the {@link ExecutorService} , but only if the triple store
 *       provides concurrency control for writers on the same index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JustificationRemover implements Callable<Long> {

    protected static final Logger log = Logger
            .getLogger(JustificationRemover.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static protected boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static protected boolean DEBUG = log.isDebugEnabled();

    final SPORelation db;

    final ISPO[] a;

    final int numStmts;

    final AtomicLong sortTime;

    final AtomicLong writeTime;

    public JustificationRemover(SPORelation db, ISPO[] stmts, int numStmts,
            boolean clone, AtomicLong sortTime, AtomicLong writeTime) {

        if (db == null)
            throw new IllegalArgumentException();

        this.db = db;

        if (clone) {

            this.a = new ISPO[numStmts];

            System.arraycopy(stmts, 0, a, 0, numStmts);

        } else {

            this.a = stmts;

        }

        this.numStmts = numStmts;

        this.sortTime = sortTime;

        this.writeTime = writeTime;

    }

    public Long call() throws Exception {

        final long begin = System.currentTimeMillis();

        final IIndex ndx = db.getJustificationIndex();

        /*
         * Place statements in index order (SPO since all justifications begin
         * with the SPO of the entailed statement.
         */
        Arrays.sort(a, 0, numStmts, SPOKeyOrder.SPO.getComparator());

        final long beginWrite = System.currentTimeMillis();

        sortTime.addAndGet(beginWrite - begin);

        // thread-local key builder.
        final IKeyBuilder keyBuilder = KeyBuilder.newInstance(db.getKeyArity()*Bytes.SIZEOF_LONG);

        // remove statements from the index.
        for (int i = 0; i < numStmts; i++) {

            final ISPO spo = a[i];

            /*
             * This is the implementation for backwards
             * compatibility.  We should not see inline values here.
             */
            if (spo.s().isInline() || spo.p().isInline() || spo.o().isInline()) {
                throw new RuntimeException();
            }
            
            final long _s = spo.s().getTermId();
            final long _p = spo.p().getTermId();
            final long _o = spo.o().getTermId();
            
            /*
             * Form an iterator that will range scan the justifications having
             * that statement as their 'head'. The iterator uses the REMOVEALL
             * flag to have the justifications deleted on the server and does
             * not actually send back the keys or vals to the client.
             */

            final byte[] fromKey = keyBuilder.reset().append(_s).append(
                    _p).append(_o).getKey();

            final byte[] toKey = keyBuilder.reset().append(_s).append(_p)
                    .append(_o + 1).getKey();

            final ITupleIterator itr = ndx.rangeIterator(fromKey, toKey,
                    0/* capacity */, IRangeQuery.REMOVEALL, null/* filter */);

            // FullyBufferedJustificationIterator itr = new
            // FullyBufferedJustificationIterator(
            // db, spo);

            long n = 0;

            while (itr.hasNext()) {

                itr.next();

                // itr.remove();

                n++;

            }

            if (DEBUG) {

                log.debug("Removed " + n + " justifications for "
                        + spo.toString(/* db */));

            }

        }

        final long endWrite = System.currentTimeMillis();

        writeTime.addAndGet(endWrite - beginWrite);

        return endWrite - begin;

    }

}
