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
 * Created on Apr 9, 2008
 */

package com.bigdata.rdf.spo;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.proc.LongAggregator;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBitBuffer;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBitBufferHandler;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.rdf.spo.SPOIndexWriteProc.IndexWriteProcConstructor;
import com.bigdata.relation.accesspath.IElementFilter;

/**
 * Helper class writes an {@link ISPO}[] on one of the statement indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOIndexWriter implements Callable<Long> {

//    private final AbstractTripleStore statementStore;
    
    private final ISPO[] stmts;

    private final int numStmts;
    
    private final IElementFilter<ISPO> filter;
    
    private final AtomicLong sortTime;
    
    private final AtomicLong insertTime;
    
    private final AtomicLong numWritten;
    
    private final Comparator<ISPO> comparator;

    private final IIndex ndx;

    private final SPOKeyOrder keyOrder;
    
    private final boolean reportMutation;

	private final boolean primaryIndex;

    /**
     * Writes statements on a statement index (batch api).
     * 
     * @param statementStore
     *            The store on which the statements will be written.
     * @param a
     *            The {@link ISPO}[].
     * @param numStmts
     *            The #of elements in <i>a</i> to be written.
     * @param clone
     *            When true the statements are cloned.
     *            <p>
     *            Note:One of the {@link SPOIndexWriter}s gets to use the
     *            caller's array. The others MUST be instructed to clone the
     *            caller's array so that they can impose their distinct sort
     *            orders.
     * @param keyOrder
     *            Identifies the statement index on which to write.
     * @param filter
     *            An optional filter.
     * @param sortTime
     *            Incremented as a side-effect to report the elapsed time
     *            sorting the statements.
     * @param insertTime
     *            Incremented as a side-effect to report the elapsed time
     *            writing the statements on the <i>statementStore</i>.
     * @param numWritten
     *            Incremented as a side-effect to report the #of statements
     *            actually written on the SPO index (the counter is only
     *            incremented when writing on the SPO index to avoid double
     *            counting).
     * @param reportMutations
     *            When <code>true</code>, an indication will be reported for
     *            each statement whose state in the index was changed as a 
     *            result of this operation.
     */
    public SPOIndexWriter(final SPORelation spoRelation, final ISPO[] a,
            final int numStmts, final boolean clone,
			final SPOKeyOrder keyOrder, final boolean primaryIndex,
			final IElementFilter<ISPO> filter,
            final AtomicLong sortTime, final AtomicLong insertTime,
            final AtomicLong numWritten,
            final boolean reportMutations) {

        if (spoRelation == null)
            throw new IllegalArgumentException();
        
        if (keyOrder == null)
            throw new IllegalArgumentException();

        this.keyOrder = keyOrder;
		this.primaryIndex = primaryIndex;
        this.filter = filter;
        
        if (clone) {

            // Copy the caller's data (cloning the array, not its contents).
            
            this.stmts = new ISPO[numStmts];

            System.arraycopy(a, 0, this.stmts, 0, numStmts);

        } else {

            // use the callers reference.
            
            this.stmts = a;

        }

        this.numStmts = numStmts;

        this.sortTime = sortTime;
        
        this.insertTime = insertTime;

        this.numWritten = numWritten;
        
        this.comparator = keyOrder.getComparator();

        this.reportMutation = reportMutations;
        
        // Note: Use the index on [statementStore]!
        this.ndx = spoRelation.getIndex(keyOrder);
        
        assert ndx != null;
        
    }

    /**
     * Write the statements on the appropriate statement index.
     * <p>
     * Note: This method is designed to NOT write on the index unless either the
     * statement is new or the value associated with the statement has been
     * changed. This helps to keep down the IO costs associated with index
     * writes when the data are already in the index.
     * 
     * @return The elapsed time for the operation.
     */
    public Long call() throws Exception {

        final long begin = System.currentTimeMillis();

        { // sort

            Arrays.sort(stmts, 0, numStmts, comparator);

            sortTime.addAndGet(System.currentTimeMillis() - begin);

        }

        /*
         * Generate keys for the statements to be added.
         * 
         * Note: This also filters out duplicate statements (since the data are
         * sorted duplicates will be grouped together) and, if a filter has been
         * specified, that filter is used to filter out any matching statements.
         * 
         * The outcome is that both keys[] and vals[] are dense and encode only
         * the statements to be written on the index. Only the 1st [numToAdd]
         * entries in those arrays contain valid data.
         * 
         * @todo write a unit test in which we verify: (a) the correct
         * elimination of duplicate statements; (b) the correct filtering of
         * statements; and (c) the correct application of the override flag.
         */
        
        final SPOTupleSerializer tupleSer = (SPOTupleSerializer) ndx
                .getIndexMetadata().getTupleSerializer();
        
        int numToAdd = 0;

        ISPO last = null;

        // dense array of keys.
        final byte[][] keys = new byte[numStmts][];

        // dense array of values.
        final byte[][] vals = new byte[numStmts][];
        
        // dense array of statements to write.
        final ISPO[] denseStmts = reportMutation ? new ISPO[numStmts] : null;

        final ByteArrayBuffer vbuf = new ByteArrayBuffer(1+8/*max length*/);
        
        for (int i = 0; i < numStmts; i++) {

            final ISPO spo = stmts[i];

            if (spo == null)
                throw new IllegalArgumentException("null @ index=" + i);
            
            if (!spo.isFullyBound())
                throw new IllegalArgumentException("Not fully bound: "
                        + spo.toString());
            
            // skip statements that match the filter.
            if (filter != null && filter.accept(spo))
                continue;

            // skip duplicate records.
            if (last != null && last.equals(spo)) {
                if (keyOrder.getKeyArity() == 4) {
                    // must also compare context for quads.
                    if (last.c() == spo.c())
                        continue;

                } else
                    continue;
            }

            // generate key for the index.
            keys[numToAdd] = tupleSer.serializeKey(spo);
            
            // generate value for the index.
            vals[numToAdd] = spo.serializeValue(vbuf);

            if(reportMutation)
                denseStmts[numToAdd] = spo;
            
            last = spo;

            numToAdd++;

        }
        
        /*
         * Run the batch insert/update logic as a procedure.
         */
        final long _begin = System.currentTimeMillis();
        
        final long writeCount;
        if (reportMutation) {

            /*
             * The IResultHandler obtains from the RPC an indication of each
             * statement whose state was changed by this operation. We use that
             * information to set the metadata on the corresponding ISPO in the
             * caller's array.
             */

            final ResultBitBufferHandler aggregator = new ResultBitBufferHandler(
                    numToAdd);

            ndx.submit(0/* fromIndex */, numToAdd/* toIndex */, keys, vals,
                    IndexWriteProcConstructor.REPORT_MUTATION, aggregator);

            final ResultBitBuffer modified = aggregator.getResult();

            final boolean[] bits = modified.getResult();
            
            writeCount = modified.getOnCount();

            for (int i = 0; i < numToAdd; i++) {

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
                    
                    denseStmts[i].setModified(bits[i]);

                }

            }

        } else {

            final LongAggregator aggregator = new LongAggregator();

            ndx.submit(0/* fromIndex */, numToAdd/* toIndex */, keys, vals,
                    IndexWriteProcConstructor.INSTANCE, aggregator);

            writeCount = aggregator.getResult();

        }
        
        insertTime.addAndGet(System.currentTimeMillis() - _begin);

		if (primaryIndex) {

            /*
             * Note: Only the task writing on the primary index takes
             * responsibility for reporting the #of statements that were written
             * on the indices. This avoids double counting.
             */

            numWritten.addAndGet(writeCount);

        }

        return System.currentTimeMillis() - begin;

    }

}
