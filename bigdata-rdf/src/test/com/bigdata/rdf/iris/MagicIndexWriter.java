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

package com.bigdata.rdf.iris;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.proc.LongAggregator;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.rdf.iris.MagicIndexWriteProc.IndexWriteProcConstructor;
import com.bigdata.relation.accesspath.IElementFilter;

public class MagicIndexWriter implements Callable<Long> {

    private final IMagicTuple[] tuples;

    private final int numTuples;
    
    private final IElementFilter<IMagicTuple> filter;
    
    private final AtomicLong sortTime;
    
    private final AtomicLong insertTime;
    
    private final AtomicLong numWritten;
    
    private final Comparator<IMagicTuple> comparator;

    private final IIndex ndx;

    private final MagicKeyOrder keyOrder;

    /**
     * Writes magic tuples on an index (batch api).
     * 
     * @param statementStore
     *            The store on which the statements will be written.
     * @param a
     *            The {@link MagicTuple}[].
     * @param numTuples
     *            The #of elements in <i>a</i> to be written.
     * @param clone
     *            When true the statements are cloned.
     *            <p>
     *            Note:One of the {@link MagicIndexWriter}s gets to use the
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
     */
    public MagicIndexWriter(MagicRelation relation, IMagicTuple[] a, int numTuples,
            boolean clone, MagicKeyOrder keyOrder, IElementFilter<IMagicTuple> filter,
            AtomicLong sortTime, AtomicLong insertTime, AtomicLong numWritten) {
        
        if (relation == null)
            throw new IllegalArgumentException();
        
        if (keyOrder == null)
            throw new IllegalArgumentException();

        this.keyOrder = keyOrder;
        
        this.filter = filter;
        
        if (clone) {

            // copy the caller's data.
            
            this.tuples = new MagicTuple[numTuples];

            System.arraycopy(a, 0, this.tuples, 0, numTuples);

        } else {

            // use the callers reference.
            
            this.tuples = a;

        }

        this.numTuples = numTuples;

        this.sortTime = sortTime;
        
        this.insertTime = insertTime;

        this.numWritten = numWritten;
        
        this.comparator = keyOrder.getComparator();

        // Note: Use the index on [statementStore]!
        this.ndx = relation.getIndex(keyOrder);
        
        if (ndx == null) {
            
            throw new IllegalStateException("No index? keyOrder=" + keyOrder);
            
        }

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

        final long beginIndex = System.currentTimeMillis();

        { // sort

            final long _begin = System.currentTimeMillis();

            Arrays.sort(tuples, 0, numTuples, comparator);

            sortTime.addAndGet(System.currentTimeMillis() - _begin);

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
        
        final MagicTupleSerializer tupleSer = (MagicTupleSerializer) ndx
                .getIndexMetadata().getTupleSerializer();
        
        int numToAdd = 0;

        IMagicTuple last = null;

        final byte[][] keys = new byte[numTuples][];

        final byte[][] vals = new byte[numTuples][];

        final ByteArrayBuffer vbuf = new ByteArrayBuffer(1+8/*max length*/);
        
        for (int i = 0; i < numTuples; i++) {

            final IMagicTuple magicTuple = tuples[i];

            if (magicTuple == null)
                throw new IllegalArgumentException("null @ index=" + i);
            
            if (!magicTuple.isFullyBound())
                throw new IllegalArgumentException("Not fully bound: "
                        + magicTuple.toString());
            
            // skip statements that match the filter.
            if (filter != null && filter.accept(magicTuple))
                continue;

            // skip duplicate records.
            if (last != null && last.equals(magicTuple))
                continue;

            // generate key for the index.
            keys[numToAdd] = tupleSer.magicTuple2Key(keyOrder, magicTuple);
            
            // generate value for the index.
            vals[numToAdd] = null;

            last = magicTuple;

            numToAdd++;

        }
        
        /*
         * Run the batch insert/update logic as a procedure.
         */
        final long _begin = System.currentTimeMillis();
        
        final LongAggregator aggregator = new LongAggregator();
        
        ndx.submit(0/* fromIndex */, numToAdd/* toIndex */,
                keys, vals, IndexWriteProcConstructor.INSTANCE,
                aggregator);
        
        final long writeCount = aggregator.getResult();

        insertTime.addAndGet(System.currentTimeMillis()
                - _begin);
        
        if (keyOrder.isPrimary()) {

            /*
             * Note: Only the task writing on the SPO index takes responsibility
             * for reporting the #of statements that were written on the
             * indices.  This avoids double counting.  We use the SPO index since
             * it is always defined, even when just one access path is used.
             */

            numWritten.addAndGet(writeCount);

        }

        final long elapsed = System.currentTimeMillis() - beginIndex;

        return elapsed;

    }

}
