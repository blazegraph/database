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
 * Created on Jul 7, 2008
 */

package com.bigdata.rdf.spo;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DistinctTermScanner {

    private final ExecutorService service;
    private final IIndex ndx;

    /**
     * 
     * @param service
     * @param ndx One of the statement indices for the {@link SPORelation}.
     */
    public DistinctTermScanner(ExecutorService service, IIndex ndx) {
        
        if (service == null)
            throw new IllegalArgumentException();

        if (ndx == null)
            throw new IllegalArgumentException();
        
        this.service = service;

        this.ndx = ndx;
        
    }

    /**
     * The implementation uses a key scan to find the first term identifer for
     * the given index. It then forms a fromKey that starts at the next possible
     * term identifier and does another scan, thereby obtaining the 2nd distinct
     * term identifier for that position on that index. This process is repeated
     * iteratively until the key scan no longer identifies a match. This
     * approach skips quickly over regions of the index which have many
     * statements for the same term and makes N+1 queries to identify N distinct
     * terms. Note that there is no way to pre-compute the #of distinct terms
     * that will be identified short of running the queries.
     * 
     * FIXME write a scale-out version.
     * <p>
     * This can be implemented using ICursor by seeking to the next possible key
     * after each SPO visited. This is relatively efficient and can scale-out
     * once the ICursor support is driven through the system. Another approach
     * is to write a parallelizable task that returns an Iterator and to have a
     * special aggregator. The aggregator would have to buffer the first result
     * from each index partition in order to ensure that the term identifiers
     * were not duplicated when the subject position cross (one or more) index
     * partition boundaries.
     * <p>
     * I am not sure how best to get this specialized scan folded into the rule
     * execution. E.g., as an ITupleCursor layered over the access path's
     * iterator, in which case the SPO access path will automatically do the
     * right thing but the rule should specify that filter as an
     * IPredicateConstraint. Or as a custom method on the SPOAccessPath.
     * 
     * @todo If the indices are range partitioned and the iterator guarentees
     *       "distinct" and even locally ordered, but not globally ordered, then
     *       those steps can be parallelized. The only possibility for conflict
     *       is when the last distinct term identifier is read from one index
     *       before the right sibling index partition has reported its first
     *       distinct term identifier. We could withhold the first result from
     *       each partition until the partition that proceeds it in the metadata
     *       index has completed, which would give nearly full parallelism.
     *       <p>
     *       If the indices are range partitioned and distinct + fully ordered
     *       is required, then the operation can not be parallelized, or if it
     *       is parallelized then a merge sort must be done before returning the
     *       first result.
     *       <p>
     *       Likewise, if the indices are hash partitioned, then we can do
     *       parallel index scans and a merge sort but the caller will have to
     *       wait for the merge sort to complete before obtaining the 1st
     *       result.
     * 
     * @todo unit tests (refactor from RDF DB).
     */
    public IChunkedOrderedIterator<Long> iterator() {

        final int capacity = 10000;

        /*
         * Note: IKeyOrder<SPO> is not compatible with Long so we can not
         * declare the key order, even though it corresponds to the index that
         * we choose for the scan.
         */
        final IBlockingBuffer<Long> buffer = new BlockingBuffer<Long>(capacity,
                null/* keyOrder */, null/* filter */);

        final Future future = service.submit(new DistinctTermScanTask(ndx,
                capacity, buffer));

        buffer.setFuture(future);
        
        return buffer.iterator();

    }

    /**
     * Implements the scan.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class DistinctTermScanTask implements Callable<Long> {
        
        protected static final transient Logger log = Logger
                .getLogger(DistinctTermScanTask.class);

        private final IIndex ndx;
        
        private final int capacity;

        private final IBlockingBuffer<Long> buffer;

        public DistinctTermScanTask(IIndex ndx, int capacity,
                IBlockingBuffer<Long> buffer) {

            if (ndx == null)
                throw new IllegalArgumentException();

            if (buffer == null)
                throw new IllegalArgumentException();

            this.ndx = ndx;
            
            this.capacity = capacity;

            this.buffer = buffer;

        }

        public Long call() {

            final long NULL = IRawTripleStore.NULL;
            
            byte[] fromKey = null;

            final byte[] toKey = null;

            final SPOTupleSerializer tupleSer = (SPOTupleSerializer) ndx
                    .getIndexMetadata().getTupleSerializer();

            ITupleIterator itr = ndx.rangeIterator(fromKey, toKey, capacity,
                    IRangeQuery.KEYS, null/* filter */);

            long nterms = 0L;

            while (itr.hasNext()) {

                final ITuple tuple = itr.next();

                final long id = KeyBuilder.decodeLong(tuple.getKeyBuffer()
                        .array(), 0);

                // add to the buffer.
                buffer.add(id);

                // log.debug(ids.size() + " : " + id + " : "+ toString(id));

                // restart scan at the next possible term id.
                final long nextId = id + 1;

                fromKey = tupleSer.statement2Key(nextId, NULL, NULL);

                // new iterator.
                itr = ndx.rangeIterator(fromKey, toKey, capacity,
                        IRangeQuery.KEYS, null/* filter */);

                nterms++;

            } // while

            if (log.isDebugEnabled()) {

                log.debug("Distinct key scan: ndx="
                        + ndx.getIndexMetadata().getName() + ", #terms="
                        + nterms);

            }

            return nterms;

        } // call()

    }

}
