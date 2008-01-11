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
package com.bigdata.btree;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.isolation.IIsolatableIndex;
import com.bigdata.service.Split;

/**
 * <p>
 * Interface for mutable B+-Tree mapping arbitrary non-null keys to arbitrary
 * values.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IIndex extends ISimpleBTree, IBatchBTree {
    
    /**
     * The unique identifier for the index whose data is stored in this B+Tree
     * data structure. When using a scale-out index the same <i>indexUUID</i>
     * MUST be assigned to each mutable and immutable B+Tree having data for any
     * partition of that scale-out index. This makes it possible to work
     * backwards from the B+Tree data structures and identify the index to which
     * they belong.
     */
    public UUID getIndexUUID();

    /**
     * Interesting statistics about the index.
     */
    public String getStatistics();
    
    /**
     * Return <code>true</code> iff the index supports isolation.
     * <p>
     * Note: This is declared as a method since the {@link IIsolatableIndex}
     * interface is not always detectable. For example, a remote client will use
     * a view of the index. That view does not declare {@link IIsolatableIndex}
     * but the client can use this method instead to decide whether or not the
     * index supports transactional isolation.
     * 
     * @see IIsolatableIndex
     */
    public boolean isIsolatable();
    
    /**
     * Runs an procedure against the index. This may be used to send logic to a
     * remote index. When the index is remote both the procedure and the return
     * value MUST be {@link Serializable}. Scale-out indices add support for
     * auto-split of the procedure such that it runs locally against each
     * relevant index partition.
     * 
     * @param n
     *            The #of tuples (positive integer).
     * @param keys
     *            The keys (required).
     * @param vals
     *            The values (optional depending on the procedure).
     * @param ctor
     *            An object that can create instances of the procedure.
     * @param aggregator
     *            An object that can aggregate the results of the procedure when
     *            it is applied to multiple index partitions.
     * 
     * @return The result of running that procedure. The type of the result
     *         depends on the aggregator.
     */
    public void submit(int n, byte[][] keys, byte[][] vals,
            IIndexProcedureConstructor ctor, IResultAggregator aggregator);

    /**
     * Interface for aggregating results when a procedure is broken down and
     * executed against more than one index partition.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <R>
     *            The type of the result from applying the procedure to a single
     *            index partition.
     * @param <A>
     *            The type of the aggregated result.
     * 
     * FIXME rename this as IResultHandler and drop {@link #getResult()} from
     * the signature. Where you want to process a set of items in the return
     * then the handler should be some kind of data driven iteration interface.
     */
    public static interface IResultAggregator<R extends Object,A extends Object> {
        
        /**
         * Method is invoked for each result and is responsible for combining
         * the results in whatever manner is meaningful for the procedure.
         * Implementations of this method MUST be <strong>thread-safe</strong>
         * since the procedure MAY be applied in parallel when it spans more
         * than one index partition.
         * 
         * @param result
         *            The result from applying the procedure to a single index
         *            partition.
         * @param split
         *            The {@link Split} that generated that result.
         */
        public void aggregate(R result, Split split);

        /**
         * Return the aggregated results as an implementation dependent object.
         */
        public A getResult();
    }
    
    /**
     * A factory for {@link IIndexProcedure}s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IIndexProcedureConstructor {
     
        /**
         * 
         * @param n
         *            The #of tuples on which the procedure will operate.
         * @param offset
         *            The offset of the 1st tuple into <i>keys[]</i> and
         *            <i>vals[]</i>.
         * @param keys
         *            The keys.
         * @param vals
         *            The values.
         * 
         * @return An instance of the procedure.
         * 
         * @todo we will need a different method signature to support
         *       hash-partitioned (vs range partitioned) indices.
         */
        public IIndexProcedure newInstance(int n, int offset, byte[][] keys, byte[][] vals);
        
    }

    /**
     * Aggregates result into a list of results.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <R>
     * @param <A>
     */
    public static class ListResultAggregator<R, A extends List<R>> implements
            IResultAggregator<R, A> {
    
        public ListResultAggregator() {
            
        }

        private final A results = (A) new LinkedList<R>();

        public void aggregate(R result, Split split) {

            synchronized(results) {
            
                results.add(result);
                
            }

        }

        public A getResult() {

            return results;

        }

    }

    /**
     * Aggregates the value of an integer counter.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class IntegerCounterAggregator implements
            IResultAggregator<Integer, Integer> {

        private final AtomicInteger counter = new AtomicInteger(0);

        public IntegerCounterAggregator() {
            
        }
        
        public void aggregate(Integer result, Split split) {

            counter.addAndGet(result.intValue());

        }

        public Integer getResult() {

            return counter.get();

        }

    }

}
