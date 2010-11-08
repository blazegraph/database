/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 16, 2010
 */

package com.bigdata.bop.ap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;

import com.bigdata.bop.AbstractAccessPathOp;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.IPredicate;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.ILeafCursor;
import com.bigdata.btree.ILinearList;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.filter.Advancer;
import com.bigdata.btree.view.FusedView;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.IAccessPathExpander;
import com.bigdata.striterator.IKeyOrder;

import cutthecrap.utils.striterators.IFilter;

/**
 * Sampling operator for the {@link IAccessPath} implied by an
 * {@link IPredicate}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: AbstractSampleIndex.java 3672 2010-09-28 23:39:42Z thompsonbry
 *          $
 * @param <E>
 *            The generic type of the elements materialized from that index.
 * 
 * @todo This is a basic operator which is designed to support adaptive query
 *       optimization. However, there are a lot of possible semantics for
 *       sampling, including: uniform distribution, randomly distribution, tuple
 *       at a time versus clustered (sampling with leaves), adaptive sampling
 *       until the sample reflects some statistical property of the underlying
 *       population, etc.
 */
public class SampleIndex<E> extends AbstractAccessPathOp<E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

	/**
	 * Known annotations.
	 */
	public interface Annotations extends BOp.Annotations {

		/**
		 * The sample limit (default {@value #DEFAULT_LIMIT}).
		 */
		String LIMIT = "limit";

		int DEFAULT_LIMIT = 100;

		/**
		 * The {@link IPredicate} describing the access path to be sampled
		 * (required).
		 */
		String PREDICATE = SampleIndex.class.getName() + ".predicate";
		
	}

    public SampleIndex(SampleIndex<E> op) {

    	super(op);
    	
    }
    
	public SampleIndex(BOp[] args, Map<String, Object> annotations) {

		super(args, annotations);

    }
    
	public int limit() {

		return getProperty(Annotations.LIMIT, Annotations.DEFAULT_LIMIT);
        
    }

    @SuppressWarnings("unchecked")
    public IPredicate<E> getPredicate() {
        
        return (IPredicate<E>) getRequiredProperty(Annotations.PREDICATE);
        
    }

	/**
	 * Return a sample from the access path associated with the
	 * {@link Annotations#PREDICATE}.
	 */
	public E[] eval(final BOpContextBase context) {

		try {
			return new SampleTask(context).call();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	/**
	 * Sample an {@link IAccessPath}.
	 * 
	 * FIXME This needs to handle each of the following conditions:
	 * <p>
	 * Timestamp {read-historical, read-committed, read-write tx, unisolated}<br>
	 * Index view {standalone, partitioned,global view of partitioned}<br>
	 * 
	 * @todo The general approach uses the {@link ILinearList} interface to take
	 *       evenly distributed or randomly distributed samples from the
	 *       underlying index. This is done using an {@link IFilter} which is
	 *       evaluated local to the index. This works whether or not the access
	 *       path is using a partitioned view of the index.
	 *       <p>
	 *       When sampling an index shard the {@link ILinearList} API is not
	 *       defined for the {@link FusedView}. Since this sampling operator
	 *       exists for the purposes of estimating the cardinality of an access
	 *       path, we can dispense with the fused view and collect a number of
	 *       samples from each component of that view which is proportional to
	 *       the range count of the view divided by the range count of the
	 *       component index. This may cause tuples which have since been
	 *       deleted to become visible, but this should not cause problems when
	 *       estimating the cardinality of a join path as long as we always
	 *       report the actual tuples from the fused view in the case where the
	 *       desired sample size is LTE the estimated range count of the access
	 *       path.
	 * 
	 * @todo Better performance could be realized by accepting all tuples in a
	 *       leaf. This requires a sensitivity to the leaf boundaries which
	 *       might be obtained with an {@link ITupleCursor} extension interface
	 *       for local indices or with the {@link ILeafCursor} interface if that
	 *       can be exposed from a sufficiently low level {@link ITupleCursor}
	 *       implementation. However, when they are further constraints layered
	 *       onto the access path by the {@link IPredicate} it may be that such
	 *       clustered (leaf at once) sampling is not practical.
	 * 
	 * @todo When sampling a global view of a partitioned index, we should focus
	 *       the sample on a subset of the index partitions in order to
	 *       "cluster" the effort. This can of course introduce bias. However,
	 *       if there are a lot of index partitions then the sample will of
	 *       necessity be very small in proportion to the data volume and the
	 *       opportunity for bias will be correspondingly large.
	 * 
	 * @todo If there is an {@link IAccessPathExpander} then
	 */
	private class SampleTask implements Callable<E[]> {

		private final BOpContextBase context;

		SampleTask(final BOpContextBase context) {
			
			this.context = context;

		}

		/** Return a sample from the access path. */
		public E[] call() throws Exception {

			return sample(limit(), getPredicate()).getSample();

		}

		/**
		 * Return a sample from the access path.
		 * 
		 * @param limit
		 * @return
		 */
		public AccessPathSample<E> sample(final int limit,
				IPredicate<E> predicate) {

			final IRelation<E> relation = context.getRelation(predicate);

			// @todo assumes raw AP.
			final AccessPath<E> accessPath = (AccessPath<E>) context
					.getAccessPath(relation, predicate);

			final long rangeCount = accessPath.rangeCount(false/* exact */);

			if (limit > rangeCount) {

				/*
				 * The sample will contain everything in the access path.
				 */
				return new AccessPathSample<E>(limit, accessPath);

			}

			/*
			 * Add the CURSOR and PARALLEL flags to the predicate.
			 * 
			 * @todo turn off REVERSE if specified.
			 */
			final int flags = predicate.getProperty(
					IPredicate.Annotations.FLAGS,
					IPredicate.Annotations.DEFAULT_FLAGS)
					| IRangeQuery.CURSOR
					| IRangeQuery.PARALLEL;

			predicate = (IPredicate<E>) predicate.setProperty(
					IPredicate.Annotations.FLAGS, flags);

			/*
			 * Add advancer to collect sample.
			 */
			predicate = ((Predicate<E>) predicate)
					.addIndexLocalFilter(new SampleAdvancer<E>(//rangeCount,
							limit, accessPath.getFromKey(), accessPath
									.getToKey()));

			return new AccessPathSample<E>(limit, context.getAccessPath(
					relation, predicate));

		}

	}

	/**
	 * An advancer pattern which is designed to take evenly distributed samples
	 * from an index. The caller specifies the #of tuples to be skipped after
	 * each tuple visited. That number should be computed based on the estimated
	 * range count of the index and the desired sample size. This can fail to
	 * gather the desired number of sample if additional filters are applied
	 * which further restrict the elements selected by the predicate. However,
	 * it will still faithfully represent the expected cardinality of the
	 * sampled access path.
	 * 
	 * @author thompsonbry@users.sourceforge.net
	 * 
	 * @param <E>
	 *            The generic type of the elements visited by that access path.
	 */
	private static class SampleAdvancer<E> extends Advancer<E> {

		private static final long serialVersionUID = 1L;

		/** The desired total limit on the sample. */
		private final int limit;

		private final byte[] /*fromKey,*/ toKey;

		/*
		 * Transient data. This gets initialized when we visit the first tuple.
		 */
		
		/** The #of tuples to be skipped after every tuple visited. */
		private transient int skipCount;
		/** The #of tuples accepted so far. */
		private transient int nread = 0;
		/** The inclusive lower bound of the first tuple actually visited. */
		private transient int fromIndex;
		/** The exclusive upper bound of the last tuple which could be visited. */
		private transient int toIndex;
		
		/**
		 * 
		 * @param limit
		 *            The #of samples to visit.
		 */
		public SampleAdvancer(final int limit, final byte[] fromKey,
				final byte[] toKey) {

			this.limit = limit;
			this.toKey = toKey;
		}

		/**
		 * @todo This is taking evenly spaced samples. It is much more efficient
		 *       to take clusters of samples when you can accept the bias.
		 *       Taking a clustered sample really requires knowing where the
		 *       leaf boundaries are in the index, e.g., using
		 *       {@link ILeafCursor}.
		 * 
		 * @todo Rather than evenly spaced samples, we should be taking a random
		 *       sample. This could be achieved using a random initial offset
		 *       and random increment as long as the initial offset was in the
		 *       range of a single increment and we compute the increment such
		 *       that N+1 intervals exist.
		 */
		@Override
		protected void advance(final ITuple<E> tuple) {

			final AbstractBTree ndx = (AbstractBTree) src.getIndex();

			final int currentIndex = ndx.indexOf(tuple.getKey());

			if (nread == 0) {

				// inclusive lower bound.
				fromIndex = currentIndex;

				// exclusive upper bound.
				toIndex = toKey == null ? ndx.getEntryCount() : ndx
						.indexOf(toKey);

				final int rangeCount = (toIndex - fromIndex);

				skipCount = Math.max(1, rangeCount / limit);
				
				// minus one since src.next() already consumed one tuple.
				skipCount -= 1;

//				System.err.println("limit=" + limit + ", rangeCount="
//						+ rangeCount + ", skipCount=" + skipCount);
				
			}

			nread++;
			
			if (skipCount > 0) {

				/*
				 * If the skip count is positive, then skip over N tuples.
				 */

				final int nextIndex = Math.min(ndx.getEntryCount() - 1,
						currentIndex + skipCount);

				src.seek(ndx.keyAt(nextIndex));

			}

		}

	} // class SampleAdvancer

	/**
	 * A sample from an access path.
	 * 
	 * @param <E>
	 *            The generic type of the elements visited by that access
	 *            path.
	 * 
	 * @author thompsonbry@users.sourceforge.net
	 */
	public static class AccessPathSample<E> implements Serializable {

		private static final long serialVersionUID = 1L;

		private final IPredicate<E> pred;
		private final IKeyOrder<E> keyOrder;
		private final int limit;
		private final E[] sample;

		/**
		 * Constructor populates the sample using the caller's
		 * {@link IAccessPath#iterator()}. The caller is responsible for setting
		 * up the {@link IAccessPath} such that it provides an efficient sample
		 * of the access path with the appropriate constraints.
		 * 
		 * @param limit
		 * @param accessPath
		 */
		private AccessPathSample(final int limit,
				final IAccessPath<E> accessPath) {

			if (limit <= 0)
				throw new IllegalArgumentException();
			
			if (accessPath == null)
				throw new IllegalArgumentException();

			this.pred = accessPath.getPredicate();

			this.keyOrder = accessPath.getKeyOrder();

			this.limit = limit;

			// drain the access path iterator.
			final ArrayList<E> tmp = new ArrayList<E>(limit);

			int nsamples = 0;

			final Iterator<E> src = accessPath.iterator(0L/* offset */, limit,
					limit/* capacity */);
			
			while (src.hasNext() && nsamples < limit) {

				tmp.add(src.next());

				nsamples++;

			}

			// convert to an array of the appropriate type.
			sample = tmp.toArray((E[]) java.lang.reflect.Array.newInstance(
					tmp.get(0).getClass(), tmp.size()));

		}

		public IPredicate<E> getPredicate() {
			return pred;
		}

		public boolean isEmpty() {
			return sample != null;
		}

		public int sampleSize() {
			return sample == null ? 0 : sample.length;
		}

		public int limit() {
			return limit;
		}

		/**
		 * The sample.
		 * 
		 * @return The sample -or- <code>null</code> if the sample was
		 *         empty.
		 */
		public E[] getSample() {
			return sample;
		}

	} // AccessPathSample

}
