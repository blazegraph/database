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

import it.unimi.dsi.bits.BitVector;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
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
import com.bigdata.rawstore.Bytes;
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
 *       population, etc. Support for different kinds of sampling could be added
 *       using appropriate annotations.
 */
public class SampleIndex<E> extends AbstractAccessPathOp<E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

	/**
	 * Typesafe enumeration of different kinds of index sampling strategies.
	 * 
	 * @todo It is much more efficient to take clusters of samples when you can
	 *       accept the bias. Taking a clustered sample really requires knowing
	 *       where the leaf boundaries are in the index, e.g., using
	 *       {@link ILeafCursor}. Taking all tuples from a few leaves in each
	 *       sample might produce a faster estimation of the correlation when
	 *       sampling join paths.
	 */
	public static enum SampleType {
        /**
         * Samples are taken at even space offsets. This produces a sample
         * without any random effects. Re-sampling an index having the same data
         * with the same key-range and the limit will always return the same
         * results. This is useful to make unit test repeatable.
         */
		EVEN,
		/**
		 * Sample offsets are computed randomly.
		 */
		RANDOM;
	}

	/**
	 * Known annotations.
	 */
	public interface Annotations extends BOp.Annotations {

		/**
		 * The sample limit (default {@value #DEFAULT_LIMIT}).
		 */
		String LIMIT = SampleIndex.class.getName() + ".limit";

		int DEFAULT_LIMIT = 100;

		/**
		 * The random number generator seed -or- ZERO (0L) for a random seed
		 * (default {@value #DEFAULT_SEED}). A non-zero value may be used to
		 * create a repeatable sample.
		 */
		String SEED = SampleIndex.class.getName() + ".seed";
		
		long DEFAULT_SEED = 0L;
		
		/**
		 * The {@link IPredicate} describing the access path to be sampled
		 * (required).
		 */
		String PREDICATE = SampleIndex.class.getName() + ".predicate";

		/**
		 * The type of sample to take (default {@value #DEFAULT_SAMPLE_TYPE)}.
		 */
		String SAMPLE_TYPE = SampleIndex.class.getName() + ".sampleType";
		
		String DEFAULT_SAMPLE_TYPE = SampleType.RANDOM.name();

	}

    public SampleIndex(SampleIndex<E> op) {

    	super(op);
    	
    }
    
	public SampleIndex(BOp[] args, Map<String, Object> annotations) {

		super(args, annotations);

    }

    /**
     * @see Annotations#LIMIT
     */
	public int limit() {

		return getProperty(Annotations.LIMIT, Annotations.DEFAULT_LIMIT);
        
    }
	
    /**
     * @see Annotations#SEED
     */
	public long seed() {

		return getProperty(Annotations.SEED, Annotations.DEFAULT_SEED);
        
    }

	/**
	 * @see Annotations#SAMPLE_TYPE
	 */
	public SampleType getSampleType() {

		return SampleType.valueOf(getProperty(Annotations.SAMPLE_TYPE,
				Annotations.DEFAULT_SAMPLE_TYPE));
		
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

			return sample(limit(), getSampleType(), getPredicate()).getSample();

		}

		/**
		 * Return a sample from the access path.
		 * 
		 * @param limit
		 * @return
		 */
		public AccessPathSample<E> sample(final int limit,
				final SampleType sampleType, IPredicate<E> predicate) {

			final IRelation<E> relation = context.getRelation(predicate);

			// @todo assumes raw AP.
			final AccessPath<E> accessPath = (AccessPath<E>) context
					.getAccessPath(relation, predicate);

			final long rangeCount = accessPath.rangeCount(false/* exact */);

			if (limit >= rangeCount) {

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

			final Advancer<E> advancer;
			switch (sampleType) {
			case EVEN:
				advancer = new EvenSampleAdvancer<E>(// rangeCount,
						limit, accessPath.getFromKey(), accessPath.getToKey());
				break;
			case RANDOM:
				advancer = new RandomSampleAdvancer<E>(// rangeCount,
						seed(), limit, accessPath.getFromKey(), accessPath
								.getToKey());
				break;
			default:
				throw new UnsupportedOperationException("SampleType="
						+ sampleType);
			}
			
			predicate = ((Predicate<E>) predicate)
					.addIndexLocalFilter(advancer);

			return new AccessPathSample<E>(limit, context.getAccessPath(
					relation, predicate));

		}

	}

	/**
	 * An advancer pattern which is designed to take evenly distributed samples
	 * from an index. The caller specifies the #of tuples to be sampled. This
	 * class estimates the range count of the access path and then computes the
	 * #of samples to be skipped after each tuple visited.
	 * <p>
	 * Note: This can fail to gather the desired number of sample if additional
	 * filters are applied which further restrict the elements selected by the
	 * predicate. However, it will still faithfully represent the expected
	 * cardinality of the sampled access path (tuples tested).
	 * 
	 * @author thompsonbry@users.sourceforge.net
	 * 
	 * @param <E>
	 *            The generic type of the elements visited by that access path.
	 */
	private static class EvenSampleAdvancer<E> extends Advancer<E> {

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
		public EvenSampleAdvancer(final int limit, final byte[] fromKey,
				final byte[] toKey) {

			this.limit = limit;
			this.toKey = toKey;
		}

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

				if (toIndex < 0) {
					// convert insert position to index.
					toIndex = -toIndex + 1;
				}

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

	} // class EvenSampleAdvancer

	/**
	 * An advancer pattern which is designed to take randomly distributed
	 * samples from an index. The caller specifies the #of tuples to be sampled.
	 * This class estimates the range count of the access path and then computes
	 * a set of random offsets into the access path from which it will collect
	 * the desired #of samples.
	 * <p>
	 * Note: This can fail to gather the desired number of sample if additional
	 * filters are applied which further restrict the elements selected by the
	 * predicate. However, it will still faithfully represent the expected
	 * cardinality of the sampled access path (tuples tested).
	 * 
	 * @author thompsonbry@users.sourceforge.net
	 * 
	 * @param <E>
	 *            The generic type of the elements visited by that access path.
	 */
	private static class RandomSampleAdvancer<E> extends Advancer<E> {

		private static final long serialVersionUID = 1L;

		/** The random number generator seed. */
		private final long seed;
		
		/** The desired total limit on the sample. */
		private final int limit;

		private final byte[] fromKey, toKey;

		/*
		 * Transient data. This gets initialized when we visit the first tuple.
		 */
		
		/** The offset of each tuple to be sampled. */
		private transient int[] offsets;
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
		public RandomSampleAdvancer(final long seed, final int limit,
				final byte[] fromKey, final byte[] toKey) {

			this.seed = seed;
			this.limit = limit;
			this.fromKey = fromKey;
			this.toKey = toKey;
		}

		@Override
		protected boolean init() {

			final AbstractBTree ndx = (AbstractBTree) src.getIndex();

			// inclusive lower bound.
			fromIndex = fromKey == null ? 0 : ndx.indexOf(fromKey);

			if (fromIndex < 0) {
				// convert insert position to index.
				fromIndex = -fromIndex + 1;
			}

			// exclusive upper bound.
			toIndex = toKey == null ? ndx.getEntryCount() : ndx.indexOf(toKey);

			if (toIndex < 0) {
				// convert insert position to index.
				toIndex = -toIndex + 1;
			}

			// get offsets to be sampled.
			offsets = new SmartOffsetSampler().getOffsets(seed, limit,
					fromIndex, toIndex);

			// Skip to the first tuple.
			src.seek(ndx.keyAt(offsets[0]));

			return true;
			
		}

		@Override
		protected void advance(final ITuple<E> tuple) {

			final AbstractBTree ndx = (AbstractBTree) src.getIndex();

			if (nread < offsets.length - 1) {

				/*
				 * Skip to the next tuple.
				 */

				final int nextIndex = offsets[nread];

//				System.err.println("limit=" + limit + ", rangeCount="
//						+ (toIndex - fromIndex) + ", fromIndex=" + fromIndex
//						+ ", toIndex=" + toIndex + ", currentIndex="
//						+ currentIndex + ", nextIndex=" + nextIndex);
				
				src.seek(ndx.keyAt(nextIndex));

			}

			nread++;
			
		}

	} // class RandomSampleAdvancer

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

	/**
	 * Interface for obtaining an array of tuple offsets to be sampled.
	 * 
	 * @author thompsonbry
	 */
	public interface IOffsetSampler {

		/**
		 * Return an array of tuple indices which may be used to sample a key
		 * range of some index.
		 * <p>
		 * Note: The caller must stop when it runs out of offsets, not when the
		 * limit is satisfied, as there will be fewer offsets returned when the
		 * half open range is smaller than the limit.
		 * 
		 * @param seed
		 *            The seed for the random number generator -or- ZERO (0L)
		 *            for a random seed. A non-zero value may be used to create
		 *            a repeatable sample.
		 * @param limit
		 *            The maximum #of tuples to sample.
		 * @param fromIndex
		 *            The inclusive lower bound.
		 * @param toIndex
		 *            The exclusive upper bound0
		 * 
		 * @return An array of at most <i>limit</i> offsets into the index. The
		 *         offsets will lie in the half open range (fromIndex,toIndex].
		 *         The elements of the array will be in ascending order. No
		 *         offsets will be repeated.
		 * 
		 * @throws IllegalArgumentException
		 *             if <i>limit</i> is non-positive.
		 * @throws IllegalArgumentException
		 *             if <i>fromIndex</i> is negative.
		 * @throws IllegalArgumentException
		 *             if <i>toIndex</i> is negative.
		 * @throws IllegalArgumentException
		 *             unless <i>toIndex</i> is GT <i>fromIndex</i>.
		 */
		int[] getOffsets(final long seed, int limit, final int fromIndex,
				final int toIndex);
	}

	/**
	 * A smart implementation which uses whichever implementation is most
	 * efficient for the limit and key range to be sampled.
	 * 
	 * @author thompsonbry
	 */
	public static class SmartOffsetSampler implements IOffsetSampler {

		/**
		 * {@inheritDoc}
		 */
		public int[] getOffsets(final long seed, int limit,
				final int fromIndex, final int toIndex) {

			if (limit < 1)
				throw new IllegalArgumentException();
			if (fromIndex < 0)
				throw new IllegalArgumentException();
			if (toIndex < 0)
				throw new IllegalArgumentException();
			if (toIndex <= fromIndex)
				throw new IllegalArgumentException();
			
			final int rangeCount = (toIndex - fromIndex);

			if (limit > rangeCount)
				limit = rangeCount;

			if (limit == rangeCount) {

				// Visit everything.
				return new EntireRangeOffsetSampler().getOffsets(seed, limit,
						fromIndex, toIndex);
				
			}

			/*
			 * Random offsets visiting a subset of the key range using a
			 * selection without replacement pattern (the same tuple is never
			 * visited twice).
			 * 
			 * FIXME When the limit approaches the range count and the range
			 * count is large (too large for a bit vector or acceptance set
			 * approach), then we are better off creating a hash set of offsets
			 * NOT to be visited and then just choosing (rangeCount-limit)
			 * offsets to reject. This will be less expensive than computing the
			 * acceptance set directly. However, to really benefit from the
			 * smaller memory profile, we would also need to wrap that with an
			 * iterator pattern so the smaller memory representation could be of
			 * use when the offset[] is applied (e.g., modify the IOffsetSampler
			 * interface to be an iterator with various ctor parameters rather
			 * than returning an array as we do today).
			 */
			
			// FIXME BitVectorOffsetSampler is broken.
			if (false && rangeCount < Bytes.kilobyte32 * 8) {

				// NB: 32k range count uses a 4k bit vector.
				return new BitVectorOffsetSampler().getOffsets(seed, limit,
						fromIndex, toIndex);
			
			}

			/*
			 * When limit is small (or significantly smaller than the
			 * rangeCount), then we are much better off creating a hash set of
			 * the offsets which have been accepted.
			 * 
			 * Good unless [limit] is very large.
			 */
			return new AcceptanceSetOffsetSampler().getOffsets(seed, limit,
					fromIndex, toIndex);
			
		}

	}

	/**
	 * Returns all offsets in the half-open range, but may only be used when
	 * the limit GTE the range count.
	 */
	static public class EntireRangeOffsetSampler implements IOffsetSampler {

		/**
		 * {@inheritDoc}
		 * 
		 * @throws UnsupportedOperationException
		 *             if <i>limit!=rangeCount</i> (after adjusting for limits
		 *             greater than the rangeCount).
		 */
		public int[] getOffsets(final long seed, int limit,
				final int fromIndex, final int toIndex) {

			if (limit < 1)
				throw new IllegalArgumentException();
			if (fromIndex < 0)
				throw new IllegalArgumentException();
			if (toIndex < 0)
				throw new IllegalArgumentException();
			if (toIndex <= fromIndex)
				throw new IllegalArgumentException();

			final int rangeCount = (toIndex - fromIndex);

			if (limit > rangeCount)
				limit = rangeCount;

			if (limit != rangeCount)
				throw new UnsupportedOperationException();

			// offsets of tuples to visit.
			final int[] offsets = new int[limit];

			for (int i = 0; i < limit; i++) {

				offsets[i] = fromIndex + i;

			}

			return offsets;

		}
	}

	/**
	 * Return a randomly selected ordered array of offsets in the given
	 * half-open range.
	 * <p>
	 * This approach is based on a bit vector. If the bit is already marked,
	 * then the offset has been used and we scan until we find the next free
	 * offset. This requires [rangeCount] bits, so it works well when the
	 * rangeCount of the key range is small. For example, a range count of 32k
	 * requires a 4kb bit vector, which is quite manageable.
	 * 
	 * FIXME There is something broken in this class, probably an assumption I
	 * have about how {@link LongArrayBitVector} works. If you enable it in the
	 * stress test, it will fail.
	 */
	static public class BitVectorOffsetSampler implements IOffsetSampler {

		public int[] getOffsets(final long seed, int limit,
				final int fromIndex, final int toIndex) {

			if (limit < 1)
				throw new IllegalArgumentException();
			if (fromIndex < 0)
				throw new IllegalArgumentException();
			if (toIndex < 0)
				throw new IllegalArgumentException();
			if (toIndex <= fromIndex)
				throw new IllegalArgumentException();

			final int rangeCount = (toIndex - fromIndex);

			if (limit > rangeCount)
				limit = rangeCount;

			// offsets of tuples to visit.
			final int[] offsets = new int[limit];

			// create a cleared bit vector of the stated capacity.
			final BitVector v = LongArrayBitVector.ofLength(//
					rangeCount// capacity (in bits)
					);

			// Random number generator using caller's seed (if given).
			final Random rnd = seed == 0L ? new Random() : new Random(seed);

			// Choose random tuple indices for the remaining tuples.
			for (int i = 0; i < limit; i++) {

				/*
				 * Look for an unused bit starting at this index. If necessary,
				 * this will wrap around to zero.
				 */

				// k in (0:rangeCount-1).
				int k = rnd.nextInt(rangeCount);

				if (v.getBoolean((long) k)) {
					// This bit is already taken.
					final long nextZero = v.nextZero((long) k);
					if (nextZero != -1L) {
						k = (int) nextZero;
					} else {
						final long priorZero = v.previousZero((long) k);
						if (priorZero != -1L) {
							k = (int) priorZero;
						} else {
							// No empty bit found?
							throw new AssertionError();
						}
					}
				}
				
				assert !v.getBoolean(k);

				// Set the bit.
				v.add(k, true);

				assert v.getBoolean(k);

				offsets[i] = fromIndex + k;

				assert offsets[i] < toIndex;

			}

			// put them into sorted order for more efficient traversal.
			Arrays.sort(offsets);

			// System.err.println(Arrays.toString(offsets));

			return offsets;
		
		}

	}

	/**
	 * An implementation based on an acceptance set of offsets which have been
	 * accepted. This implementation is a good choice when the limit moderate
	 * (~100k) and the rangeCount is significantly greater than the limit. The
	 * memory demand is the O(limit).
	 * 
	 * @author thompsonbry
	 */
	static public class AcceptanceSetOffsetSampler implements IOffsetSampler {

		public int[] getOffsets(final long seed, int limit,
				final int fromIndex, final int toIndex) {

			if (limit < 1)
				throw new IllegalArgumentException();
			if (fromIndex < 0)
				throw new IllegalArgumentException();
			if (toIndex < 0)
				throw new IllegalArgumentException();
			if (toIndex <= fromIndex)
				throw new IllegalArgumentException();

			final int rangeCount = (toIndex - fromIndex);

			if (limit > rangeCount)
				limit = rangeCount;

			// offsets of tuples to visit.
			final int[] offsets = new int[limit];

			// hash set of accepted offsets.
			final IntOpenHashSet v = new IntOpenHashSet(
					rangeCount// capacity
					);

			// Random number generator using caller's seed (if given).
			final Random rnd = seed == 0L ? new Random() : new Random(seed);

			// Choose random tuple indices for the remaining tuples.
			for (int i = 0; i < limit; i++) {

				/*
				 * Look for an unused bit starting at this index. If necessary,
				 * this will wrap around to zero.
				 */

				// k in (0:rangeCount-1).
				int k = rnd.nextInt(rangeCount);

				int round = 0;
				while (v.contains(k)) {

					k++;

					if (k == rangeCount) {
						// wrap around.
						if (++round > 1) {
							// no empty bit found?
							throw new AssertionError();
						}
						// reset starting index.
						k = 0;
					}

				}

				assert !v.contains(k);

				// Set the bit.
				v.add(k);

				offsets[i] = fromIndex + k;

				assert offsets[i] < toIndex;

			}

			// put them into sorted order for more efficient traversal.
			Arrays.sort(offsets);

			// System.err.println(Arrays.toString(offsets));

			return offsets;

		}

	}

}
