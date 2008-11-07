/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Nov 6, 2008
 */

package com.bigdata.btree;

import java.io.Serializable;

import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.resources.DefaultSplitHandler;

/**
 * An interface that is used to generate a bloom filter for an
 * {@link AbstractBTree} and which allows the caller to specify the expected
 * number of index entries, the desired error rate for the filter at that #of
 * index entries, and the maximum error rate before the bloom filter will be
 * disabled.
 * <p>
 * While the bloom filter is always a "perfect" fit for an {@link IndexSegment}
 * since the #of index entries is known before we build the {@link IndexSegment},
 * that is not true of a {@link BTree}. For a {@link BTree}, the performance
 * of the bloom filter will degrade as you exceed the #of index entries for
 * which it was provisioned (at a desired error rate). Therefore the factory
 * also specifies the error rate above which the bloom filter will be disabled
 * for a {@link BTree}. The factory works backwards from the actual bit length
 * of the bloom filter and the maximum allowed error rate to derive the #of
 * index entries which would achieve that error rate. The bloom filter is
 * disabled when inserts into the {@link BTree} reach that threshold #of index
 * entries.
 * <p>
 * Once a bloom filter has been disabled for a {@link BTree} it will not be
 * re-enabled for that {@link BTree}. However, in the scale-out architecture
 * the a new mutable {@link BTree} is created each time the journal overflows
 * and the data from the historical view is migrated into {@link IndexSegment}s.
 * Both the new mutable {@link BTree} and {@link IndexSegment}s will have bloom
 * filters configured according to this factory. The bloom filter on the new
 * {@link BTree} will operate until the #of index entries in that {@link BTree}
 * (not in the index or the index partition) exceeds the threashold at which the
 * bloom filter would be expected to operate with the specified maximum error
 * rate, at which point it will be disabled.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BloomFilterFactory implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -7761038651189410149L;

    /**
     * The expected #of index entries.
     */
    public final int n;

    /**
     * The desired error rate for the bloom filter at that #of index entries.
     */
    public final double p;

    /**
     * The maximum error rate for the bloom filter (it will be disabled for a
     * {@link BTree} once the bloom filter can be expected to realize this error
     * rate).
     */ 
    public final double maxP;
    
    /**
     * The maximum #of index entries before the expected performance will be
     * worse than the specified maximum error rate. A {@link BTree} will
     * automatically disable its bloom filter once this many elements have been
     * inserted. In practice, this is only a constraint on scale-up indices. For
     * scale-out indices the maximum applies only the mutable {@link BTree}
     * absorbing writes destined for a specific index partition and the
     * {@link IndexSegment}s will have "perfect fit" bloom filters.
     */
    public final int maxN;

    /**
     * The default expected #of index entries {@value #DEFAULT_N}.
     */
    public static final transient int DEFAULT_N = 1000000; // 1M
    
    /**
     * The default target error rate {@value #DEFAULT_ERROR_RATE}.
     */
    public static final transient double DEFAULT_ERROR_RATE = 0.02;

    /**
     * The default maximum error rate {@value #DEFAULT_MAX_ERROR_RATE}.
     */
    public static final transient double DEFAULT_MAX_ERROR_RATE = 0.15;

    /**
     * The recommened default factory configuration. This configuration is
     * designed to provide a bloom filter with good performance up to ~2M index
     * entries and then shutoff automatically. This works for both the scale-up
     * case (good for small indices and turns off for large indices) and for
     * scale-out (just about right sized for scale-out, depending on the split
     * handler).
     * <p>
     * <p>
     * The space requirements of a {@link BloomFilter} are approximately one
     * byte per index entry for a reasonable false positive rate (aka error
     * rate). Use {@link BloomFilterFactory#DEFAULT} for reasonable defaults.
     * With these values, the bloom filter will be limited to an index with no
     * more than ~2M entries. After than the bloom filter begins to have
     * diminishing results due to an increasing false positive rate and will be
     * automatically disabled. If you increase the expected #of index entries,
     * then you can handle larger indices with the same error rate, but this
     * swiftly gets into diminishing returns again because the latency required
     * to materialize and persist the bloom filter becomes noticeable. Therefore
     * the default configuration for the bloom filter is highly accurate up to
     * 1M index entries (0.02 error rate) and then begins to loose precision. If
     * the index grows large enough then the expected error rate of the bloom
     * filter will degrade to the point where it will be automatically disabled.
     * <p>
     * While this places an effective upper bound on the size of a
     * <em>scale-up</em> index that can make effective use of a bloom filter,
     * scale-out indices DO NOT share the same limitation. Each time a scale-out
     * index is partitioned, it is broken into a mutable {@link BTree} for
     * absorbing writes for an index partition and zero or more
     * {@link IndexSegment}s. The default configuration of the
     * {@link DefaultSplitHandler} for a scale-out index caps the #of entries in
     * an index partition at ~ 1.5M. However, many of those index entries are
     * going to migrate to the {@link IndexSegment}s, so the #of index entries
     * in the {@link BTree} is never that large. Finally, #of index entries in
     * an {@link IndexSegment} is always known when the {@link IndexSegment} is
     * built, so the {@link BloomFilter} for an {@link IndexSegment} is always a
     * perfect fit.
     * 
     * @see DefaultSplitHandler
     */
    public static final transient BloomFilterFactory DEFAULT = new BloomFilterFactory(
            DEFAULT_N, DEFAULT_ERROR_RATE, DEFAULT_MAX_ERROR_RATE); 
    
    /**
     * Configuration with the caller specified #of index entries and having a
     * target error rate of {@value #DEFAULT_ERROR_RATE} and a maximum error
     * rate of {@value #DEFAULT_MAX_ERROR_RATE}.
     * 
     * @param n
     *            The expected #of index entries for a {@link BTree} (this
     *            value is ignored for {@link IndexSegment}s).
     *            
     * @throws IllegalArgumentException
     *             if <i>n</i> is non-positive.
     */
    public BloomFilterFactory(final int n) {
     
        this(n, DEFAULT_ERROR_RATE, DEFAULT_MAX_ERROR_RATE);
        
    }
    
    /**
     * Core impl.
     * 
     * @param n
     *            The expected #of index entries (this value is ignored for
     *            {@link IndexSegment}s).
     * @param p
     *            The desired error rate for the bloom filter at that #of index
     *            entries (or at the actual #of index entries for an
     *            {@link IndexSegment}).
     * @param maxP
     *            The maximum error rate for the bloom filter for a
     *            {@link BTree} (it will be disabled for a {@link BTree} once
     *            the bloom filter can be expected to realize this error rate).
     * 
     * @throws IllegalArgumentException
     *             if <i>n</i> is non-positive.
     * @throws IllegalArgumentException
     *             unless <i>p</i> lies in (0:1].
     * @throws IllegalArgumentException
     * @throws IllegalArgumentException
     *             unless <i>maxP</i> lies in (<i>p</i>:1].
     */
    public BloomFilterFactory(final int n, final double p, final double maxP) {

        if (n <= 0)
            throw new IllegalArgumentException();
        if (p <= 0d || p > 1d)
            throw new IllegalArgumentException();
        if (maxP <= p || maxP > 1d)
            throw new IllegalArgumentException();

        this.n = n;

        this.p = p;

        this.maxP = maxP;

        // #of hash functions.
        final int k = BloomFilter.getHashFunctionCount(p);

        // bit length of the filter.
        final long m = BloomFilter.getBitLength(k, n);

        /*
         * The maximum #of index entries before we disable the filter because
         * the expected performance will be worse than the specified maximum
         * error rate.
         */
        this.maxN = BloomFilter.getEntryCountForErrorRate(k, m, maxP);

    }

    /**
     * Create and return a new (empty) bloom filter for a {@link BTree} or
     * {@link IndexSegment}.
     * <p>
     * The bloom filter can be provisioned with reference to
     * {@link src/architecture/bloomfilter.xls}. Let <code>p</code> be the
     * probability of a false positive (aka the error rate) and <code>n</code>
     * be the #of index entries. The values p=.02 and n=1M result in a space
     * requirement of 8656171 bits or approximately 1mb and uses ~ 8.6 bits per
     * element. In order to achieve the same error rate with n=10M, the size
     * requirements of the bloom filter will be approximately 10mb since the
     * filter will still use ~ 8.6 bits per element for that error rate, or
     * roughly one byte per index entry.
     * <p>
     * The maximum record length for the backing store can easily be exceeded by
     * a large bloom filter, large bloom filters will require significant time
     * to read/write during checkpoints, and large bloom filters will be written
     * each time a dirty index is involved in a commit.
     * <p>
     * While the scale-out architecture uses group commits and hence can be
     * expected to perform more commits during a bulk data load, it also uses
     * one bloom filter per {@link AbstractBTree} so the #of index entries is
     * bounded by the configured {@link ISplitHandler}. On the other hand, the
     * bloom filter performance will degrade as a scale-up index grows in size
     * since the bloom filter can not be made very large for a scale-up store
     * (the maximum record size is reduced in order to permit more records) and
     * large indices will therefore experience increasing false positive rates
     * as they grow.
     * <p>
     * Whether or not a bloom filter is useful depends on the application. The
     * bloom filter will ONLY be used for point tests such as contains(),
     * lookup(), or an {@link IAccessPath} that is fully bound and therefore can
     * benefit from testing a bloom filter.
     */
    public BloomFilter newBloomFilter() {

        return new BloomFilter(n, p, /* maxP,*/ maxN);
        
// // target error rate at the target #of index entries.
//        final double p = 0.02;
//        
//        final int n = 1000000; // 1M. (works nicely for U1, U5, ....)
////        final int n = 10000000; // 10M (appears to introduce query latency and exceeds the disk cache size).
//        //final int n = 100000000; // 100M (too large for the default max store record length!)
//
//        final BloomFilter filter;
//        
//        filter = new BloomFilter(n, p, maxP, maxN);
//        
//        // the maximum error rate that we will accept before turning off the bloom filter.
//        final double max_p = 0.15;
//
//        // the #of index entries that may be expected to realize that maximum error rate.
//        final int max_n = filter.getEntryCountForErrorRate(max_p);
//        
////        filter = new BloomFilter(metadata.getErrorRate(), nentries);
//        
//        return filter;
        
    }
    
    public String toString() {
        
        final StringBuilder sb = new StringBuilder(getClass().getSimpleName());

        sb.append("{ n=" + n);
        sb.append(", p=" + p);
        sb.append(", maxP=" + maxP);
        sb.append(", maxN=" + maxN);
        sb.append("}");

        return sb.toString();

    }

}
