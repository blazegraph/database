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
 * Created on Feb 11, 2008
 */

package com.bigdata.btree;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.filter.Reverserator;
import com.bigdata.btree.filter.TupleRemover;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IKeyRangeIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.ISimpleIndexProcedure;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.isolation.IsolatedFusedView;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.relation.accesspath.AbstractAccessPath;
import com.bigdata.resources.DefaultSplitHandler;
import com.bigdata.service.MetadataService;
import com.bigdata.service.Split;

/**
 * <p>
 * A fused view providing read-write operations on multiple B+-Trees mapping
 * variable length unsigned byte[] keys to arbitrary values. The sources MUST
 * support deletion markers. The order of the sources MUST correspond to the
 * recency of their data. Writes will be directed to the first source in the
 * sequence (the most recent source). Deletion markers are used to prevent a
 * miss on a key for a source from reading through to an older source. If a
 * deletion marker is encoutered the index entry will be understood as "not
 * found" in the fused view rather than reading through to an older source where
 * it might still have a binding.
 * </p>
 * 
 * @todo consider implementing {@link IAutoboxBTree} here and collapsing
 *       {@link ILocalBTreeView} and {@link IAutoboxBTree}.
 * 
 * @todo Can I implement {@link ILinearList} here? That would make it possible
 *       to use keyAt() and indexOf() and might pave the way for a faster
 *       {@link DefaultSplitHandler} and also for a {@link MetadataService} that
 *       supports overflow since the index segments could be transparent at that
 *       point.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FusedView implements IIndex, ILocalBTreeView {//, IValueAge {

    protected static final Logger log = Logger.getLogger(FusedView.class);

    /**
     * Error message if the view has more than {@link Long#MAX_VALUE} elements
     * and you requested an exact range count.
     */
    static protected transient final String ERR_RANGE_COUNT_EXCEEDS_MAX_LONG = "The range count can not be expressed as a 64-bit signed integer";

    /**
     * A hard reference to the mutable {@link BTree} from index zero of the
     * sources specified to the ctor.
     */
    private final BTree btree;

    /**
     * Holds the various btrees that are the sources for the view.
     * 
     * FIXME Change this to assemble the AbstractBTree[] dynamically from the
     * {@link #btree} hard reference and hard references to the
     * {@link IndexSegmentStore} using
     * {@link IndexSegmentStore#loadIndexSegment()}. We could actually use hard
     * references for the index segments inside of a {@link WeakReference} to an
     * array of those references.
     */
    private final AbstractBTree[] srcs;

    /**
     * A {@link ThreadLocal} {@link Tuple} that is used to copy the value
     * associated with a key out of the btree during lookup operations.
     * <p>
     * Note: This field is NOT static. This limits the scope of the
     * {@link ThreadLocal} {@link Tuple} to the containing {@link FusedView}
     * instance.
     */
    protected final ThreadLocal<Tuple> lookupTuple = new ThreadLocal<Tuple>() {

        @Override
        protected com.bigdata.btree.Tuple initialValue() {

            return new Tuple(btree,VALS);

        }

    };
    
    /**
     * A {@link ThreadLocal} {@link Tuple} that is used for contains() tests.
     * The tuple does not copy either the keys or the values. Contains is
     * implemented as a lookup operation that either return this tuple or
     * <code>null</code>. When isolation is supported, the version metadata
     * is examined to determine if the matching entry is flagged as deleted in
     * which case contains() will report "false".
     * <p>
     * Note: This field is NOT static. This limits the scope of the
     * {@link ThreadLocal} {@link Tuple} to the containing {@link FusedView}
     * instance.
     */
    protected final ThreadLocal<Tuple> containsTuple = new ThreadLocal<Tuple>() {

        @Override
        protected com.bigdata.btree.Tuple initialValue() {

            return new Tuple(btree, 0);
            
        }
        
    };
    
    public String toString() {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append(getClass().getSimpleName());
        
        sb.append("{ ");

        sb.append(Arrays.toString(srcs));
        
        sb.append("}");
        
        return sb.toString();
        
    }
    
    final public AbstractBTree[] getSources() {

        // Note: clone the array to prevent modification.
        return srcs.clone();
        
    }

    public int getSourceCount() {
        
        return srcs.length;
        
    }
    
    public BTree getMutableBTree() {
        
        return (BTree) btree;
        
    }
    
    protected void assertNotReadOnly() {
        
        if (getMutableBTree().isReadOnly()) {

            // Can't write on this view.
            throw new IllegalStateException();
            
        }
        
    }
    
    public IResourceMetadata[] getResourceMetadata() {

        final IResourceMetadata[] resources = new IResourceMetadata[srcs.length];

        for (int i = 0; i < srcs.length; i++) {

            resources[i] = srcs[i].getStore().getResourceMetadata();

        }

        return resources;

    }

    public FusedView(AbstractBTree src1, AbstractBTree src2) {

        this(new AbstractBTree[] { src1, src2 });

    }

    /**
     * 
     * @param srcs
     *            The ordered sources for the fused view. The order of the
     *            elements in this array determines which value will be selected
     *            for a given key by lookup() and which value is retained by
     *            rangeQuery().
     * 
     * @exception IllegalArgumentException
     *                if a source is used more than once.
     * @exception IllegalArgumentException
     *                unless all sources have the same indexUUID
     * @exception IllegalArgumentException
     *                unless all sources support delete markers.
     */
    public FusedView(final AbstractBTree[] srcs) {
        
        checkSources(srcs);
        
        this.btree = (BTree) srcs[0];
        
        this.srcs = srcs.clone();
        
    }
    
    /**
     * Checks the sources to make sure that they are all either isolatable, 
     * all non-null, and all have the same index UUID.
     * 
     * @param srcs The sources for a view.
     */
    static void checkSources(final AbstractBTree[] srcs) {
        
        if (srcs == null)
            throw new IllegalArgumentException("sources is null");

        /*
         * @todo allow this as a degenerate case, or create a factory that
         * produces the appropriate view?
         */
        if (srcs.length < 2) {
            
            throw new IllegalArgumentException(
                    "At least two sources are required");
            
        }
        
        for (int i = 0; i < srcs.length; i++) {
            
            if (srcs[i] == null)
                throw new IllegalArgumentException("Source null @ index=" + i);

            if (!srcs[i].getIndexMetadata().getDeleteMarkers()) {

                throw new IllegalArgumentException(
                        "Source does not maintain delete markers @ index=" + i);

            }

            for (int j = 0; j < i; j++) {
                
                if (srcs[i] == srcs[j])
                    
                    throw new IllegalArgumentException(
                            "Source used more than once"
                            );
                

                if (!srcs[i].getIndexMetadata().getIndexUUID().equals(
                        srcs[j].getIndexMetadata().getIndexUUID())) {
                    
                    throw new IllegalArgumentException(
                            "Sources have different index UUIDs @ index=" + i
                            );
                    
                }
                
            }
            
        }
        
    }

    public IndexMetadata getIndexMetadata() {
        
        return btree.getIndexMetadata();
        
    }
    
    public IBloomFilter getBloomFilter() {
        
        // double checked locking.
        if (bloomFilter == null) {

            synchronized (this) {

                bloomFilter = new FusedBloomFilter();
                
            }

        }

        return bloomFilter;

    }

    private volatile IBloomFilter bloomFilter = null;

    synchronized final public ICounterSet getCounters() {

        if (counterSet == null) {

            counterSet = new CounterSet();

            for (int i = 0; i < srcs.length; i++) {
                
                counterSet.makePath("view[" + i + "]").attach(
                        srcs[i].getCounters());
            
            }

        }
        
        return counterSet;
        
    }
    private CounterSet counterSet;

    /**
     * The counter for the first source.
     */
    public ICounter getCounter() {
        
        return btree.getCounter();
        
    }
    
    /**
     * Resolves the old value against the view and then directs the write to the
     * first of the sources specified to the ctor.
     */
    public byte[] insert(final byte[] key, final byte[] value) {

        final byte[] oldval = lookup(key);
        
        btree.insert(key, value);
        
        return oldval;

    }
    
    public Object insert(Object key, Object val) {

        key = getTupleSerializer().serializeKey(key);

        val = getTupleSerializer().serializeVal(val);

        final ITuple tuple = lookup((byte[]) key, lookupTuple.get());

        // direct the write to the first source.
        btree.insert((byte[]) key, (byte[]) val);
        
        if (tuple == null || tuple.isDeletedVersion()) {

            /*
             * Either there was no entry under that key for any source or the
             * entry is already marked as deleted in the view.
             */
            
            return null;
            
        }

        return tuple.getObject();
        
    }

    /**
     * Resolves the old value against the view and then directs the write to the
     * first of the sources specified to the ctor. The remove is in fact treated
     * as writing a deleted marker into the index.
     */
    public byte[] remove(final byte[] key) {

        /*
         * Slight optimization prevents remove() from writing on the index if
         * there is no entry under that key for any source (or if there is
         * already a deleted entry under that key).
         */

        final Tuple tuple = lookup(key, lookupTuple.get());

        if (tuple == null || tuple.isDeletedVersion()) {

            /*
             * Either there was no entry under that key for any source or the
             * entry is already marked as deleted in the view so we are done.
             */
            
            return null;
            
        }

        final byte[] oldval = tuple.getValue();

        // remove from the 1st source.
        btree.remove(key);
        
        return oldval;

    }
    
    public Object remove(Object key) {

        key = getTupleSerializer().serializeKey(key);
        
        /*
         * Slight optimization prevents remove() from writing on the index if
         * there is no entry under that key for any source (or if there is
         * already a deleted entry under that key).
         */
        final Tuple tuple = lookup((byte[])key, lookupTuple.get());

        if (tuple == null || tuple.isDeletedVersion()) {

            /*
             * Either there was no entry under that key for any source or the
             * entry is already marked as deleted in the view so we are done.
             */
            
            return null;
            
        }

        // remove from the 1st source.
        btree.remove(key);

        return tuple.getObject();

    }

    /**
     * Return the first value for the key in an ordered search of the trees in
     * the view.
     */
    final public byte[] lookup(final byte[] key) {

        final Tuple tuple = lookup(key, lookupTuple.get());

        if (tuple == null || tuple.isDeletedVersion()) {

            /*
             * Interpret a deletion marker as "not found".
             */
            
            return null;
            
        }

        return tuple.getValue();
        
    }

    public Object lookup(Object key) {

        key = getTupleSerializer().serializeKey(key);

        final Tuple tuple = lookup((byte[]) key, lookupTuple.get());

        if (tuple == null || tuple.isDeletedVersion()) {

            /*
             * Interpret a deletion marker as "not found".
             */
            
            return null;
            
        }

        return tuple.getObject();
        
    }

    /**
     * Per {@link AbstractBTree#lookup(byte[], Tuple)} but0 processes the
     * {@link AbstractBTree}s in the view in their declared sequence and stops
     * when it finds the first index entry for the key, even it the entry is
     * marked as deleted for that key.
     * 
     * @param key
     *            The search key.
     * @param tuple
     *            A tuple to be populated with data and metadata about the index
     *            entry (required).
     * 
     * @return <i>tuple</i> iff an index entry was found under that <i>key</i>.
     */
    final public Tuple lookup(final byte[] key, final Tuple tuple) {

        return lookup(0, key, tuple);

    }

    /**
     * Core implementation processes the {@link AbstractBTree}s in the view in
     * their declared sequence and stops when it finds the first index entry for
     * the key, even it the entry is marked as deleted for that key.
     * 
     * @param startIndex
     *            The index of the first source to be read. This permits the
     *            lookup operation to start at an index into the {@link #srcs}
     *            other than zero. This is used by {@link IsolatedFusedView} to
     *            read from just the groundState (everything except the
     *            writeSet, which is the source at index zero(0)).
     * @param key
     *            The search key.
     * @param tuple
     *            A tuple to be populated with data and metadata about the index
     *            entry (required).
     * 
     * @return <i>tuple</i> iff an index entry was found under that <i>key</i>.
     */
    final protected Tuple lookup(final int startIndex, final byte[] key,
            final Tuple tuple) {

        for (int i = 0; i < srcs.length; i++) {

            if( srcs[i].lookup(key, tuple) == null) {
                
                // No match yet.
                
                continue;
                
            }

            return tuple;
            
        }

        // no match.
        
        return null;

    }
    
    /**
     * Processes the {@link AbstractBTree}s in the view in sequence and returns
     * true iff the first {@link AbstractBTree} with an index entry under the
     * key is non-deleted.
     */
    final public boolean contains(final byte[] key) {

        final Tuple tuple = lookup(key, containsTuple.get());
        
        if (tuple == null || tuple.isDeletedVersion()) {

            /*
             * Interpret a deletion marker as "not found".
             */
            
            return false;
            
        }

        return true;
        
    }

    public boolean contains(Object key) {
        
        key = getTupleSerializer().serializeKey(key);

        return contains((byte[]) key);
        
    }

    private ITupleSerializer getTupleSerializer() {
        
        return getIndexMetadata().getTupleSerializer();
        
    }
    
    /**
     * Returns the sum of the range count on each index in the view. This is the
     * maximum #of entries that could lie within that key range. However, the
     * actual number could be less if there are entries for the same key in more
     * than one source index.
     */
    final public long rangeCount() {

        return rangeCount(null/* fromKey */, null/* toKey */);
        
    }

    /**
     * Returns the sum of the range count on each index in the view. This is the
     * maximum #of entries that could lie within that key range. However, the
     * actual number could be less if there are entries for the same key in more
     * than one source index.
     * 
     * @todo this could be done using concurrent threads.
     */
    final public long rangeCount(byte[] fromKey, byte[] toKey) {

        if (fromKey == null || toKey == null) {

            /*
             * Note: When an index partition is split, the new index partitions
             * will initially use the same source index segments as the original
             * index partition. Therefore we MUST impose an explicit constraint
             * on the fromKey / toKey if none is given so that we do not read
             * tuples lying outside of the index partition boundaries! However,
             * if there is only a BTree in the view then the partition metadata
             * might not be defined, so we check for that first.
             */

            final LocalPartitionMetadata pmd = getIndexMetadata()
                    .getPartitionMetadata();

            if (pmd != null) {
            
                if (fromKey == null) {
                
                    fromKey = pmd.getLeftSeparatorKey();
                    
                }

                if (toKey == null) {

                    toKey = pmd.getRightSeparatorKey();

                }

            }

        }
        
        long count = 0;
        
        for (int i = 0; i < srcs.length; i++) {

            final long inc = srcs[i].rangeCount(fromKey, toKey);

            if (count + inc < count) {

                log.warn(ERR_RANGE_COUNT_EXCEEDS_MAX_LONG);
                
                return Long.MAX_VALUE;
                
            }

            count += inc;
            
        }
        
        return count;
        
    }

    /**
     * The exact range count is obtained using a key-range scan over the view.
     */
    final public long rangeCountExact(byte[] fromKey, byte[] toKey) {

        if (fromKey == null || toKey == null) {

            /*
             * Note: When an index partition is split, the new index partitions
             * will initially use the same source index segments as the original
             * index partition. Therefore we MUST impose an explicit constraint
             * on the fromKey / toKey if none is given so that we do not read
             * tuples lying outside of the index partition boundaries! However,
             * if there is only a BTree in the view then the partition metadata
             * might not be defined, so we check for that first.
             */

            final LocalPartitionMetadata pmd = getIndexMetadata()
                    .getPartitionMetadata();

            if (pmd != null) {
            
                if (fromKey == null) {
                
                    fromKey = pmd.getLeftSeparatorKey();
                    
                }

                if (toKey == null) {

                    toKey = pmd.getRightSeparatorKey();

                }

            }

        }

        final ITupleIterator itr = rangeIterator(fromKey, toKey,
                0/* capacity */, 0/* flags */, null/* filter */);

        long n = 0;

        while (itr.hasNext()) {

            itr.next();

            if (n == Long.MAX_VALUE)
                throw new RuntimeException(ERR_RANGE_COUNT_EXCEEDS_MAX_LONG);
            
            n++;

        }

        return n;
        
    }
    
    /**
     * An exact range count that includes any deleted tuples. This is obtained
     * using a key-range scan over the view.
     * 
     * @see #rangeCountExact(byte[], byte[])
     */
    public long rangeCountExactWithDeleted(byte[] fromKey, byte[] toKey) {

        if (fromKey == null || toKey == null) {

            /*
             * Note: When an index partition is split, the new index partitions
             * will initially use the same source index segments as the original
             * index partition. Therefore we MUST impose an explicit constraint
             * on the fromKey / toKey if none is given so that we do not read
             * tuples lying outside of the index partition boundaries! However,
             * if there is only a BTree in the view then the partition metadata
             * might not be defined, so we check for that first.
             */

            final LocalPartitionMetadata pmd = getIndexMetadata()
                    .getPartitionMetadata();

            if (pmd != null) {

                if (fromKey == null) {

                    fromKey = pmd.getLeftSeparatorKey();

                }

                if (toKey == null) {

                    toKey = pmd.getRightSeparatorKey();

                }

            }

        }

        // set the DELETED flag so we also see the deleted tuples.
        final Iterator itr = rangeIterator(fromKey, toKey, 0/* capacity */,
                IRangeQuery.DELETED/* flags */, null/* filter */);

        long n = 0L;

        while (itr.hasNext()) {

            itr.next();

            if (n == Long.MAX_VALUE)
                throw new RuntimeException(ERR_RANGE_COUNT_EXCEEDS_MAX_LONG);

            n++;

        }

        return n;

    }

    public ITupleIterator rangeIterator() {

        return rangeIterator(null, null);
        
    }

    /**
     * Returns an iterator that visits the distinct entries. When an entry
     * appears in more than one index, the entry is choosen based on the order
     * in which the indices were declared to the constructor.
     */
    final public ITupleIterator rangeIterator(final byte[] fromKey,
            final byte[] toKey) {

        return rangeIterator(fromKey, toKey, 0/* capacity */,
                DEFAULT/* flags */, null/* filter */);
        
    }

    /**
     * <p>
     * Core implementation.
     * </p>
     * <p>
     * Note: The {@link FusedView}'s iterator first obtains an ordered array of
     * iterators for each of the source {@link AbstractBTree}s. The <i>filter</i>
     * is NOT passed through to these source iterators. Instead, an
     * {@link FusedTupleIterator} is obtained and the filter is applied to that
     * iterator. This means that filters always see a fused representation of
     * the source iterators.
     * </p>
     * <p>
     * Note: This implementation supports {@link IRangeQuery#REVERSE}. This may
     * be used to locate the {@link ITuple} before a specified key, which is a
     * requirement for several aspects of the overall architecture including
     * atomic append of file blocks, locating an index partition in the metadata
     * index, and finding the last member of a set or map.
     * </p>
     * <p>
     * Note: When the {@link IRangeQuery#CURSOR} flag is specified, it is passed
     * through and an {@link ITupleCursor} is obtained for each source
     * {@link AbstractBTree}. A {@link FusedTupleCursor} is then obtained which
     * implements the {@link ITupleCursor} extensions.
     * </p>
     */
    @SuppressWarnings("unchecked")
    public ITupleIterator rangeIterator(//
            byte[] fromKey,//
            byte[] toKey, //
            final int capacity, //
            final int flags,//
            final IFilterConstructor filter//
            ) {

        if (fromKey == null || toKey == null) {

            /*
             * Note: When an index partition is split, the new index partitions
             * will initially use the same source index segments as the original
             * index partition. Therefore we MUST impose an explicit constraint
             * on the fromKey / toKey if none is given so that we do not read
             * tuples lying outside of the index partition boundaries! However,
             * if there is only a BTree in the view then the partition metadata
             * might not be defined, so we check for that first.
             */

            final LocalPartitionMetadata pmd = getIndexMetadata()
                    .getPartitionMetadata();

            if (pmd != null) {
            
                if (fromKey == null) {
                
                    fromKey = pmd.getLeftSeparatorKey();
                    
                }

                if (toKey == null) {

                    toKey = pmd.getRightSeparatorKey();

                }

            }

        }

        // reverse scan?
        final boolean reverseScan = (flags & REVERSE) != 0;
        
        // cursor requested?
        final boolean cursorMode = (flags & CURSOR) != 0;
        
        // read only?
        final boolean readOnly = ((flags & READONLY) != 0);

        // iff the aggregate iterator should visit deleted entries.
        final boolean deleted = (flags & DELETED) != 0;
        
        // removeAll?
        final boolean removeAll = (flags & REMOVEALL) != 0;
        
        if (readOnly && removeAll) {

            // REMOVEALL is not compatible with READONLY.
            throw new IllegalArgumentException();

        }

        final int n = srcs.length;

        if (log.isInfoEnabled())
            log.info("nsrcs=" + n + ", flags=" + flags + ", readOnly="
                    + readOnly + ", deleted=" + deleted + ", reverseScan="
                    + reverseScan);
        
        /*
         * Note: We request KEYS since we need to compare the keys in order to
         * decide which tuple to return next.
         * 
         * Note: We request DELETED so that we will see deleted entries. This is
         * necessary in order for processing to stop at the first entry for a
         * give key regardless of whether it is deleted or not. If the caller
         * does not want to see the deleted tuples, then they are silently
         * dropped from the aggregate iterator.
         * 
         * Note: The [filter] is NOT passed through to the source iterators.
         * This is because the filter must be applied to the aggregate iterator
         * in order to operate on the fused view.
         * 
         * Note: The REVERSE flag is NOT passed through to the source iterators.
         * It is handled below by layering on a filter.
         * 
         * Note: The REMOVEALL flag is NOT passed through to the source
         * iterators. It is handled below by laying on a filter.
         */
        final int sourceFlags = (//
                (flags | KEYS | DELETED)//
                | (reverseScan || removeAll ? CURSOR : 0) //
                )//
                & (~REMOVEALL)// turn off
                & (~REVERSE)// turn off
                ;
        
        /*
         * The source iterator produces a fused view of the source indices. We
         * then layer the filter(s) over the fused view iterator. A subclass is
         * used if CURSOR support is required for the fused view.
         */
        ITupleIterator src;
        
        if (cursorMode || removeAll || reverseScan) {
        
            /*
             * CURSOR was specified for the aggregate iterator or is required to
             * support REMOVEALL.
             */

            final ITupleCursor[] itrs = new ITupleCursor[n];

            for (int i = 0; i < n; i++) {

                itrs[i] = (ITupleCursor) srcs[i].rangeIterator(fromKey, toKey,
                        capacity, sourceFlags, null/* filter */);

            }

            // Note: aggregate source implements ITupleCursor.
            src = new FusedTupleCursor(flags, deleted, itrs, 
                    readOnly?new ReadOnlyIndex(this):this);
            
        } else {

            /*
             * CURSOR was neither specified nor required for the aggregate
             * iterator.
             * 
             * Note: If reverse was specified, then we pass it into the source
             * iterators for the source B+Trees. The resulting fused iterator
             * will already have reversal traversal semantics so we do not need
             * to layer a Reverserator on top.
             */

            final ITupleIterator[] itrs = new ITupleIterator[n];
            
            for (int i = 0; i < n; i++) {

               itrs[i] = srcs[i].rangeIterator(fromKey, toKey, capacity,
                        sourceFlags, null/* filter */);

            }

            src = new FusedTupleIterator(flags, deleted, itrs);

        }

        if (reverseScan) {

            /*
             * Reverse scan iterator.
             * 
             * Note: The reverse scan MUST be layered directly over the
             * ITupleCursor. Most critically, REMOVEALL combined with a REVERSE
             * scan needs to process the tuples in reverse index order and then
             * delete them as it goes.
             */

            src = new Reverserator((ITupleCursor) src);

        }

        if (filter != null) {

            /*
             * Apply the optional filter.
             * 
             * Note: This needs to be after the reverse scan and before
             * REMOVEALL (those are the assumptions for the flags).
             */
            
            src = filter.newInstance(src);
            
        }
        
        if ((flags & REMOVEALL) != 0) {
            
            assertNotReadOnly();
            
            /*
             * Note: This iterator removes each tuple that it visits from the
             * source iterator.
             */
            
            src = new TupleRemover() {
                private static final long serialVersionUID = 1L;
                @Override
                protected boolean remove(ITuple e) {
                    // remove all visited tuples.
                    return true;
                }
            }.filter(src);

        }
        
        return src;

    }
    
    final public Object submit(final byte[] key,
            final ISimpleIndexProcedure proc) {

        return proc.apply(this);

    }

    @SuppressWarnings("unchecked")
    final public void submit(byte[] fromKey, byte[] toKey,
            final IKeyRangeIndexProcedure proc, final IResultHandler handler) {

        if (fromKey == null) {

            /*
             * Note: When an index partition is split, the new index partitions
             * will initially use the same source index segments as the original
             * index partition. Therefore we MUST impose an explicit constraint
             * on the fromKey/toKey if none is given so that we do not read
             * tuples lying outside of the index partition boundaries!
             */
            fromKey = getIndexMetadata().getPartitionMetadata()
                    .getLeftSeparatorKey();

        }

        if (toKey == null) {

            /*
             * Note: When an index partition is split, the new index partitions
             * will initially use the same source index segments as the original
             * index partition. Therefore we MUST impose an explicit constraint
             * on the fromKey/toKey if none is given so that we do not read
             * tuples lying outside of the index partition boundaries!
             */
            toKey = getIndexMetadata().getPartitionMetadata()
                    .getRightSeparatorKey();

        }
        
        final Object result = proc.apply(this);
        
        if (handler != null) {
            
            handler.aggregate(result, new Split(null,0,0));
            
        }
        
    }
    
    @SuppressWarnings("unchecked")
    final public void submit(int fromIndex, int toIndex, byte[][] keys, byte[][] vals,
            AbstractKeyArrayIndexProcedureConstructor ctor, IResultHandler aggregator) {

        final Object result = ctor.newInstance(this, fromIndex, toIndex, keys,
                vals).apply(this);

        if (aggregator != null) {

            aggregator.aggregate(result, new Split(null, fromIndex, toIndex));

        }
        
    }

    /**
     * Inner class providing a fused view of the optional bloom filters
     * associated with each of the source indices.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class FusedBloomFilter implements IBloomFilter {

        /**
         * Unsupported operation.
         * 
         * @throws UnsupportedOperationException
         *             always.
         */
        public boolean add(byte[] key) {
            
            throw new UnsupportedOperationException();
            
        }

        /**
         * Applies the {@link IBloomFilter} for each source index in turn and
         * returns <code>true</code> if ANY of the component index filters
         * return <code>true</code> (if any filters say that their index has
         * data for that key then you need to read the index).
         */
        public boolean contains(final byte[] key) {

            final AbstractBTree[] srcs = getSources();
            
            for (int i = 0; i < srcs.length; i++) {

                final IBloomFilter filter = srcs[i].getBloomFilter();

                if (filter != null && filter.contains(key)) {

                    return true;
                    
                }

            }

            return false;

        }

        /**
         * This implementation notifies the bloom filter for the first source
         * index (if it exists). Normally false positives will be reported
         * directly to the specific bloom filter instance by the contains() or
         * lookup() method for that index. However, the
         * {@link AbstractAccessPath} also tests the bloom filter and needs a
         * means to report false positives. It should be the only one that calls
         * this method on this implementation class.
         */
        public void falsePos() {

            final IBloomFilter filter = btree.getBloomFilter();

            if (filter != null) {

                filter.falsePos();

            }
            
        }
        
    }

//    /*
//     * API used to report how long it has been since the BTree was last used.
//     * This is used to clear BTrees that are not in active use from a variety of
//     * caches. This helps us to better manage RAM.
//     */
//
//    final public void touch() {
//        
//        timestamp = System.nanoTime();
//        
//    }
//    
//    final public long timestamp() {
//        
//        return timestamp;
//        
//    }
//    
//    private long timestamp = System.nanoTime();
    
}
