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
/*
 * Created on Feb 7, 2007
 */

package com.bigdata.scaleup;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BatchContains;
import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.BatchLookup;
import com.bigdata.btree.BatchRemove;
import com.bigdata.btree.EmptyEntryIterator;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IFusedView;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.ReadOnlyFusedView;
import com.bigdata.journal.ICommitter;
import com.bigdata.journal.Journal;

/**
 * A mutable B+-Tree that is dynamically partitioned into one or more key
 * ranges. Each key range is a <i>partition</i>. A partition is defined by the
 * lowest value key that can enter that partition (the separator key). A
 * {@link MetadataIndex} contains the definitions of the partitions. Each
 * partition has a mutable {@link BTree} that will absorb writes for a
 * partition. The same {@link BTree} is used to absorb writes distined for any
 * partitions of the same index on the same {@link Journal}. This class
 * internally tracks whether or not a write has occurred on each partition. That
 * information is used to guide eviction decisions when the
 * {@link Journal#overflow()}s and buffered writes are migrated onto one or
 * more {@link IndexSegment}s each of which contains historical read-only data
 * for a single index partition.
 * <p>
 * Writes on a {@link PartitionIndex} simply write through to the {@link BTree}
 * corresponding to that index (having the same name) on the
 * {@link Journal journal} (they are effectively multiplexed on the journal).
 * <p>
 * Read operations process the spanned partitions sequence so that the
 * aggregated results are in index order. For each partition, the read request
 * is constructed using ah {@link IFusedView} of the mutable {@link BTree} used
 * to absorb writes and the live {@link IndexSegment}s for that partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo add a restart safe data counter for each index partition that may be
 *       used to assign 64-bit unique identifiers in conjunction with the 32-bit
 *       unique (within index) partition identifier assigned by the
 *       {@link MetadataIndex} to each index partition.
 * 
 * @todo add a restart safe data structure tracking each partition for which
 *       writes have been absorbed on the {@link Journal}. this could be just
 *       an ordered set of partition identifiers for which writes have been
 *       absorbed that is maintained by an insertion sort, an ordered array of
 *       <partId:writeCounter>, or a btree mapping partition identifiers to
 *       write counters. The data need to be associated as part of the metadata
 *       for the btree that is absorbing writes for that partition. The simplest
 *       way to do this is to subclass BTree or UnisolatedBTree so that they are
 *       partition aware and automatically track the #of writes destined for
 *       each partition.
 * 
 * @todo For a scale-out solution the clients are responsible for talking with
 *       the metadata service and locating the data service for each partition.
 *       The client is responsible for directing the writes to the correct data
 *       service. The data service simply fronts for the journal and directs the
 *       writes onto the {@link BTree} absorbing writes for the named index. It
 *       is a (client) error for a write to be directed to a {@link Journal} not
 *       tasked with buffering data for the partition into which that write
 *       should go.
 */
public class PartitionedIndexView implements IIndex, ICommitter {

    /**
     * The mutable {@link BTree} used to absorb all writes for the
     * {@link PartitionedIndexView}.
     */
    protected final BTree btree;

    /**
     * The metadata index used to locate the partitions relevant to a given read
     * operation.
     */
    private final MetadataIndex mdi;

    /**
     * A cache of the fused views for in use partitions.
     * 
     * @todo reconcile this with
     *       {@link MasterJournal#getIndex(String, IResourceMetadata)}
     *       which provides a weak value cache for index segments.
     */
    private final Map<Integer,ReadOnlyFusedView> views = new HashMap<Integer,ReadOnlyFusedView>();

    public MasterJournal getMaster() {
        
        return getSlave().getMaster();
        
    }
    
    public SlaveJournal getSlave() {
        
        return (SlaveJournal)getBTree().getStore();
        
    }
    
    /**
     * The mutable {@link BTree} used to absorb writes.
     */
    public BTree getBTree() {
        
        return btree;
        
    }
    
    /**
     * Opens and returns the live {@link IndexSegment}s for the partition.
     * These {@link IndexSegments} MUST be explicitly closed in order to reclaim
     * resources and release the lock on the backing files.
     * 
     * @param pmd
     *            The partition.
     * 
     * @return The live {@link IndexSegment}s for that partition.
     */
    protected IndexSegment[] openIndexSegments(PartitionMetadata pmd) {
        
        final int liveCount = pmd.getLiveCount();
        
        IndexSegment[] segs = new IndexSegment[liveCount];
        
        MasterJournal master = getMaster();
        
        int n = 0;
        
        IResourceMetadata[] resources = pmd.getResources();
        
        for(int i=0; i<resources.length; i++) {
            
            if(resources[i].state() != ResourceState.Live) continue;
            
            segs[n++] = (IndexSegment) master.getIndex(getName(), resources[i]);
            
        }
        
        assert n == liveCount;
        
        return segs;

    }

    /**
     * Close all open views, including any backing index segments.
     */
    protected void closeViews() {

        Iterator<Map.Entry<Integer,ReadOnlyFusedView>> itr = views.entrySet().iterator();
        
        while(itr.hasNext()) {
            
            ReadOnlyFusedView view = itr.next().getValue();
            
            // @todo assumes one open segment per view.
            IndexSegment seg = (IndexSegment)view.srcs[1];
            
            seg.close();
            
        }

        views.clear();
        
    }
    
    /**
     * Return a fused view on the partition in which the key would be found. The
     * view is cached and will be reused on subsequent requests. This operation
     * can have high latency if the view for the partition is not in the cache.
     * 
     * @param key
     *            The search key.
     * 
     * @return The fused view.
     * 
     * @todo use an weak value LRU policy to close out partitions that are not
     *       being actively used. note that we still need to know which
     *       partitions have been touched on a {@link PartitionedIndexView} when we
     *       export its data in response to a journal overflow event. This
     *       suggests a hard reference queue to hold onto recently used views
     *       and a persistent bitmap or flag in the metadata index to identify
     *       those partitions for which we need to export data. Note that we
     *       MUST hold onto any view that is currently being used to report data
     *       to the application, e.g., by a striterator. This means that we need
     *       to use a hash map with weak or soft reference values.
     *       <p>
     *       Alternatively, perhaps we can write a custom iterator over the
     *       mutable btree for the index that automatically exports or performs
     *       a compacting merge for each partition for which it encounters data
     *       on an entry scan.
     */
    protected IIndex getView(byte[] key) {
        
        PartitionMetadata pmd = mdi.find(key);
        
        ReadOnlyFusedView view = views.get(pmd.getPartitionId());
        
        if(view==null) {
            
            if(pmd.getLiveCount()==0) {
                
                // the btree is the view.
                return btree;
                
            }
            
            /*
             * Open the live index segments for this partition (high latency).
             */
            
            IndexSegment[] segs = openIndexSegments(pmd);
            
            AbstractBTree[] sources = new AbstractBTree[segs.length+1];
            
            // the mutable btree.
            sources[0] = btree;
            
            // the immutable historical segments.
            System.arraycopy(segs, 0, sources, 1, segs.length);
            
            // create the fused view.
            view = new ReadOnlyFusedView(sources);
            
            // place the view in the cache.
            views.put(pmd.getPartitionId(), view);
            
        }
        
        return view;
        
    }

    /**
     * Return the resources required to provide a coherent view of the partition
     * in which the key is found.
     * 
     * @param key
     *            The key.
     * 
     * @return The resources required to read on the partition containing that
     *         key. The resources are arranged in reverse timestamp order
     *         (increasing age).
     * 
     * @todo reconcile with {@link #getView(byte[])}?
     */
    protected IResourceMetadata[] getResources(byte[] key) {

        JournalMetadata journalResource = new JournalMetadata((Journal) btree
                .getStore(), ResourceState.Live);

        PartitionMetadata pmd = mdi.find(key);

        final int liveCount = pmd.getLiveCount();

        IResourceMetadata[] tmp = new IResourceMetadata[liveCount + 1];
        
        int n = 0;

        tmp[n++] = journalResource;

        // @todo this should probably be a loop out to pmd.getResources().length
        for (int i = 0; i < tmp.length; i++) {

            // @todo refactor out of loop.
            IResourceMetadata seg = pmd.getResources()[i];

            if (seg.state() != ResourceState.Live)
                continue;

            tmp[n++] = seg;

        }

        assert n == tmp.length;
        
        return tmp;

    }
    
    public String getName() {
        
        return mdi.getManagedIndexName();
        
    }
    
    /**
     * @param btree
     *            The mutable {@link BTree} used to absorb all writes for the
     *            {@link PartitionedIndexView}.
     * @param mdi
     *            The metadata index used to locate the partitions relevant to a
     *            given read operation.
     */
    public PartitionedIndexView(BTree btree, MetadataIndex mdi) {
        
        this.btree = btree;
        
        this.mdi = mdi;
        
    }

    /*
     * IIndex
     */
    
    /**
     * The UUID for the scale-out index.
     */
    public UUID getIndexUUID() {
        
        return mdi.getManagedIndexUUID();
        
    }
    
    /*
     * Non-batch API.
     * 
     * The write operations are trivial since we always direct them to the named
     * btree on the journal.
     * 
     * The point read operations are also trivial conceptually. The are always
     * directed to the fused view of the mutable btree on the journal and the
     * live index segments for the partition that spans the search key.
     * 
     * The rangeCount and rangeIterator operations are more complex. The need to
     * be processed by each partition spanned by the from/to key range. We need
     * to present the operation to each partition in turn and append the results
     * together.
     */

    public Object insert(Object key, Object value) {

        return btree.insert(key, value);
        
    }

    public Object remove(Object key) {

        return btree.remove(key);
        
    }

    public boolean contains(byte[] key) {

        return getView(key).contains(key);
        
    }

    public Object lookup(Object key) {

        return getView((byte[])key).lookup(key);
        
    }

    /**
     * For each partition spanned by the key range, report the sum of the counts
     * for each source in the view for that partition (since this is based on 
     * {@link ReadOnlyFusedView}s for each partition, it will may overcount the #of
     * entries actually in the range on each partition).
     * <p>
     * If the count would exceed {@link Integer#MAX_VALUE} then the result is
     * {@link Integer#MAX_VALUE}.
     */
    public int rangeCount(byte[] fromKey, byte[] toKey) {

        // index of the first partition to check.
        final int fromIndex = (fromKey == null ? 0 : mdi.findIndexOf(fromKey));

        // index of the last partition to check.
        final int toIndex = (toKey == null ? mdi.getEntryCount()-1 : mdi.findIndexOf(toKey));

        // per javadoc, keys out of order returns zero(0).
        if(toIndex<fromIndex) return 0;

        // use to counters so that we can look for overflow.
        int count = 0;
        int lastCount = 0;
        
        for( int index = fromIndex; index<=toIndex; index++) {

            // The first key that would enter the nth partition.
            byte[] separatorKey = mdi.keyAt(index);

            // Add in the count from that partition.
            count += getView(separatorKey).rangeCount(fromKey, toKey);
            
            if(count<lastCount) {
                
                return Integer.MAX_VALUE;
                
            }
            
            lastCount = count;
            
        }
        
        return count;
        
    }

    /**
     * Return an iterator that visits all values in the key range in key order.
     * The iterator will visit each partition spanned by the key range in turn.
     */
    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {

        // index of the first partition to check.
        final int fromIndex = (fromKey == null ? 0 : mdi.findIndexOf(fromKey));

        // index of the last partition to check.
        final int toIndex = (toKey == null ? mdi.getEntryCount()-1 : mdi.findIndexOf(toKey));

        // keys are out of order.
        if(toIndex<fromIndex) return EmptyEntryIterator.INSTANCE;

        // iterator that will visit all key/vals in that range.
        return new PartitionedRangeIterator(fromKey, toKey, fromIndex, toIndex);

    }
    
    /*
     * Batch API.
     * 
     * The write operations are trivial since we always direct them to the named
     * btree on the journal.
     * 
     * The read operations are more complex. We need to partition the set of
     * keys based on the partitions to which those keys would be directed. This
     * is basically a join against the {@link MetadataIndex}. This operation is
     * also required on the rangeCount and rangeIterator methods on the
     * non-batch api.
     */

    public void insert(BatchInsert op) {

        btree.insert(op);

    }

    public void remove(BatchRemove op) {

        btree.remove(op);

    }

    // FIXME contains(batch)
    public void contains(BatchContains op) {

        throw new UnsupportedOperationException();

    }

    // FIXME lookup(batch)
    public void lookup(BatchLookup op) {

        throw new UnsupportedOperationException();

    }

    /**
     * An iterator that visits all key/value entries in a specified key range
     * across one or more partitions.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class PartitionedRangeIterator implements IEntryIterator {

        /**
         * The first key to visit or null iff there is no lower bound.
         */
        private final byte[] fromKey;

        /**
         * The first key NOT to visit or null iff there is no upper bound.
         */
        private final byte[] toKey;

        /**
         * The index of the first partition spanned by the key range.
         */
        private final int fromIndex;

        /**
         * The index of the last partition spanned by the key range.
         */
        private final int toIndex;

        /**
         * The index of the partition whose entries are currently being visited.
         */
        private int index;
        
        /**
         * The iterator for the current partition.
         */
        private IEntryIterator src;

        /**
         * The last visited key and null iff we have not visited anything yet.
         */
        private byte[] key = null;

        /**
         * The last visited value.
         */
        private Object val = null;

        /**
         * 
         * @param fromKey
         *            The first key to visit or null iff there is no lower
         *            bound.
         * @param toKey
         *            The first key NOT to visit or null iff there is no upper
         *            bound.
         * @param fromIndex
         *            The index of the first partition spanned by the key range.
         * @param toIndex
         *            The index of the last partition spanned by the key range.
         */
        public PartitionedRangeIterator(byte[] fromKey, byte[] toKey, int fromIndex,int toIndex) {

            assert fromIndex >= 0;
            assert toIndex >= fromIndex;
            
            this.fromKey = fromKey;
            
            this.toKey = toKey;
            
            this.fromIndex = fromIndex;
            
            this.toIndex = toIndex;

            // the first partition to visit.
            this.index = fromIndex;
            
            // The first key that would enter that partition.
            byte[] separatorKey = mdi.keyAt(index);

            // The rangeIterator for that partition.
            src = getView(separatorKey).rangeIterator(fromKey, toKey);
            
        }

        public boolean hasNext() {

            if(src.hasNext()) return true;

            // The current partition has been exhausted.
            if(index < toIndex) {
                
                // the next partition to visit.
                index++;
                
                // The first key that would enter that partition.
                byte[] separatorKey = mdi.keyAt(index);

                // The rangeIterator for that partition.
                src = getView(separatorKey).rangeIterator(fromKey, toKey);

                return src.hasNext();

            } else {

                // All partitions have been exhausted.
                return false;
            
            }
            
        }

        public Object next() {

            if (!hasNext()) {

                throw new NoSuchElementException();

            }

            /*
             * eagerly fetch the key and value so that we can report them using
             * getKey() and getValue()
             */
            val = src.next();
            key = src.getKey();

            return val;

        }

        public Object getValue() {

            if (key == null) {
                
                // nothing has been visited yet.
                throw new IllegalStateException();
                
            }

            return val;

        }

        public byte[] getKey() {

            if (key == null) {
                
                // nothing has been visited yet.
                throw new IllegalStateException();
                
            }
            
            return key;

        }

        /**
         * Not supported.
         * 
         * @exception UnsupportedOperationException
         *                Always.
         */
        public void remove() {

            throw new UnsupportedOperationException();

        }

    }

    /**
     * The commit is delegated to the mutable B+-Tree since that is where all
     * the writes are absorbed.
     */
    public long handleCommit() {

        return btree.handleCommit();
        
    }

}
