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
package com.bigdata.journal;

import java.io.IOException;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;

/**
 * BTree mapping commit times to {@link ICommitRecord}s. The keys are the long
 * integers. The values are {@link Entry} objects recording the commit time of
 * the index and the address of the {@link ICommitRecord} for that commit time.
 * A canonicalizing cache is maintained such that the caller will never observe
 * distinct concurrent instances of the same {@link ICommitRecord}. This in
 * turn facilitates canonicalizing caches for objects loaded from that
 * {@link ICommitRecord}.
 */
public class CommitRecordIndex extends BTree {

    /**
     * Instance used to encode the timestamp into the key.
     */
    private IKeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG);
    
    /**
     * A weak value cache for {@link ICommitRecord}s. Note that lookup may be
     * by exact match -or- by the record have the largest timestamp LTE to the
     * given probe. For the latter, we have to determine the timestamp of the
     * matching record and use that to test the cache (after we have already
     * done the lookup in the index).
     * <p>
     * The main purpose of this cache is not to speed up access to historical
     * {@link ICommitRecord}s but rather to establish a canonicalizing lookup
     * service for {@link ICommitRecord}s so that we can in turn establish a
     * canonicalizing mapping for read-only objects (typically the unnamed
     * indices) loaded from a given {@link ICommitRecord}.
     */
    final private WeakValueCache<Long, ICommitRecord> cache = new WeakValueCache<Long, ICommitRecord>(
            new LRUCache<Long, ICommitRecord>(10));

    /**
     * Create a new instance.
     * 
     * @param store
     *            The backing store.
     * 
     * @return The new instance.
     */
    static public CommitRecordIndex create(IRawStore store) {
    
        IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        metadata.setClassName(CommitRecordIndex.class.getName());
        
        return (CommitRecordIndex) BTree.create(store, metadata);
        
    }

    /**
     * Load from the store.
     * 
     * @param store
     *            The backing store.
     * @param checkpoint
     *            The {@link Checkpoint} record.
     * @param metadataId
     *            The metadata record for the index.
     */
    public CommitRecordIndex(IRawStore store, Checkpoint checkpoint, IndexMetadata metadata) {

        super(store, checkpoint, metadata);

    }
    
    /**
     * Encodes the commit time into a key.
     * 
     * @param commitTime
     *            The commit time.
     * 
     * @return The corresponding key.
     */
    protected byte[] getKey(long commitTime) {

        /*
         * Note: The {@link UnicodeKeyBuilder} is NOT thread-safe
         */
        return keyBuilder.reset().append(commitTime).getKey();

    }

    /**
     * Existence test for a commit record with the specified commit timestamp
     * (exact match).
     * 
     * @param commitTime
     *            The commit timestamp.
     * 
     * @return true iff such an {@link ICommitRecord} exists in the index with
     *         that commit timestamp (exact match(.
     */
    synchronized public boolean hasTimestamp(long commitTime) {
        
        return super.contains(getKey(commitTime));
        
    }
    
    /**
     * Return the {@link ICommitRecord} with the given timestamp (exact match).
     * 
     * @param commitTime
     *            The commit time.
     * 
     * @return The {@link ICommitRecord} index or <code>null</code> iff there
     *         is no {@link ICommitTimestamp} for that commit time.
     */
    synchronized public ICommitRecord get(long commitTime) {

        ICommitRecord commitRecord;

        // exact match cache test.
        commitRecord = cache.get(commitTime);
        
        if (commitRecord != null)
            return commitRecord;

        // exact match index lookup.
        final byte[] val = super.lookup(getKey(commitTime));

        if (val == null) {

            // nothing under that key.
            return null;
            
        }
        
        // deserialize the entry.
        final Entry entry = deserializeEntry(new DataInputBuffer(val));

        /*
         * re-load the commit record from the store.
         */
        commitRecord = loadCommitRecord(store,entry.addr);
        
        /*
         * save commit time -> commit record mapping in transient cache.
         */
        cache.put(commitRecord.getTimestamp(),commitRecord,false/*dirty*/);

        // return btree.
        return commitRecord;

    }

    /**
     * Return the {@link ICommitRecord} having the largest timestamp that is
     * less than or equal to the given timestamp. This is used primarily to
     * locate the commit record that will serve as the ground state for a
     * transaction having <i>timestamp</i> as its start time. In this context
     * the LTE search identifies the most recent commit state that not later
     * than the start time of the transaction.
     * 
     * @param timestamp
     *            The given timestamp (may be negative for historical reads or
     *            positive for transaction identifiers, but MAY NOT be
     *            {@link ITx#UNISOLATED} NOR {@link ITx#READ_COMMITTED}).
     * 
     * @return The commit record -or- <code>null</code> iff there are no
     *         {@link ICommitRecord}s in the index that satisify the probe.
     * 
     * @see #get(long)
     */
    synchronized public ICommitRecord find(long timestamp) {

        if (timestamp == ITx.UNISOLATED) {

            throw new IllegalArgumentException("Can not specify 'UNISOLATED' as timestamp");
            
        }

        if (timestamp == ITx.READ_COMMITTED) {

            throw new IllegalArgumentException("Can not specify 'READ_COMMITTED' as timestamp");
            
        }
        
        // find (first less than or equal to).
        final int index = findIndexOf(Math.abs(timestamp));
        
        if(index == -1) {
            
            // No match.
            
            return null;
            
        }
        
        return valueAtIndex(index);

    }

    /**
     * Find the first commit record strictly greater than the timestamp.
     * 
     * @param timestamp
     *            The timestamp.
     * 
     * @return The commit record -or- <code>null</code> if there is no commit
     *         record whose timestamp is strictly greater than <i>timestamp</i>.
     */
    synchronized public ICommitRecord findNext(long timestamp) {

        if (timestamp == ITx.UNISOLATED) {

            throw new IllegalArgumentException("Can not specify 'UNISOLATED' as timestamp");
            
        }

        if (timestamp == ITx.READ_COMMITTED) {

            throw new IllegalArgumentException("Can not specify 'READ_COMMITTED' as timestamp");
            
        }
        
        // find first strictly greater than.
        final int index = findIndexOf(Math.abs(timestamp)) + 1;
        
        if (index == nentries) {

            // No match.

            return null;
            
        }
        
        return valueAtIndex(index);

    }

    /**
     * Return the commit record at the index.
     * 
     * @param index
     *            The index.
     * @return
     * 
     * @see #findIndexOf(long)
     */
    private ICommitRecord valueAtIndex(int index) {
        
        /*
         * Retrieve the entry for the commit record from the index.  This
         * also stores the actual commit time for the commit record.
         */
        final Entry entry = deserializeEntry( new DataInputBuffer( super.valueAt( index ) ));

        /*
         * Test the cache for this commit record using its actual commit time.
         */
        ICommitRecord commitRecord = cache.get(entry.commitTime);

        if(commitRecord == null) {
            
            /*
             * Load the commit record from the store using the address stored in
             * the entry.
             */ 
        
            commitRecord = loadCommitRecord(store,entry.addr);

            assert entry.commitTime == commitRecord.getTimestamp();
            
            /*
             * Insert the commit record into the cache usings its actual commit
             * time.
             */
            cache.put(entry.commitTime,commitRecord,false/* dirty */);
            
        }
        
        return commitRecord;

    }
    
    /**
     * Find the index of the {@link ICommitRecord} having the largest timestamp
     * that is less than or equal to the given timestamp.
     * 
     * @return The index of the {@link ICommitRecord} having the largest
     *         timestamp that is less than or equal to the given timestamp -or-
     *         <code>-1</code> iff there are no {@link ICommitRecord}s
     *         defined.
     */
    synchronized public int findIndexOf(long timestamp) {
        
        int pos = super.indexOf(getKey(timestamp));
        
        if (pos < 0) {

            /*
             * the key lies between the entries in the index, or possible before
             * the first entry in the index. [pos] represents the insert
             * position. we convert it to an entry index and subtract one to get
             * the index of the first commit record less than the given
             * timestamp.
             */
            
            pos = -(pos+1);

            if(pos == 0) {

                // No entry is less than or equal to this timestamp.
                return -1;
                
            }
                
            pos--;

            return pos;
            
        } else {
            
            /*
             * exact hit on an entry.
             */
            
            return pos;
            
        }

    }
    
    /**
     * Re-load a commit record from the store.
     * 
     * @param store
     *            The store.
     * @param addr
     *            The address of the {@link ICommitRecord}.
     * 
     * @return The {@link ICommitRecord} loaded from the specified address.
     */
    protected ICommitRecord loadCommitRecord(IRawStore store, long addr) {
        
        return CommitRecordSerializer.INSTANCE.deserialize(store.read(addr));

    }
    
    /**
     * Add an entry for a commit record.
     * 
     * @param commitRecordAddr
     *            The address at which that commit record was written on the
     *            store.
     * @param commitRecord
     *            The commit record.
     * 
     * @exception IllegalArgumentException
     *                if <i>commitRecord</i> is <code>null</code>.
     * @exception IllegalArgumentException
     *                if there is already a {@link ICommitRecord} registered
     *                under for the {@link ICommitRecord#getTimestamp()}.
     * @exception IllegalArgumentException
     *                if <i>addr</i> is invalid.
     */
    synchronized public void add(long commitRecordAddr, ICommitRecord commitRecord) {
        
        if (commitRecord == null)
            throw new IllegalArgumentException();

        if (commitRecordAddr == 0L)
            throw new IllegalArgumentException();

        final long commitTime = commitRecord.getTimestamp();
        
        final byte[] key = getKey(commitTime);
        
        if(super.contains(key)) {
            
            throw new IllegalArgumentException(
                    "commit record exists: timestamp=" + commitTime);
            
        }
        
        // add a serialized entry to the persistent index.
        super.insert(key,
                serializeEntry(new Entry(commitTime, commitRecordAddr)));
        
        // should not be an existing entry for that commit time.
        assert cache.get(commitTime) == null;
        
        // add to the transient cache.
        cache.put(commitTime, commitRecord, false/*dirty*/);
        
    }

    /**
     * An entry in the persistent index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Entry {
       
        /**
         * The commit time.
         */
        public final long commitTime;
        
        /**
         * The address of the {@link ICommitRecord} whose commit timestamp is
         * {@link #commitTime}.
         */
        public final long addr;
        
        public Entry(long commitTime,long addr) {
            
            this.commitTime = commitTime;
            
            this.addr = addr;
            
        }
        
    }
    
    /**
     * Private buffer used within sychronized contexts to serialize
     * {@link Entry}s.
     */
    private DataOutputBuffer out = new DataOutputBuffer(Bytes.SIZEOF_LONG*2);
    
    /**
     * Serialize an {@link Entry}.
     * 
     * @param entry
     *            The entry.
     * 
     * @return The serialized entry.
     */
    protected byte[] serializeEntry(Entry entry) {
        
        out.reset();

        out.putLong(entry.commitTime);
        
        out.putLong(entry.addr);
        
        return out.toByteArray();
        
    }

    /**
     * De-serialize an {@link Entry}.
     * 
     * @param is
     *            The serialized data.
     *            
     * @return The {@link Entry}.
     */
    protected Entry deserializeEntry(DataInputBuffer is) {

        try {

            long commitTime = is.readLong();
            
            long addr = is.readLong();
            
            return new Entry(commitTime, addr);
            
        } catch (IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }
    
}
