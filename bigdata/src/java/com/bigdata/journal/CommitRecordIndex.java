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
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;

/**
 * BTree mapping commit times to {@link ICommitRecord}s. The keys are the long
 * integers corresponding to the timestamps assigned to commit points in the
 * store. The values are {@link Entry} objects recording the commit time of the
 * index and the address of the {@link ICommitRecord} for that commit time. A
 * canonicalizing cache is maintained such that the caller will never observe
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
    static public CommitRecordIndex create(final IRawStore store) {
    
        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        metadata.setBTreeClassName(CommitRecordIndex.class.getName());
        
        metadata.setTupleSerializer(new CommitRecordIndexTupleSerializer(
                new ASCIIKeyBuilderFactory(Bytes.SIZEOF_LONG)));
        
        return (CommitRecordIndex) BTree.create(store, metadata);
        
    }

    static public CommitRecordIndex createTransient() {
        
        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        metadata.setBTreeClassName(CommitRecordIndex.class.getName());
        
        metadata.setTupleSerializer(new CommitRecordIndexTupleSerializer(
                new ASCIIKeyBuilderFactory(Bytes.SIZEOF_LONG)));
        
        return (CommitRecordIndex) BTree.createTransient(metadata);
        
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
    public CommitRecordIndex(IRawStore store, Checkpoint checkpoint,
            IndexMetadata metadata, boolean readOnly) {

        super(store, checkpoint, metadata, readOnly);

        this.ser = new Entry.EntrySerializer();
        
    }
    
    /**
     * Used to (de-)serialize {@link Entry}s (NOT thread-safe).
     */
    private final Entry.EntrySerializer ser;
    
    /**
     * Encodes the commit time into a key.
     * 
     * @param commitTime
     *            The commit time.
     * 
     * @return The corresponding key.
     */
    private byte[] getKey(long commitTime) {

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
        final Entry entry = ser.deserializeEntry(new DataInputBuffer(val));

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
    synchronized public ICommitRecord find(final long timestamp) {

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
    synchronized public ICommitRecord findNext(final long timestamp) {

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
    private ICommitRecord valueAtIndex(final int index) {
        
        /*
         * Retrieve the entry for the commit record from the index.  This
         * also stores the actual commit time for the commit record.
         */
        final Entry entry = ser.deserializeEntry( new DataInputBuffer( super.valueAt( index ) ));

        return fetchCommitRecord(entry);
        
    }
    
    /**
     * Materialize a commit record, from cache if possible.
     * <p>
     * Note: This DOES NOT perform lookup of the commit time!
     * 
     * @param entry An {@link Entry}.
     * 
     * @return The {@link ICommitRecord}.
     * 
     * @see #get(long)
     */
    public ICommitRecord fetchCommitRecord(final Entry entry) {

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
    synchronized public void add(final long commitRecordAddr, final ICommitRecord commitRecord) {
        
        if (commitRecord == null)
            throw new IllegalArgumentException();

        if (commitRecordAddr == 0L)
            throw new IllegalArgumentException();

        final long commitTime = commitRecord.getTimestamp();
        
        final byte[] key = getKey(commitTime);
        
// Note: modified to allow replay of historical transactions.
//        if(super.contains(key)) {
//            
//            throw new IllegalArgumentException(
//                    "commit record exists: timestamp=" + commitTime);
//            
//        }
        if(!super.contains(key)) {
        // add a serialized entry to the persistent index.
        super.insert(key,
                ser.serializeEntry(new Entry(commitTime, commitRecordAddr)));
        
        // should not be an existing entry for that commit time.
        assert cache.get(commitTime) == null;
        
        // add to the transient cache.
        cache.put(commitTime, commitRecord, false/*dirty*/);
  	    } else {
			log.warn("Historical commit record exists: timestamp="+commitTime);
		}
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
        
        /**
         * Used to (de-)serialize {@link Entry}s (NOT thread-safe).
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        public static class EntrySerializer {

            /**
             * Private buffer used within sychronized contexts to serialize
             * {@link Entry}s.
             */
            private final DataOutputBuffer out = new DataOutputBuffer(
                    Bytes.SIZEOF_LONG * 2);

            /**
             * Serialize an {@link Entry}.
             * 
             * @param entry
             *            The entry.
             * 
             * @return The serialized entry.
             */
            public byte[] serializeEntry(Entry entry) {

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
            public Entry deserializeEntry(DataInputBuffer is) {

                try {

                    long commitTime = is.readLong();

                    long addr = is.readLong();

                    return new Entry(commitTime, addr);

                } catch (IOException ex) {

                    throw new RuntimeException(ex);

                }

            }

        }
        
    }
    
    /**
     * Encapsulates key and value formation for the {@link CommitRecordIndex}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class CommitRecordIndexTupleSerializer extends
            DefaultTupleSerializer<Long, Entry> {

        /**
         * 
         */
        private static final long serialVersionUID = 1;

        /**
         * Used to (de-)serialize {@link Entry}s (NOT thread-safe).
         */
        private final transient Entry.EntrySerializer ser;
        
        /**
         * De-serialization ctor.
         */
        public CommitRecordIndexTupleSerializer() {

            super();

            this.ser = new Entry.EntrySerializer();
            
        }

        /**
         * Ctor when creating a new instance.
         * 
         * @param keyBuilderFactory
         */
        public CommitRecordIndexTupleSerializer(
                final IKeyBuilderFactory keyBuilderFactory) {
            
            super(keyBuilderFactory);
            
            this.ser = new Entry.EntrySerializer();

        }
        
        /**
         * Decodes the key as a commit time.
         */
        @Override
        public Long deserializeKey(ITuple tuple) {

            final byte[] key = tuple.getKeyBuffer().array();

            final long id = KeyBuilder.decodeLong(key, 0);

            return id;

        }

        /**
         * Return the unsigned byte[] key for a commit time.
         * 
         * @param obj
         *            A commit time.
         */
        @Override
        public byte[] serializeKey(Object obj) {

            return getKeyBuilder().reset().append((Long) obj).getKey();

        }

        /**
         * Return the byte[] value an {@link Entry}.
         * 
         * @param entry
         *            An Entry.
         */
        public byte[] serializeVal(Entry entry) {
            
            return ser.serializeEntry(entry);

        }

        @Override
        public Entry deserialize(ITuple tuple) {

            return ser.deserializeEntry(tuple.getValueStream());

        }

        /**
         * The initial version (no additional persistent state).
         */
        private final static transient byte VERSION0 = 0;

        /**
         * The current version.
         */
        private final static transient byte VERSION = VERSION0;

        public void readExternal(final ObjectInput in) throws IOException,
                ClassNotFoundException {

            super.readExternal(in);
            
            final byte version = in.readByte();
            
            switch (version) {
            case VERSION0:
                break;
            default:
                throw new UnsupportedOperationException("Unknown version: "
                        + version);
            }

        }

        public void writeExternal(final ObjectOutput out) throws IOException {

            super.writeExternal(out);
            
            out.writeByte(VERSION);
            
        }

    } // CommitRecordIndexTupleSerializer

//	public Iterator<ICommitRecord> getCommitRecords(final long fromTime, final long toTime) {
//		return new Iterator<ICommitRecord>() {
//			ICommitRecord m_next = findNext(fromTime);
//			
//			public boolean hasNext() {
//				return m_next != null;
//			}
//
//			public ICommitRecord next() {
//				if (m_next == null) {
//					throw new NoSuchElementException();
//				}
//				
//				ICommitRecord ret = m_next;
//				m_next = findNext(ret.getTimestamp());
//				
//				if (m_next != null && m_next.getTimestamp() > toTime) {
//					m_next = null;
//				}
//				
//				return ret;
//			}
//
//			public void remove() {
//				throw new RuntimeException("Invalid Operation");
//			}
//			
//		};
//	}

}
