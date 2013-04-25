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
package com.bigdata.journal.jini.ha;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.DelegateIndex;
import com.bigdata.btree.ILinearList;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.Tuple;
import com.bigdata.btree.UnisolatedReadWriteIndex;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;
import com.bigdata.rawstore.Bytes;
import com.bigdata.util.ChecksumUtility;

/**
 * {@link BTree} mapping <em>commitTime</em> (long integers) to
 * {@link ISnapshotRecord} records.
 * <p>
 * This object is thread-safe for concurrent readers and writers.
 * <p>
 * Note: This is used as a transient data structure that is populated from the
 * file system by the {@link HAJournalServer}. 
 */
public class SnapshotIndex extends DelegateIndex implements ILinearList {

    /**
     * The underlying index. Access to this is NOT thread safe unless you take
     * the appropriate lock on the {@link #readWriteLock}.
     */
    private final BTree btree;

    /**
     * The {@link ReadWriteLock} used by the {@link UnisolatedReadWriteIndex} to
     * make operations on the underlying {@link #btree} thread-safe.
     */
    private final ReadWriteLock readWriteLock;
    
    @SuppressWarnings("unchecked")
    private Tuple<ISnapshotRecord> getLookupTuple() {
        
        return btree.getLookupTuple();
        
    }
    
//    /**
//     * Instance used to encode the timestamp into the key.
//     */
//    final private IKeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG);

    /**
     * Create a transient instance.
     * 
     * @return The new instance.
     */
    static public SnapshotIndex createTransient() {
    
        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
//        metadata.setBTreeClassName(SnapshotIndex.class.getName());

        metadata.setTupleSerializer(new TupleSerializer(
                new ASCIIKeyBuilderFactory(Bytes.SIZEOF_LONG)));

        final BTree ndx = BTree.createTransient(/*store, */metadata);
        
        return new SnapshotIndex(ndx);
        
    }

    private SnapshotIndex(final BTree ndx) {

        // Wrap B+Tree for read/write thread safety.
        super(new UnisolatedReadWriteIndex(ndx));
        
        this.btree = ndx;
        
//        this.delegate = new UnisolatedReadWriteIndex(ndx);
        
        // Save reference to lock for extended synchronization patterns.
        this.readWriteLock = UnisolatedReadWriteIndex.getReadWriteLock(ndx);
        
    }
    
//    /**
//     * Load from the store.
//     * 
//     * @param store
//     *            The backing store.
//     * @param checkpoint
//     *            The {@link Checkpoint} record.
//     * @param metadata
//     *            The metadata record for the index.
//     */
//    public SnapshotIndex(final IRawStore store, final Checkpoint checkpoint,
//            final IndexMetadata metadata, final boolean readOnly) {
//
//        super(store, checkpoint, metadata, readOnly);
//
//    }
    
    /**
     * Encodes the commit time into a key.
     * 
     * @param commitTime
     *            The commit time.
     * 
     * @return The corresponding key.
     */
    public byte[] getKey(final long commitTime) {

        return getIndexMetadata().getKeyBuilder().reset().append(commitTime)
                .getKey();

    }

    Lock readLock() {
        return readWriteLock.readLock();
    }
    
    Lock writeLock() {
        return readWriteLock.writeLock();
    }

    public long getEntryCount() {

        return super.rangeCount();
        
    }
    
    @Override
    public long indexOf(final byte[] key) {
        final Lock lock = readLock();
        lock.lock();
        try {
            return btree.indexOf(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] keyAt(final long index) {
        final Lock lock = readLock();
        lock.lock();
        try {
            return btree.keyAt(index);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] valueAt(final long index) {
        final Lock lock = readLock();
        lock.lock();
        try {
            return btree.valueAt(index);
        } finally {
            lock.unlock();
        }
    }

//    @Override
    @SuppressWarnings("unchecked")
    public Tuple<ISnapshotRecord> valueAt(final long index,
            final Tuple<ISnapshotRecord> t) {
        final Lock lock = readLock();
        lock.lock();
        try {
            return btree.valueAt(index, t);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Return the {@link IRootBlock} identifying the journal having the largest
     * commitTime that is less than or equal to the given timestamp. This is
     * used primarily to locate the commit record that will serve as the ground
     * state for a transaction having <i>timestamp</i> as its start time. In
     * this context the LTE search identifies the most recent commit state that
     * not later than the start time of the transaction.
     * 
     * @param timestamp
     *            The given timestamp.
     * 
     * @return The description of the relevant journal resource -or-
     *         <code>null</code> iff there are no journals in the index that
     *         satisify the probe.
     * 
     * @throws IllegalArgumentException
     *             if <i>timestamp</i> is less than ZERO (0L).
     */
     public ISnapshotRecord find(final long timestamp) {

        if (timestamp < 0L)
            throw new IllegalArgumentException();

        final Lock lock = readLock();

        lock.lock();
        
        try {

            // find (first less than or equal to).
            final long index = findIndexOf(timestamp);

            if (index == -1) {

                // No match.
                return null;

            }

            return valueAtIndex(index);

        } finally {

            lock.unlock();

        }
        
    }

    /**
     * Retrieve the entry from the index.
     */
    private ISnapshotRecord valueAtIndex(final long index) {

        return (ISnapshotRecord) valueAt(index, getLookupTuple()).getObject();

//        final byte[] val = super.valueAt(index);
//
//        assert val != null : "Entry has null value: index=" + index;
//        
//        final IRootBlockView entry = new RootBlockView(false/* rootBlock0 */,
//                ByteBuffer.wrap(val), ChecksumUtility.getCHK());
//
//        return entry;

    }
    
    /**
     * Return the {@link ISnapshotRecord} identifying the first snapshot whose
     * <em>commitTime</em> is strictly greater than the timestamp.
     * 
     * @param timestamp
     *            The timestamp. A value of ZERO (0) may be used to find the
     *            first snapshot.
     * 
     * @return The root block of that snapshot -or- <code>null</code> if there
     *         is no snapshot whose timestamp is strictly greater than
     *         <i>timestamp</i>.
     *         
     * @throws IllegalArgumentException
     *             if <i>timestamp</i> is less than ZERO (0L).
     */
    public ISnapshotRecord findNext(final long timestamp) {

        if (timestamp < 0L)
            throw new IllegalArgumentException();

        final Lock lock = readLock();

        lock.lock();

        try {

            // find first strictly greater than.
            final long index = findIndexOf(timestamp) + 1;

            if (index == rangeCount()) {

                // No match.

                return null;

            }

            return valueAtIndex(index);

        } finally {

            lock.unlock();

        }

    }

    /**
     * Find the index of the {@link ISnapshotRecord} associated with the largest
     * commitTime that is less than or equal to the given timestamp.
     * 
     * @param commitTime
     *            The timestamp.
     * 
     * @return The index of the {@link ISnapshotRecord} associated with the
     *         largest commitTime that is less than or equal to the given
     *         timestamp -or- <code>-1</code> iff the index is empty.
     * 
     * @throws IllegalArgumentException
     *             if <i>timestamp</i> is less than ZERO (0L).
     */
    public long findIndexOf(final long commitTime) {
        
        if (commitTime < 0L)
            throw new IllegalArgumentException();

        /*
         * Note: Since this is the sole index access, we don't need to take the
         * lock to coordinate a consistent view of the index in this method.
         */
        long pos = indexOf(getKey(commitTime));
        
        if (pos < 0) {

            /*
             * the key lies between the entries in the index, or possible before
             * the first entry in the index. [pos] represents the insert
             * position. we convert it to an entry index and subtract one to get
             * the index of the first commit record less than the given
             * timestamp.
             */
            
            pos = -(pos+1);

            if (pos == 0) {

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
     * Add an entry under the commitTime associated with the
     * {@link ISnapshotRecord} record.
     * 
     * @param snapshotRecord
     *            The {@link ISnapshotRecord} record.
     * 
     * @exception IllegalArgumentException
     *                if <i>commitTime</i> is <code>0L</code>.
     * @exception IllegalArgumentException
     *                if <i>rootBLock</i> is <code>null</code>.
     * @exception IllegalArgumentException
     *                if there is already an entry registered under for the
     *                given timestamp.
     */
    public void add(final ISnapshotRecord snapshotRecord) {

        if (snapshotRecord == null)
            throw new IllegalArgumentException();

        final long commitTime = snapshotRecord.getRootBlock()
                .getLastCommitTime();

        if (commitTime == 0L)
            throw new IllegalArgumentException();

        final Lock lock = writeLock();

        lock.lock();

        try {

            final byte[] key = getKey(commitTime);

            if (super.contains(key)) {

                throw new IllegalArgumentException("entry exists: timestamp="
                        + commitTime);

            }

            // add a serialized entry to the persistent index.
            super.insert(key, snapshotRecord);

        } finally {

            lock.unlock();

        }

    }
   
    /**
     * Find and return the {@link ISnapshotRecord} for the oldest snapshot (if
     * any).
     * 
     * @return That {@link ISnapshotRecord} -or- <code>null</code> if there are
     *         no snapshots.
     */
    public ISnapshotRecord getOldestSnapshot() {

        final Lock lock = readLock();

        lock.lock();
        
        try {

            if (rangeCount() == 0L) {

                // Empty index.
                return null;

            }

            // Lookup first tuple in index.
            final ITuple<ISnapshotRecord> t = valueAt(0L, getLookupTuple());

            final ISnapshotRecord r = t.getObject();

            return r;

        } finally {

            lock.unlock();
            
        }

    }
    
    /**
     * Find the {@link ISnapshotRecord} for the most recent snapshot (if any).
     * 
     * @return That {@link ISnapshotRecord} -or- <code>null</code> if there are
     *         no snapshots.
     */
    public ISnapshotRecord getNewestSnapshot() {
        
        final Lock lock = readLock();

        lock.lock();

        try {

            final long entryCount = getEntryCount();

            if (entryCount == 0L)
                return null;

            return valueAt(entryCount - 1, getLookupTuple()).getObject();

        } finally {
            
            lock.unlock();
            
        }
        
//            @SuppressWarnings("unchecked")
//            final ITupleIterator<ISnapshotRecord> itr = rangeIterator(
//                    null/* fromKey */, null/* toKey */, 1/* capacity */,
//                    IRangeQuery.DEFAULT | IRangeQuery.REVERSE/* flags */, null/* filter */);
//
//            if (!itr.hasNext()) {
//
//                return null;
//
//            }
//
//            final ITuple<ISnapshotRecord> t = itr.next();
//
//            final ISnapshotRecord snapshotRecord = t.getObject();
//
//            return snapshotRecord;
        
    }
    
    /**
     * Find the oldest snapshot whose commit counter is LTE the specified commit
     * counter.
     * 
     * @return The {@link ISnapshotRecord} for that snapshot -or-
     *         <code>null</code> if there is no such snapshot.
     * 
     * @throws IllegalArgumentException
     *             if <code>commitCounter LT ZERO (0)</code>
     */
    public ISnapshotRecord findByCommitCounter(final long commitCounter) {

        if (commitCounter < 0L)
            throw new IllegalArgumentException();

        final Lock lock = readLock();

        lock.lock();
        
        try {

            // Reverse scan.
            @SuppressWarnings("unchecked")
            final ITupleIterator<ISnapshotRecord> itr = rangeIterator(
                    null/* fromKey */, null/* toKey */, 0/* capacity */,
                    IRangeQuery.DEFAULT | IRangeQuery.REVERSE/* flags */, null/* filter */);

            while (itr.hasNext()) {

                final ITuple<ISnapshotRecord> t = itr.next();

                final ISnapshotRecord r = t.getObject();

                final IRootBlockView rb = r.getRootBlock();

                if (rb.getCommitCounter() <= commitCounter) {

                    // First snapshot LTE that commit counter.
                    return r;

                }

            }

            return null;

        } finally {

            lock.unlock();
            
        }
       
    }
    
    /**
     * Return the snapshot that is associated with the specified ordinal index
     * (origin ZERO) counting backwards from the most recent snapshot (0)
     * towards the earliest snapshot (nsnapshots-1).
     * <p>
     * Note: The effective index is given by <code>(entryCount-1)-index</code>.
     * If the effective index is LT ZERO (0) then there is no such snapshot and
     * this method will return <code>null</code>.
     * 
     * @param index
     *            The index.
     * 
     * @return The {@link ISnapshotRecord} for that snapshot -or-
     *         <code>null</code> if there is no such snapshot.
     * 
     * @throws IllegalArgumentException
     *             if <code>index LT ZERO (0)</code>
     */
    public ISnapshotRecord getSnapshotByReverseIndex(final int index) {

        if (index < 0)
            throw new IllegalArgumentException();
        
        final Lock lock = readLock();

        lock.lock();
        
        try {

            final long entryCount = rangeCount();
            
            if (entryCount > Integer.MAX_VALUE)
                throw new AssertionError();

            final int effectiveIndex = ((int) entryCount - 1) - index;
            
            if (effectiveIndex < 0) {

                // No such snapshot.
                return null;

            }

            final ITuple<ISnapshotRecord> t = valueAt(effectiveIndex,
                    getLookupTuple());

            final ISnapshotRecord r = t.getObject();
            
            return r;

        } finally {
            
            lock.unlock();
            
        }

    }

    /**
     * Interface for access to the snapshot metadata.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static interface ISnapshotRecord {
    
        /**
         * Return the bytes on the disk for the snapshot file.
         */
        public long sizeOnDisk();
        
        /**
         * Return the {@link IRootBlockView} of the snapshot.
         * @return
         */
        public IRootBlockView getRootBlock();
        
    }
    
    public static class SnapshotRecord implements ISnapshotRecord,
            Externalizable {

        private static final int VERSION0 = 0x0;

        private static final int currentVersion = VERSION0;
        
        /**
         * Note: This is NOT {@link Serializable}.
         */
        private IRootBlockView rootBlock;

        private long sizeOnDisk;

        /**
         * De-serialization constructor.
         */
        public SnapshotRecord() {
        }
        
        public SnapshotRecord(final IRootBlockView rootBlock,
                final long sizeOnDisk) {

            if (rootBlock == null)
                throw new IllegalArgumentException();

            if (sizeOnDisk < 0L)
                throw new IllegalArgumentException();

            this.rootBlock = rootBlock;

            this.sizeOnDisk = sizeOnDisk;

        }
        
        @Override
        public long sizeOnDisk() {
            return sizeOnDisk;
        }

        @Override
        public IRootBlockView getRootBlock() {
            return rootBlock;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof ISnapshotRecord))
                return false;
            final ISnapshotRecord t = (ISnapshotRecord) o;
            if (sizeOnDisk() != t.sizeOnDisk())
                return false;
            if (!getRootBlock().equals(t.getRootBlock()))
                return false;
            return true;
        }

        @Override
        public int hashCode() {
            return getRootBlock().hashCode();
        }

        @Override
        public void writeExternal(final ObjectOutput out) throws IOException {

            out.writeInt(currentVersion);

            final byte[] a = BytesUtil.getBytes(rootBlock.asReadOnlyBuffer());

            final int sizeOfRootBlock = a.length;

            out.writeInt(sizeOfRootBlock);

            out.write(a, 0, sizeOfRootBlock);

            out.writeLong(sizeOnDisk);

        }

        @Override
        public void readExternal(final ObjectInput in) throws IOException,
                ClassNotFoundException {

            final int version = in.readInt();

            switch (version) {
            case VERSION0:
                break;
            default:
                throw new IOException("Unknown version: " + version);
            }

            final int sizeOfRootBlock = in.readInt();

            final byte[] a = new byte[sizeOfRootBlock];

            in.readFully(a, 0, sizeOfRootBlock);
            
            rootBlock = new RootBlockView(false/* rootBlock0 */,
                    ByteBuffer.wrap(a), ChecksumUtility.getCHK());

            sizeOnDisk = in.readLong();
            
        }
        
    } // SnapshotRecord
    
    /**
     * Encapsulates key and value formation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    static protected class TupleSerializer extends
            DefaultTupleSerializer<Long, ISnapshotRecord> {

        /**
         * 
         */
        private static final long serialVersionUID = -2851852959439807542L;

        /**
         * De-serialization ctor.
         */
        public TupleSerializer() {

            super();
            
        }

        /**
         * Ctor when creating a new instance.
         * 
         * @param keyBuilderFactory
         */
        public TupleSerializer(final IKeyBuilderFactory keyBuilderFactory) {

            super(keyBuilderFactory);

        }
        
        /**
         * Decodes the key as a commit time.
         */
        @Override
        @SuppressWarnings("rawtypes") 
        public Long deserializeKey(final ITuple tuple) {

            return KeyBuilder
                    .decodeLong(tuple.getKeyBuffer().array(), 0/* offset */);

        }

//        /**
//         * De-serializes an object from the {@link ITuple#getValue() value} stored
//         * in the tuple (ignores the key stored in the tuple).
//         */
//        public ISnapshotRecord deserialize(final ITuple tuple) {
//
//            if (tuple == null)
//                throw new IllegalArgumentException();
//
//            return (IRootBlockView) new RootBlockView(false/* rootBlock0 */,
//                    ByteBuffer.wrap(tuple.getValue()), ChecksumUtility.getCHK());
//            
//        }

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

    }

}
