package com.bigdata.journal;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.objndx.BTree;
import com.bigdata.objndx.BTreeMetadata;
import com.bigdata.objndx.IValueSerializer;
import com.bigdata.objndx.KeyBuilder;
import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.IRawStore;

/**
 * BTree mapping commit times to {@link ICommitRecord}s. The keys are the long
 * integers. The values are {@link Entry} objects recording the commit time of
 * the index and the {@link Addr address} of the {@link ICommitRecord} for that
 * commit time.
 */
public class CommitRecordIndex extends BTree {

    /**
     * @todo refactor to share with the {@link Journal}?
     */
    private KeyBuilder keyBuilder = new KeyBuilder();

//    /**
//     * Cache of added/retrieved commit records.
//     * 
//     * @todo This only works for exact timestamp matches so the cache might not
//     * be very useful here.  Also, this must be a weak value cache or it will
//     * leak memory.
//     */
//    private Map<Long, ICommitRecord> cache = new HashMap<Long, ICommitRecord>();

    public CommitRecordIndex(IRawStore store) {

        super(store, DEFAULT_BRANCHING_FACTOR, ValueSerializer.INSTANCE);

    }

    /**
     * Load from the store.
     * 
     * @param store
     *            The backing store.
     * @param metadataId
     *            The metadata record for the index.
     */
    public CommitRecordIndex(IRawStore store, BTreeMetadata metadata) {

        super(store, metadata);

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
         * Note: The {@link KeyBuilder} is NOT thread-safe
         */
        return keyBuilder.reset().append(commitTime).getKey();

    }

    /**
     * Existence test for a commit record with the specified commit timestamp.
     * 
     * @param commitTime
     *            The commit timestamp.
     * @return true iff such an {@link ICommitRecord} exists in the index with
     *         that commit timestamp.
     */
    synchronized public boolean hasTimestamp(long commitTime) {
        
        return super.contains(getKey(commitTime));
        
    }
    
    /**
     * Return the {@link ICommitRecord} with the given timestamp (exact match).
     * This method tests a cache of the named btrees and will return the same
     * instance if the index is found in the cache.
     * 
     * @param commitTime
     *            The commit time.
     * 
     * @return The {@link ICommitRecord} index or <code>null</code> iff there
     *         is no {@link ICommitTimestamp} for that commit time.
     */
    synchronized public ICommitRecord get(long commitTime) {

        ICommitRecord commitRecord;
        
//        commitRecord = cache.get(commitTime);
//        
//        if (commitRecord != null)
//            return commitRecord;

        final Entry entry = (Entry) super.lookup(getKey(commitTime));

        if (entry == null) {

            return null;
            
        }

        /*
         * re-load the commit record from the store.
         */
        commitRecord = loadCommitRecord(store,entry.addr);
        
//        /*
//         * save commit time -> commit record mapping in transient cache (this
//         * only works for for exact matches on the timestamp so the cache may
//         * not be very useful here).
//         */
//        cache.put(commitRecord.getTimestamp(),commitRecord);

        // return btree.
        return commitRecord;

    }

    /**
     * Return the {@link ICommitRecord} having the largest timestamp that is
     * strictly less than the given timestamp.
     * 
     * @param timestamp
     *            The given timestamp.
     * 
     * @return The commit record -or- <code>null</code> iff there are no
     *         {@link ICommitRecord}s in the that satisify the probe.
     * 
     * @see #get(long)
     */
    synchronized public ICommitRecord find(long timestamp) {

        final int index = findIndexOf(timestamp);
        
        if(index == -1) {
            
            // No match.
            
            return null;
            
        }

        // return the matched record.
        
        Entry entry = (Entry) super.valueAt( index );
        
        return loadCommitRecord(store,entry.addr);

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
     * Add an entry for a commit record..
     * 
     * @param commitRecord
     *            The commit record.
     * 
     * @param commitRecordAddr
     *            The address at which that commit record was written on the
     *            store.
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
        
        // add an entry to the persistent index.
        super.insert(key,new Entry(commitTime,commitRecordAddr));
        
//        // add to the transient cache.
//        cache.put(commitTime, commitRecord);
        
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
         * The {@link Addr address} of the {@link ICommitRecord} whose commit
         * timestamp is {@link #commitTime}.
         */
        public final long addr;
        
        public Entry(long commitTime,long addr) {
            
            this.commitTime = commitTime;
            
            this.addr = addr;
            
        }
        
    }
    
    /**
     * The values are {@link Entry}s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ValueSerializer implements IValueSerializer {

        /**
         * 
         */
        private static final long serialVersionUID = 8085229450005958541L;

        public static transient final IValueSerializer INSTANCE = new ValueSerializer();

        final public static transient int VERSION0 = 0x0;

        public ValueSerializer() {
        }

        public void putValues(DataOutputStream os, Object[] values, int n)
                throws IOException {

            os.writeInt(VERSION0);
            
            for (int i = 0; i < n; i++) {

                Entry entry = (Entry) values[i];

                LongPacker.packLong(os, entry.commitTime);
                LongPacker.packLong(os, entry.addr);
                
            }

        }
        
        public void getValues(DataInputStream is, Object[] values, int n)
                throws IOException {

            final int version = is.readInt();
            
            if (version != VERSION0)
                throw new RuntimeException("Unknown version: " + version);
            
            for (int i = 0; i < n; i++) {

                final long commitTime = Long.valueOf(LongPacker.unpackLong(is));

                final long addr = Long.valueOf(LongPacker.unpackLong(is));
                
                values[i] = new Entry(commitTime,addr);

            }

        }

    }

}
