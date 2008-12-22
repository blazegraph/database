package com.bigdata.service;

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
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;

/**
 * {@link BTree} whose keys are commit times. No values are stored in the
 * {@link BTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CommitTimeIndex extends BTree {

    /**
     * Instance used to encode the timestamp into the key.
     */
    final private IKeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG);

    /**
     * Create a transient instance.
     * 
     * @return The new instance.
     */
    static public CommitTimeIndex createTransient() {

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        metadata.setBTreeClassName(CommitTimeIndex.class.getName());

        metadata.setTupleSerializer(new TupleSerializer(
                new ASCIIKeyBuilderFactory(Bytes.SIZEOF_LONG)));

        return (CommitTimeIndex) BTree
                .createTransient(/* store, */metadata);
        
    }

    /**
     * Load from the store.
     * 
     * @param store
     *            The backing store.
     * @param checkpoint
     *            The {@link Checkpoint} record.
     * @param metadata
     *            The metadata record for the index.
     */
    public CommitTimeIndex(final IRawStore store, final Checkpoint checkpoint,
            final IndexMetadata metadata) {

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
    protected byte[] encodeKey(final long commitTime) {

        return keyBuilder.reset().append(commitTime).getKey();

    }

    protected long decodeKey(final byte[] key) {

        return KeyBuilder.decodeLong(key, 0);

    }
    
    /**
     * Return the largest commitTime that is less than or equal to the given
     * timestamp. This is used primarily to locate the commit point that
     * will serve as the ground state for a transaction having <i>timestamp</i>
     * as its start time. In this context the LTE search identifies the most
     * recent commit point that not later than the start time of the
     * transaction.
     * 
     * @param timestamp
     *            The given timestamp.
     * 
     * @return The timestamp -or- <code>-1L</code> iff there is no entry
     *         in the index which satisifies the probe.
     * 
     * @throws IllegalArgumentException
     *             if <i>timestamp</i> is less than or equals to ZERO (0L).
     */
    synchronized public long find(final long timestamp) {

        if (timestamp <= 0L)
            throw new IllegalArgumentException();
        
        // find (first less than or equal to).
        final int index = findIndexOf(timestamp);
        
        if(index == -1) {
            
            // No match.
            
            return -1L;
            
        }

        return decodeKey(keyAt(index));
        
    }

    /**
     * Find the first commit time strictly greater than the timestamp.
     * 
     * @param timestamp
     *            The timestamp. A value of ZERO (0) may be used to find the
     *            first commit time.
     * 
     * @return The commit time -or- <code>-1L</code> if there is no commit
     *         record whose timestamp is strictly greater than <i>timestamp</i>.
     */
    synchronized public long findNext(final long timestamp) {

        /*
         * Note: can also be written using rangeIterator().next().
         */
        
        if (timestamp < 0L)
            throw new IllegalArgumentException();
        
        // find first strictly greater than.
        final int index = findIndexOf(Math.abs(timestamp)) + 1;
        
        if (index == nentries) {

            // No match.

            return -1L;
            
        }
        
        return decodeKey(valueAt(index));

    }

    /**
     * Find the index having the largest timestamp that is less than or
     * equal to the given timestamp.
     * 
     * @return The index having the largest timestamp that is less than or
     *         equal to the given timestamp -or- <code>-1</code> iff there
     *         are no index entries.
     */
    synchronized public int findIndexOf(final long timestamp) {
        
        int pos = super.indexOf(encodeKey(timestamp));
        
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
     * Add an entry for the commitTime.
     * 
     * @param commitTime
     *            A timestamp representing a commit time.
     * 
     * @exception IllegalArgumentException
     *                if <i>commitTime</i> is <code>0L</code>.
     * @exception IllegalArgumentException
     *                if there is already an entry registered under for the
     *                given timestamp.
     */
    public void add(final long commitTime) {

        if (commitTime == 0L)
            throw new IllegalArgumentException();
        
        final byte[] key = encodeKey(commitTime);
        
        if(super.contains(key)) {
            
            throw new IllegalArgumentException("entry exists: timestamp="
                    + commitTime);
            
        }
        
        super.insert(key, null);
        
    }
    
    /**
     * Encapsulates key and value formation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class TupleSerializer extends
            DefaultTupleSerializer<Long, Void> {

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
        public Long deserializeKey(ITuple tuple) {

            final byte[] key = tuple.getKeyBuffer().array();

            final long id = KeyBuilder.decodeLong(key, 0);

            return id;

        }

    }

}