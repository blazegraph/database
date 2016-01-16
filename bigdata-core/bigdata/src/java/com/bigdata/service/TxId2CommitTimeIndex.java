/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.service;

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
import com.bigdata.rawstore.IRawStore;
import com.bigdata.service.AbstractTransactionService.TxState;
import com.bigdata.util.Bytes;

/**
 * {@link BTree} whose keys are the absolute value of the txIds and whose values
 * are {@link ITxState0} tuples for the transaction.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TxId2CommitTimeIndex.java 5948 2012-02-02 20:16:05Z thompsonbry
 *          $
 */
public class TxId2CommitTimeIndex extends BTree {

    /**
     * Instance used to encode the timestamp into the key.
     */
    final private IKeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG);

    /**
     * Create a transient instance.
     * 
     * @return The new instance.
     */
    static public TxId2CommitTimeIndex createTransient() {

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        metadata.setBTreeClassName(TxId2CommitTimeIndex.class.getName());

        metadata.setTupleSerializer(new TupleSerializer(
                new ASCIIKeyBuilderFactory(Bytes.SIZEOF_LONG * 2)));

        return (TxId2CommitTimeIndex) BTree.createTransient(/* store, */metadata);

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
    public TxId2CommitTimeIndex(final IRawStore store, final Checkpoint checkpoint,
            final IndexMetadata metadata, boolean readOnly) {

        super(store, checkpoint, metadata, readOnly);

    }
    
    /**
     * Encodes the txId into a key.
     * 
     * @param txId
     *            The transaction start time (may be negative).
     * 
     * @return The corresponding key.
     */
    private byte[] encodeKey(final long startTime) {

        // Note: absolute value of the start time!
        final long tmp = Math.abs(startTime);

        return keyBuilder.reset().append(tmp).getKey();

    }

    final static private long decodeKey(final byte[] key) {

        return KeyBuilder.decodeLong(key, 0);

    }

    private static class MyTxState implements ITxState0 {

        private final long txId;
        private final long readsOnCommitTime;
        
        private MyTxState(final long txId, final long readsOnCommitTime) {
            
            this.txId = txId;
            
            
            this.readsOnCommitTime = readsOnCommitTime;
            
        }
        
        @Override
        final public long getStartTimestamp() {

            return txId;
            
        }

        @Override
        final public long getReadsOnCommitTime() {

            return readsOnCommitTime;
            
        }
        
    }
    
    /**
     * Return the largest key that is less than or equal to the given timestamp.
     * This is used primarily to locate the commit point that will serve as the
     * ground state for a transaction having <i>timestamp</i> as its start time.
     * In this context the LTE search identifies the most recent commit point
     * that not later than the start time of the transaction.
     * 
     * @param timestamp
     *            The given timestamp.
     * 
     * @return The timestamp -or- <code>-1L</code> iff there is no entry in the
     *         index which satisifies the probe.
     * 
     * @throws IllegalArgumentException
     *             if <i>timestamp</i> is less than or equals to ZERO (0L).
     */
    synchronized public long find(final long timestamp) {

        if (timestamp <= 0L)
            throw new IllegalArgumentException();
        
        // find (first less than or equal to).
        final long index = findIndexOf(timestamp);
        
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
        final long index = findIndexOf(Math.abs(timestamp)) + 1;
        
        if (index == nentries) {

            // No match.

            return -1L;
            
        }
        
        return decodeKey(keyAt(index));

    }

    /**
     * Find the index having the largest timestamp that is less than or
     * equal to the given timestamp.
     * 
     * @return The index having the largest timestamp that is less than or
     *         equal to the given timestamp -or- <code>-1</code> iff there
     *         are no index entries.
     */
    synchronized public long findIndexOf(final long timestamp) {
        
        long pos = super.indexOf(encodeKey(timestamp));
        
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
     * Add an entry.
     * 
     * @param txState
     *            The transaction object.
     * 
     * @exception IllegalArgumentException
     *                if <i>txState</i> is <code>null</code>.
     */
//    * @exception IllegalArgumentException
//    *                if there is already an entry registered under for the
//    *                given timestamp.
    public void add(final TxState txState) {

//        if (txId == 0L)
//            throw new IllegalArgumentException();

        if (txState == null)
            throw new IllegalArgumentException();

        if (txState.tx == 0L)
            throw new IllegalArgumentException();

        if (txState.getReadsOnCommitTime() < 0L)
            throw new IllegalArgumentException();

        final TupleSerializer tupleSer = (TupleSerializer) getIndexMetadata()
                .getTupleSerializer();
        
        final byte[] key = tupleSer.serializeKey(txState);

        if (super.contains(key)) {

            throw new IllegalArgumentException("entry exists: key=" + key
                    + ", newValue=" + txState);

        }

        final byte[] val = tupleSer.serializeVal(txState);

        super.insert(key, val);
        
    }
    
    /**
     * Encapsulates key and value formation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    static protected class TupleSerializer extends
            DefaultTupleSerializer<Long, ITxState0> {

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
        
        @Override
        public byte[] serializeKey(final Object obj) {

            final long txId;

            if (obj instanceof ITxState0) {

                txId = ((ITxState) obj).getStartTimestamp();

            } else if (obj instanceof Long) {

                txId = ((Long) obj).longValue();

            } else {

                throw new IllegalArgumentException("class=" + obj.getClass());
                
            }

            // Note: absolute value of the start time!
            final long tmp = Math.abs(txId);

            return getKeyBuilder().reset().append(tmp).getKey();

        }
        
        /**
         * Decodes the key as a transaction identifier.
         */
        @Override
        @SuppressWarnings("rawtypes")
        public Long deserializeKey(final ITuple tuple) {

            final byte[] key = tuple.getKeyBuffer().array();

            final long id = KeyBuilder.decodeLong(key, 0);

            return id;

        }

        /**
         * Decodes the value as a commit time.
         */
        @SuppressWarnings("rawtypes")
        public ITxState0 deserialize(final ITuple tuple) {

            final byte[] val = tuple.getValueBuffer().array();

            final long txId = KeyBuilder.decodeLong(val, 0/*off*/);

            final long readsOnCommitTime = KeyBuilder.decodeLong(val,
                    Bytes.SIZEOF_LONG/* off */);

            return new MyTxState(txId,readsOnCommitTime);
            
        }
        
        @Override
        public byte[] serializeVal(final ITxState0 val) {

            return getKeyBuilder().reset().append(val.getStartTimestamp())
                    .append(val.getReadsOnCommitTime()).getKey();

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

    }

}
