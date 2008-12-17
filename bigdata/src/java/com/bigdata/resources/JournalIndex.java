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
package com.bigdata.resources;

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
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.IJournal;
import com.bigdata.mdi.JournalMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;

/**
 * {@link BTree} mapping {@link IJournal} <em>createTimes</em> (long integers)
 * to {@link JournalMetadata} records describing the {@link IJournal}.
 * <p>
 * Note: Access to this object MUST be synchronized.
 * <p>
 * Note: This is used as a transient data structure that is populated from the
 * file system by the {@link ResourceManager}.
 */
public class JournalIndex extends BTree {

    /**
     * Instance used to encode the timestamp into the key.
     */
    final private IKeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG);

    /**
     * Create a transient instance.
     * 
     * @return The new instance.
     */
    static public JournalIndex createTransient() {
    
        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        metadata.setBTreeClassName(JournalIndex.class.getName());

        metadata.setTupleSerializer(new TupleSerializer(
                new ASCIIKeyBuilderFactory(Bytes.SIZEOF_LONG)));

        return (JournalIndex) BTree.createTransient(/*store, */metadata);
        
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
    public JournalIndex(final IRawStore store, final Checkpoint checkpoint,
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
    protected byte[] getKey(final long commitTime) {

//        return metadata.getTupleSerializer().serializeKey(commitTime);

        return keyBuilder.reset().append(commitTime).getKey();

    }

    /**
     * Return the {@link JournalMetadata} identifying the journal having the
     * largest createTime that is less than or equal to the given timestamp.
     * This is used primarily to locate the commit record that will serve as the
     * ground state for a transaction having <i>timestamp</i> as its start
     * time. In this context the LTE search identifies the most recent commit
     * state that not later than the start time of the transaction.
     * 
     * @param timestamp
     *            The given timestamp.
     * 
     * @return The description of the relevant journal resource -or-
     *         <code>null</code> iff there are no journals in the index that
     *         satisify the probe.
     * 
     * @throws IllegalArgumentException
     *             if <i>timestamp</i> is less than or equals to ZERO (0L).
     */
    synchronized public JournalMetadata find(final long timestamp) {

        if (timestamp <= 0L)
            throw new IllegalArgumentException();
        
        // find (first less than or equal to).
        final int index = findIndexOf(timestamp);
        
        if(index == -1) {
            
            // No match.
            
            return null;
            
        }

        return valueAtIndex(index);
        
    }

    /**
     * Retrieve the entry from the index.
     */
    private JournalMetadata valueAtIndex(final int index) {

        final JournalMetadata entry = (JournalMetadata) SerializerUtil
                .deserialize(super.valueAt(index));

        return entry;

    }
    
    /**
     * Find the first journal whose <em>createTime</em> is strictly greater
     * than the timestamp.
     * 
     * @param timestamp
     *            The timestamp. A value of ZERO (0) may be used to find the
     *            first journal.
     * 
     * @return The commit record -or- <code>null</code> if there is no commit
     *         record whose timestamp is strictly greater than <i>timestamp</i>.
     */
    synchronized public JournalMetadata findNext(final long timestamp) {

        /*
         * Note: can also be written using rangeIterator().next().
         */
        
        if (timestamp < 0L)
            throw new IllegalArgumentException();
        
        // find first strictly greater than.
        final int index = findIndexOf(Math.abs(timestamp)) + 1;
        
        if (index == nentries) {

            // No match.

            return null;
            
        }
        
        return valueAtIndex(index);

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
    synchronized public int findIndexOf(final long timestamp) {
        
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
     * Add an entry under the commitTime associated with the
     * {@link JournalMetadata} record.
     * 
     * @param resourceMetadata
     *            The {@link JournalMetadata} record.
     * 
     * @exception IllegalArgumentException
     *                if <i>commitTime</i> is <code>0L</code>.
     * @exception IllegalArgumentException
     *                if <i>resourceMetadata</i> is <code>null</code>.
     * @exception IllegalArgumentException
     *                if there is already an entry registered under for the
     *                given timestamp.
     */
    synchronized public void add(final JournalMetadata resourceMetadata) {

        if (resourceMetadata == null)
            throw new IllegalArgumentException();

        assert resourceMetadata.isJournal();
        
        final long createTime = resourceMetadata.getCreateTime();

        if (createTime == 0L)
            throw new IllegalArgumentException();

        final byte[] key = getKey(createTime);
        
        if(super.contains(key)) {
            
            throw new IllegalArgumentException("entry exists: timestamp="
                    + createTime);
            
        }
        
        // add a serialized entry to the persistent index.
        super.insert(key, SerializerUtil.serialize(resourceMetadata));
        
    }
    
    /**
     * Encapsulates key and value formation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class TupleSerializer extends
            DefaultTupleSerializer<Long, JournalMetadata> {

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
