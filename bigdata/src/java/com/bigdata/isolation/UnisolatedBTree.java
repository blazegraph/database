/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Feb 12, 2007
 */

package com.bigdata.isolation;

import com.bigdata.objndx.BTree;
import com.bigdata.objndx.BTreeMetadata;
import com.bigdata.objndx.BatchContains;
import com.bigdata.objndx.BatchInsert;
import com.bigdata.objndx.BatchLookup;
import com.bigdata.objndx.BatchRemove;
import com.bigdata.objndx.IBatchOp;
import com.bigdata.objndx.IEntryIterator;
import com.bigdata.objndx.ISimpleBTree;
import com.bigdata.rawstore.IRawStore;

/**
 * <p>
 * A scalable mutable B+-Tree mapping variable length unsigned byte[] keys to
 * byte[] values that is capable of being isolated by a transaction and supports
 * deletion markers. Application values are transparently encapsulated in
 * {@link IValue} object which keep track of version counters (in support of
 * transactions) and deletion markers (in support of both transactions and
 * partitioned indices). Users of this interface will only see application
 * values, not {@link IValue} objects.
 * </p>
 * <p>
 * Note that {@link #rangeCount(byte[], byte[])} MAY report more entries than
 * would actually be visited. This is because it depends on
 * {@link #indexOf(byte[])} and the internal counts of the #of spanned entries
 * for a node do not differentiate between entries which have and have not been
 * {@link IValue#isDeleted() deleted}.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo efficient sharing of nodes and leaves for concurrent read-only views
 *       (stealing children vs wrapping them with a flyweight wrapper; reuse of
 *       the same btree instance for reading from the same historical state).
 * 
 * @see IsolatedBTree, a {@link BTree} that has been isolated by a transaction.
 *
 * The following is an ad-hoc summary of the behavior of some methods exposed by
 * this class:
<pre>

contains() - done.
insert() - done.
remove() - done.
lookup() - done.

addAll() - ok as implemented.  values will be wrapped in {@link IValue} objects
as they are inserted.  if the source is also an {@link UnisolatedBTree} then the
application values will be inserted into this tree, not the {@link IValue} objects.

indexOf() - ok as implemented (counts deleted entries).
keyAt() - ok as implemented, but will return keys for deleted entries.
valueAt() - overriden to return null for a deleted entry.

rangeCount - ok as implemented (counts deleted entries).
rangeIterator - must filter out deleted entries.

entryIterator() - only non-deleted entries.

IBatchBTree - all methods are overriden to use {@link IBatchOp#apply(ISimpleBTree)}
so that they will correctly apply the semantics of the {@link UnisolatedBTree}.

</pre>
 */
public class UnisolatedBTree extends BTree implements IIsolatableIndex {

    protected final IConflictResolver conflictResolver;
    
    /**
     * The delegate that handles write-write conflict resolution during backward
     * validation. The conflict resolver is expected to make a best attempt
     * using data type specific rules to reconcile the state for two versions of
     * the same persistent identifier. If the conflict can not be resolved, then
     * validation will fail. State-based conflict resolution when combined with
     * validation (aka optimistic locking) is capable of validating the greatest
     * number of interleavings of transactions (aka serialization orders).
     * 
     * @return The conflict resolver to be applied during validation or
     *         <code>null</code> iff no conflict resolution will be performed.
     */
    public IConflictResolver getConflictResolver() {
            
        return conflictResolver;
    
    }

    /**
     * Create an isolated btree.
     * 
     * @param store
     * @param branchingFactor
     * @param conflictResolver
     *            An optional object that handles write-write conflict
     *            resolution during backward validation. The conflict resolver
     *            is expected to make a best attempt using data type specific
     *            rules to reconcile the state for two versions of the same
     *            persistent identifier. If the conflict can not be resolved,
     *            then validation will fail. State-based conflict resolution
     *            when combined with validation (aka optimistic locking) is
     *            capable of validating the greatest number of interleavings of
     *            transactions (aka serialization orders).
     */
    public UnisolatedBTree(IRawStore store, int branchingFactor, IConflictResolver conflictResolver ) {
       
        super(store, branchingFactor, Value.Serializer.INSTANCE );
        
        this.conflictResolver = conflictResolver;
        
    }

    /**
     * Re-load the isolated btree.
     * 
     * @param store
     * 
     * @param metadata
     */
    public UnisolatedBTree(IRawStore store, BTreeMetadata metadata) {
        
        super(store,metadata);
        
        this.conflictResolver = ((UnisolatedBTreeMetadata) metadata).conflictResolver;
        
    }

    public BTreeMetadata newMetadata() {
        
        return new UnisolatedBTreeMetadata(this);
        
    }
    
    /**
     * Extends {@link BTreeMetadata} to also store the {@link IConflictResolver}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class UnisolatedBTreeMetadata extends BTreeMetadata {

        private static final long serialVersionUID = -4938674944860230200L;

        public final IConflictResolver conflictResolver;
        
        /**
         * @param btree
         */
        protected UnisolatedBTreeMetadata(UnisolatedBTree btree) {
            
            super(btree);
            
            this.conflictResolver = btree.conflictResolver;
            
        }

    }
    
    /**
     * True iff the key does not exist or if it exists but is marked as
     * {@link IValue#isDeleted()}.
     * 
     * @param key
     *            The search key.
     * 
     * @return True iff there is an non-deleted entry for the search key.
     */
    public boolean contains(byte[] key) {
        
        if (key == null)
            throw new IllegalArgumentException();

        Value value = (Value)super.lookup(key);
        
        if(value==null||value.deleted) return false;
        
        return true;
        
    }

    /**
     * Return the {@link IValue#getValue()} associated with the key or
     * <code>null</code> if the key is not found or if the key was found by
     * the entry is flagged as {@link IValue#isDeleted()}.
     * 
     * @param key
     *            The search key.
     * 
     * @return The application value stored under that search key (may be null)
     *         or null if the key was not found or if they entry was marked as
     *         deleted.
     */
    public Object lookup(Object key) {

        if (key == null)
            throw new IllegalArgumentException();
        
        Value value = (Value)super.lookup(key);
        
        if(value==null||value.deleted) return null;
        
        return value.datum;
        
    }

    /**
     * If the key exists and the entry is not deleted, then inserts a deleted
     * entry under the key. if the key exists and is deleted, then this is a
     * NOP.
     * 
     * @param key
     *            The search key.
     * 
     * @return The old value (may be null) and null if the key did not exist or
     *         if the entry was marked as deleted.
     */
    public Object remove(Object key) {

        if (key == null)
            throw new IllegalArgumentException();

        Value value = (Value)super.lookup(key);
        
        if(value==null||value.deleted) return null;
        
        super.insert(key, new Value(value.nextVersionCounter(), true, null));
        
        return value.datum; // may be null.
        
    }

    /**
     * If the key does not exists or if the key exists, then insert/update an
     * entry under that key with a new version counter. Otherwise, update the
     * entry under that key in order to increment the version counter (this
     * includes the case where the key is paired with a deletion marker).
     * 
     * @param key
     *            The search key.
     * @param val
     *            The value.
     * 
     * @return The old value under that key (may be null) and null if the key
     *         was marked as deleted or if the key was not found.
     */
    public Object insert(Object key, Object val) {

        if (key == null)
            throw new IllegalArgumentException();
 
        Value value = (Value)super.lookup(key);
        
        if (value == null) {

            super.insert(key, new Value(IValue.FIRST_VERSION_UNISOLATED, false,
                    (byte[]) val));

            return null;

        }

        super.insert(key, new Value(value.nextVersionCounter(), false,
                (byte[]) val));
        
        return value.datum;
        
    }

    /**
     * Overriden to return <code>null</code> if the entry at that index is
     * deleted.
     */
    public Object valueAt(int index) {
        
        Value value = (Value)super.valueAt(index);
        
        if(value==null||value.deleted) return null;
        
        return value.datum;
        
    }
    
    /**
     * This method will include deleted entries in the key range in the returned
     * count.
     */
    public int rangeCount(byte[] fromKey, byte[] toKey) {
        
        return super.rangeCount(fromKey, toKey);
        
    }

    @Override
    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {
        // TODO Auto-generated method stub
        return super.rangeIterator(fromKey, toKey);
    }

    @Override
    public IEntryIterator entryIterator() {
        // TODO Auto-generated method stub
        return super.entryIterator();
    }

    public void contains(BatchContains op) {

        op.apply(this);
        
    }

    public void insert(BatchInsert op) {
        
        op.apply(this);
        
    }

    public void lookup(BatchLookup op) {
        
        op.apply(this);
        
    }

    public void remove(BatchRemove op) {
        
        op.apply(this);
        
    }

}
