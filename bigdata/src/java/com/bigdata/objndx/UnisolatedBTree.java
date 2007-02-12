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

package com.bigdata.objndx;

import com.bigdata.rawstore.IRawStore;

/**
 * A scalable mutable B+-Tree mapping variable length unsigned byte[] keys to
 * byte[] values and supporting transactions and deletion markers. All read
 * operations implemented by this class read through on a miss to an unisolated
 * index specified to the constructor. All write operations write solely on the
 * isolated index.
 * <p>
 * Note that {@link #rangeCount(byte[], byte[])} MAY report more entries than
 * would actually be visited. Since it because it depends on
 * {@link #indexOf(byte[])} and the latter does not differentiate between
 * entries which have and have not been {@link IValue#isDeleted() deleted}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME I need to differentiate between a class than knows about delete markers
 * and a class that knows about delete markers and is providing isolation by
 * reading through to a read-only historical btree containing Value objects and
 * writing Value objects in the isolated btree.
 * 
 * FIXME Some methods need to be overriden for this api. The most obvious are
 * remove() (must be an insert that sets a delete markers and clears the old
 * value), contains() (must return true iff the value is not marked as deleted),
 * lookup() (must return null if the value is marked as deleted), and insert
 * (must insert a Value object wrapping the application value and what to do if
 * the value is marked as deleted?)
 * 
 * @todo the batch api also needs to be overriden for this btree so that it has
 *       the correct semantics. unfortunately, this either needs to be done
 *       inside of the methods implemented on the {@link Leaf} or (more simply)
 *       by a simple loop over the batch operation implemented in this class and
 *       leaving optimized batch operations to an unisolated index.
 * 
 * @todo efficient sharing of nodes and leaves for concurrent read-only views
 *       (stealing children vs wrapping them with a flyweight wrapper; reuse of
 *       the same btree instance for reading from the same historical state).
 * 
 * @see IsolatedBTree
 */
public class UnisolatedBTree extends BTree implements IIsolatableIndex {

    /**
     * Create an isolated btree.
     * 
     * @param store
     * @param branchingFactor
     */
    public UnisolatedBTree(IRawStore store, int branchingFactor) {
       
        super(store, branchingFactor, Value.Serializer.INSTANCE );
        
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
    public Object lookup(byte[] key) {
        
        Value value = (Value)super.lookup(key);
        
        if(value==null||value.deleted) return null;
        
        return value.value;
        
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
    public Object remove(byte[] key) {

        Value value = (Value)lookup(key);
        
        if(value==null||value.deleted) return null;
        
        return super.insert(key, new Value(value.versionCounter, true, null));
        
    }

    /**
     * If the key does not exists or if the key exists and the entry is deleted,
     * then insert/update an entry under that key with a new version counter.
     * Otherwise, update the entry under that key in order to increment the
     * version counter.
     * 
     * @param key
     *            The search key.
     * @param val
     *            The value.
     * 
     * @return The old value under that key (may be null) and null if the key
     *         was marked as deleted or if the key was not found.
     */
    public Object insert(byte[]key, Object val) {
 
        Value value = (Value)super.lookup(key);
        
        if (value == null || value.deleted) {

            super.insert(key, new Value(IValue.FIRST_VERSION_UNISOLATED, false,
                    null));

            return null;

        }

        super.insert(key, new Value(value.nextVersionCounter(), false,
                value.value));
        
        return value.value;
        
    }
    
}
