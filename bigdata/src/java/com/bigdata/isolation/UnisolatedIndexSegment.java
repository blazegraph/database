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
 * Created on Mar 7, 2007
 */

package com.bigdata.isolation;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BatchContains;
import com.bigdata.btree.BatchLookup;
import com.bigdata.btree.IEntryFilter;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentExtensionMetadata;
import com.bigdata.btree.IndexSegmentFileStore;
import com.bigdata.isolation.UnisolatedBTree.DeletedEntryFilter;

/**
 * <p>
 * A scalable read-only B+-Tree mapping variable length unsigned byte[] keys to
 * byte[] values that is capable of being isolated by a transaction (it
 * maintains version counters) and supports deletion markers. Application data
 * are transparently encapsulated in {@link IValue} objects which keep track of
 * version counters (in support of transactions) and deletion markers (in
 * support of both transactions and partitioned indices). Users of this class
 * will only see application values, not {@link IValue} objects.
 * </p>
 * 
 * @see UnisolatedBTree, which provides a mutable implementation with a similar
 *      contract.
 * 
 * @todo This class should either share code or tests cases with
 *       {@link UnisolatedBTree} (I just copied over the logic for non-mutation
 *       operations). There are no direct tests of this class at this time.
 * 
 * @todo define extension that stores the index name for a named index to which
 *       the segment belongs (add method to {@link AbstractBTree} to allow
 *       subclassing {@link IndexSegmentExtensionMetadata})? (Note that we
 *       already store the indexUUID and indices might be renamed so this may
 *       not be a good idea).
 * 
 * @todo add a boolean flag to mark index segments that are the final result of
 *       a compacting merge. This will make it possible to reconstruct from the
 *       file system which index segments are part of the consistent state for a
 *       given restart time.
 * 
 * @todo consider caching the first/last key in support of both correct
 *       rejection of queries directed to the wrong index segment and managing
 *       the metadataMap for a distributed index (it could be part of the
 *       extension metadata record).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class UnisolatedIndexSegment extends IndexSegment implements IIsolatableIndex {

    /**
     * 
     */
    public UnisolatedIndexSegment(IndexSegmentFileStore store) {
        super(store);
    }

//    /**
//     * This method breaks isolation to return the {@link Value} for a key.
//     * It is used by {@link IsolatedBTree#validate(UnisolatedBTree)} to test
//     * version counters when a key already exists in the global scope.
//     * 
//     * @todo make protected and refactor tests so that we do not need public
//     *       access to this method. there should be tests in this package
//     *       that examine the specific version counters that are assigned
//     *       such that we do not need to expose this method as public.
//     */
//    final public Value getValue(byte[] key) {
//
//        return (Value) super.lookup(key);
//
//    }

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

        Value value = (Value) super.lookup(key);

        if (value == null || value.deleted)
            return false;

        return true;

    }

    /**
     * Return the {@link IValue#getValue()} associated with the key or
     * <code>null</code> if the key is not found or if the key was found
     * by the entry is flagged as {@link IValue#isDeleted()}.
     * 
     * @param key
     *            The search key.
     * 
     * @return The application value stored under that search key (may be
     *         null) or null if the key was not found or if they entry was
     *         marked as deleted.
     */
    public Object lookup(Object key) {

        if (key == null)
            throw new IllegalArgumentException();

        Value value = (Value) super.lookup(key);

        if (value == null || value.deleted)
            return null;

        return value.datum;

    }

    /**
     * Overriden to return <code>null</code> if the entry at that index is
     * deleted.
     */
    public Object valueAt(int index) {

        Value value = (Value) super.valueAt(index);

        if (value == null || value.deleted)
            return null;

        return value.datum;

    }

    /**
     * This method will include deleted entries in the key range in the
     * returned count.
     */
    public int rangeCount(byte[] fromKey, byte[] toKey) {

        return super.rangeCount(fromKey, toKey);

    }

    /**
     * Visits only the non-deleted entries in the key range.
     */
    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey, int capacity, int flags, IEntryFilter filter) {

        final IEntryFilter f = DeletedEntryFilter.INSTANCE;

        if (filter != null) {

            f.add(filter);

        }

        return super.rangeIterator(fromKey, toKey, capacity, flags, f);

    }

    public IEntryIterator entryIterator() {

        return rangeIterator(null, null);

    }

    public void contains(BatchContains op) {

        op.apply(this);

    }

    public void lookup(BatchLookup op) {

        op.apply(this);

    }

}
