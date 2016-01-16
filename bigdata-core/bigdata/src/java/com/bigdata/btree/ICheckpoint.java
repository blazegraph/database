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
/*
 * Created on Nov 15, 2006
 */
package com.bigdata.btree;

import com.bigdata.rawstore.IRawStore;

/**
 * Metadata for an index checkpoint record.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface ICheckpoint {

    /**
     * The address used to read this {@link Checkpoint} record from the store.
     * <p>
     * Note: This is set as a side-effect by {@link #write(IRawStore)}.
     * 
     * @throws IllegalStateException
     *             if the {@link Checkpoint} record has not been written on a
     *             store.
     */
    long getCheckpointAddr();

    /**
     * Return <code>true</code> iff the checkpoint address is defined.
     */
    boolean hasCheckpointAddr();

    /**
     * Address that can be used to read the {@link IndexMetadata} record for the
     * index from the store.
     */
    long getMetadataAddr();

    /**
     * Address of the root node or leaf of the {@link BTree}.
     * 
     * @return The address of the root -or- <code>0L</code> iff the index does
     *         not have a root page.
     */
    long getRootAddr();

    /**
     * Address of the {@link IBloomFilter}.
     * 
     * @return The address of the bloom filter -or- <code>0L</code> iff the
     *         index does not have a bloom filter.
     */
    long getBloomFilterAddr();

    /**
     * The height of a B+Tree. ZERO(0) means just a root leaf. Values greater
     * than zero give the #of levels of abstract nodes. There is always one
     * layer of leaves which is not included in this value.
     * 
     * @return The global depth and ZERO (0) unless the checkpoint record is for
     *         an {@link IndexTypeEnum#BTree}
     */
    int getHeight();

    /**
     * The global depth of the root directory (HTree only).
     * 
     * @return The global depth and ZERO (0) unless the checkpoint record is for
     *         an {@link IndexTypeEnum#HTree}
     */
    int getGlobalDepth();

    /**
     * The #of non-leaf nodes (B+Tree) or directories (HTree). This is ZERO (0)
     * for a non-recursive data structure such as a solution set stream.
     */
    long getNodeCount();

    /**
     * The #of leaves (B+Tree), hash buckets (HTree), or ZERO (0) for a solution
     * set stream.
     */
    long getLeafCount();

    /**
     * The #of index entries (aka tuple count).
     */
    long getEntryCount();

    /**
     * Return the value of the B+Tree local counter stored in the
     * {@link Checkpoint} record.
     */
    long getCounter();

    /**
     * Return the value of the next record version number to be assigned that is
     * stored in the {@link Checkpoint} record. This number is incremented each
     * time a node or leaf is written onto the backing store. The initial value
     * is ZERO (0). The first value assigned to a node or leaf will be ZERO (0).
     */
    long getRecordVersion();

    /**
     * The type of index for this checkpoint record.
     */
    IndexTypeEnum getIndexType();

}
