/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Feb 11, 2008
 */

package com.bigdata.mdi;

import com.bigdata.btree.ILinearList;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IIndex;
import com.bigdata.service.IMetadataService;

/**
 * Interface for a metadata index.
 * 
 * @todo define implementations of this interface that handle smart caching and
 *       update of index partition metadata for the client side. It may be that
 *       those implementations should be a wrapper around an {@link IIndex} that
 *       encapsulates the logic for partition operations, but I also need to
 *       handle caching in a smart way.
 *       <p>
 *       The {@link IMetadataService} currently exposes some of the methods from
 *       the metadata index - perhaps those should be taken out of that API and
 *       moved onto {@link IMetadataIndex}, especially since we tend to cache
 *       things.
 * 
 * @todo If these methods are to be invoked remotely then we will need to
 *       returning the byte[] rather than the de-serialized
 *       {@link PartitionMetadata}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IMetadataIndex extends IIndex, ILinearList {

    /**
     * The metadata template for the scale-out index managed by this metadata
     * index.
     */
    public IndexMetadata getScaleOutIndexMetadata();
    
    /**
     * The partition with that separator key or <code>null</code> (exact match
     * on the separator key).
     * 
     * @param key
     *            The separator key (the first key that would go into that
     *            partition).
     * 
     * @return The partition with that separator key or <code>null</code>.
     */
    public PartitionMetadata get(byte[] key);

    /**
     * Find the index of the partition spanning the given key.
     * 
     * @return The index of the partition spanning the given key or
     *         <code>-1</code> iff there are no partitions defined.
     * 
     * @exception IllegalStateException
     *                if there are partitions defined but no partition spans the
     *                key. In this case the {@link MetadataIndex} lacks an entry
     *                for the key <code>new byte[]{}</code>.
     */
    public int findIndexOf(byte[] key);
    
    /**
     * Find and return the partition spanning the given key.
     * 
     * @return The partition spanning the given key or <code>null</code> if
     *         there are no partitions defined.
     */
    public PartitionMetadata find(byte[] key);

    /**
     * Return the index of the partitions corresponding to the fromKey and the
     * toKey. These are the partitions against which an operation over that
     * key-range must be mapped. Note that the indices will have the same value
     * if both keys lie within the same index partition.
     * 
     * @param fromKey
     *            The lowest key that will be counted (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will not be counted (exclusive). When
     *            <code>null</code> there is no upper bound.
     * 
     * @return An array of two elements. a[0] is the fromIndex. a[1] is the
     *         toIndex.
     * 
     * @throws IllegalArgumentException
     *             if the keys are out of order.
     * 
     * @todo change return type to <code>long</code> and pack into high/low
     *       word if this method will be executed remotely.
     */
    public int[] findIndices(byte[] fromKey,byte[] toKey);
    
}
