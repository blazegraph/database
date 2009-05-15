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
 * Created on May 7, 2009
 */

package com.bigdata.service.ndx;

import java.util.Arrays;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.keys.KVO;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.Split;

/**
 * Basic implementation - you only need to provide resolution for the
 * {@link IMetadataIndex}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractSplitter implements ISplitter {

    protected static final transient Logger log = Logger.getLogger(AbstractSplitter.class);

    /**
     * Return the {@link IMetadataIndex} that will be used to compute the
     * {@link Split}s
     * 
     * @param ts
     *            The timestamp of the {@link IMetadataIndex} view.
     *            
     * @return The {@link IMetadataIndex}.
     */
    protected abstract IMetadataIndex getMetadataIndex(long ts);

    public AbstractSplitter() {

    }

    /**
     * {@inheritDoc}
     * 
     * Find the partition for the first key. Check the last key, if it is in the
     * same partition then then this is the simplest case and we can just send
     * the data along.
     * <p>
     * Otherwise, perform a binary search on the remaining keys looking for the
     * index of the first key GTE the right separator key for that partition.
     * The batch for this partition is formed from all keys from the first key
     * for that partition up to but excluding the index position identified by
     * the binary search (if there is a match; if there is a miss, then the
     * binary search result needs to be converted into a key index and that will
     * be the last key for the current partition).
     * <p>
     * Examine the next key and repeat the process until all keys have been
     * allocated to index partitions.
     * <p>
     * Note: Split points MUST respect the "row" identity for a sparse row
     * store, but we get that constraint by maintaining the index partition
     * boundaries in agreement with the split point constraints for the index.
     * <p>
     * Note: The splitter always detect keys out of order and will throw an
     * {@link IllegalArgumentException}. This is done since it is otherwise too
     * easy for applications to produce unordered data which would then quietly
     * violate this expectation if we relied on asserts.
     * 
     * @see Arrays#sort(Object[], int, int, java.util.Comparator)
     * 
     * @see BytesUtil#compareBytes(byte[], byte[])
     * 
     * @todo Caching? This procedure performs the minimum #of lookups using
     *       {@link IMetadataIndex#find(byte[])} since that operation will be an
     *       RMI in a distributed federation. The find(byte[] key) operation is
     *       difficult to cache since it locates the index partition that would
     *       span the key and many, many different keys could fit into that same
     *       index partition. The only effective cache technique may be an LRU
     *       that scans ~10 caches locators to see if any of them is a match
     *       before reaching out to the remote {@link IMetadataService}. Or
     *       perhaps the locators can be cached in a local BTree and a miss
     *       there would result in a read through to the remote
     *       {@link IMetadataService} but then we have the problem of figuring
     *       out when to release locators if the client is long-lived.
     */
    public LinkedList<Split> splitKeys(final long ts, final int fromIndex,
            final int toIndex, final byte[][] keys) {

        if (keys == null)
            throw new IllegalArgumentException();

        if (fromIndex < 0)
            throw new IllegalArgumentException();

        if (fromIndex >= toIndex)
            throw new IllegalArgumentException();

        if (toIndex > keys.length)
            throw new IllegalArgumentException();
        
        final LinkedList<Split> splits = new LinkedList<Split>();
        
        // start w/ the first key.
        int currentIndex = fromIndex;
        
        byte[] lastKey = null;
        
        while (currentIndex < toIndex) {
            
            final byte[] key = keys[currentIndex];
            
            if (key == null) {

                throw new IllegalArgumentException("null @ index="
                        + currentIndex);

            }

            if (lastKey != null && BytesUtil.compareBytes(lastKey, key) > 0) {

                /*
                 * Make sure that the keys are ordered.
                 * 
                 * Note: We do allow duplicate keys since that is common when
                 * writes are combined on an asynchronous write pipeline but
                 * duplicate detection can not be enabled. E.g., TERM2ID which
                 * uses KVOLatch.
                 */

                throw new IllegalArgumentException("keys out of order @ index="
                        + currentIndex + " : lastKey="
                        + BytesUtil.toString(lastKey) + ", thisKey="
                        + BytesUtil.toString(key));

            }
            
            // update before we go any further.
            lastKey = key;
            
            /*
             * This is partition spanning the current key (RMI)
             * 
             * Note: Using the caller's timestamp here!
             */
            final PartitionLocator locator = getMetadataIndex(ts).find(key);

            if (locator == null)
                throw new RuntimeException("No index partitions?");
            
            final byte[] rightSeparatorKey = locator.getRightSeparatorKey();

            if (rightSeparatorKey == null) {

                /*
                 * The last index partition does not have an upper bound and
                 * will absorb any keys that order GTE to its left separator
                 * key.
                 */

                isValidSplit( locator, currentIndex, toIndex, keys );
                
                splits.add(new Split(locator, currentIndex, toIndex));

                // done.
                currentIndex = toIndex;

            } else {

                /*
                 * Otherwise this partition has an upper bound, so figure out
                 * the index of the last key that would go into this partition.
                 * 
                 * We do this by searching for the rightSeparator of the index
                 * partition itself.
                 */
                
                int pos = BytesUtil.binarySearch(keys, currentIndex, toIndex
                        - currentIndex, rightSeparatorKey);

                if (pos >= 0) {

                    /*
                     * There is a hit on the rightSeparator key. The index
                     * returned by the binarySearch is the exclusive upper bound
                     * for the split. The key at that index is excluded from the
                     * split - it will be the first key in the next split.
                     * 
                     * Note: There is a special case when the keys[] includes
                     * duplicates of the key that corresponds to the
                     * rightSeparator. This causes a problem where the
                     * binarySearch returns the index of ONE of the keys that is
                     * equal to the rightSeparator key and we need to back up
                     * until we have found the FIRST ONE.
                     * 
                     * Note: The behavior of the binarySearch is effectively
                     * under-defined here and sometimes it will return the index
                     * of the first key EQ to the rightSeparator while at other
                     * times it will return the index of the second or greater
                     * key that is EQ to the rightSeparatoer.
                     */
                    
                    while (pos > currentIndex) {
                        
                        if (BytesUtil.bytesEqual(keys[pos - 1],
                                rightSeparatorKey)) {

                            // keep backing up.
                            pos--;

                            continue;

                        }
                        
                        break;
                        
                    }

                    if (log.isDebugEnabled())
                        log.debug("Exact match on rightSeparator: pos=" + pos
                                + ", key=" + BytesUtil.toString(keys[pos]));

                } else if (pos < 0) {

                    /*
                     * There is a miss on the rightSeparator key (it is not
                     * present in the keys that are being split). In this case
                     * the binary search returns the insertion point. We then
                     * compute the exclusive upper bound from the insertion
                     * point.
                     */

                    pos = -pos - 1;

                    assert pos > currentIndex && pos <= toIndex : "Expected pos in ["
                            + currentIndex + ":" + toIndex + ") but pos=" + pos;

                }

                /*
                 * Note: this test can be enabled if you are having problems
                 * with KeyAfterPartition or KeyBeforePartition. It will go
                 * through more effort to validate the constraints on the split.
                 * However, due to the additional byte[] comparisons, this
                 * SHOULD be disabled except when tracking a bug.
                 */
//                assert validSplit( locator, currentIndex, pos, keys );

                splits.add(new Split(locator, currentIndex, pos));

                currentIndex = pos;

            }

        }

        return splits;

    }

    /**
     * Reshape the data into an unsigned byte[][] and then invoke
     * {@link #splitKeys(long, int, int, byte[][])}.
     */
    public LinkedList<Split> splitKeys(final long ts, final int fromIndex,
            final int toIndex, final KVO[] a) {

        /*
         * Change the shape of the data so that we can split it.
         */

        final byte[][] keys = new byte[a.length][];

        for (int i = 0; i < a.length; i++) {

            keys[i] = a[i].key;

        }

        return splitKeys(ts, fromIndex, toIndex, keys);

    }

    /**
     * Paranoia testing for generated splits.
     * 
     * @param locator
     * @param fromIndex
     * @param toIndex
     * @param keys
     * @return
     */
    private boolean isValidSplit(final PartitionLocator locator,
            final int fromIndex, final int toIndex, final byte[][] keys) {

        assert fromIndex <= toIndex : "fromIndex=" + fromIndex + ", toIndex="
                + toIndex;

        assert fromIndex >= 0 : "fromIndex=" + fromIndex;

        assert toIndex <= keys.length : "toIndex=" + toIndex + ", keys.length="
                + keys.length;

        // begin with the left separator on the index partition.
        byte[] lastKey = locator.getLeftSeparatorKey();
        
        assert lastKey != null;

        for (int i = fromIndex; i < toIndex; i++) {

            final byte[] key = keys[i];

            assert key != null;

            if (lastKey != null) {

                final int ret = BytesUtil.compareBytes(lastKey, key);

                if (ret > 0)
                    throw new IllegalArgumentException("keys out of order: i="
                            + i + ", lastKey=" + BytesUtil.toString(lastKey)
                            + ", key=" + BytesUtil.toString(key)
//                            + ", keys=" + BytesUtil.toString(keys)
                            );
                
            }
            
            lastKey = key;
            
        }

        // Note: Must be strictly LT the rightSeparator key (when present).
        {

            final byte[] key = locator.getRightSeparatorKey();

            if (key != null) {

                final int ret = BytesUtil.compareBytes(lastKey, key);

                if (ret >= 0)
                    throw new IllegalArgumentException(
                            "keys out of order: lastKey="
                                    + BytesUtil.toString(lastKey)
                                    + ", rightSeparator="
                                    + BytesUtil.toString(key)
                    // +", keys="+BytesUtil.toString(keys)
                    );

            }
            
        }
        
        return true;
        
    }
    
}
