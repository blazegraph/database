/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Oct 4, 2010
 */

package com.bigdata.rdf.spo;

import java.io.Serializable;

import org.apache.log4j.Logger;

import com.bigdata.btree.ILinearList;
import com.bigdata.btree.ISimpleSplitHandler;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.SuccessorUtil;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;

/**
 * A split handler for the xxxC quads indices which ensures that all quads for
 * the same "triple" are in the same shard. The use of this split handler allows
 * optimizations for default graph queries in scale-out. Since all quads for a
 * given triple are known to be on the same shard, we can use the
 * {@link ContextAdvancer} to skip to the next possible triple without imposing
 * a "DISTINCT" filter.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class XXXCShardSplitHandler implements ISimpleSplitHandler, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private static final transient Logger log = Logger
            .getLogger(XXXCShardSplitHandler.class);

    /**
     * Return a separator key which does not split quads for the same triple
     * prefix.
     * <p>
     * A "quad" key is formed by an <code>IV[4]</code> array. For an index whose
     * last key component is "C" (context), the first three key components
     * identify some permutation of a triple. This will return a separator key
     * such that all "quads" for a given triple are on the same shard.
     * <p>
     * On entry, <i>splitAt</i> will be the index of some quads key. From that
     * key, this method generates (a) the key corresponding to that triple from
     * <code>IV[3]</code>; and (b) the key corresponding to the next triple from
     * <code>successor(IV[3])</code>.
     * <p>
     * The {@link ILinearList#indexOf(byte[])} positions of the two keys are
     * considered within the index segment. The key is returned whose index
     * position is closest to the given <i>splitAt</i> index. If neither of
     * these two keys lies within the allowable half-open range
     * <code>(fromIndex:toIndex])</code> then an error message is logged and
     * <code>null</code> is returned, indicating that the {@link IndexSegment}
     * should not be split.
     */
    public byte[] getSeparatorKey(final IndexSegment seg, final int fromIndex,
            final int toIndex, final int splitAt) {

        // extract the key.
        final byte[] key = seg.keyAt(splitAt);

        // decode the first three components of the key.
        @SuppressWarnings("unchecked")
        final IV[] terms = IVUtility.decode(key, 3/* nterms */);

        // key builder.
        final IKeyBuilder keyBuilder = KeyBuilder.newInstance(64/* capacity */);

        // encode the first three components of the key.
        IVUtility.encode(keyBuilder, terms[0]);
        IVUtility.encode(keyBuilder, terms[1]);
        IVUtility.encode(keyBuilder, terms[2]);

        // obtain the key for that triple prefix.
        final byte[] fromKey = keyBuilder.getKey();

        /*
         * The index position of [fromKey] in the segment.
         * 
         * Note: The [fromKey] is a triple and the index contains quads so
         * indexOf() will return an insertion point which we convert to an
         * index. That index will be a quad actually present in the B+Tree.
         */
        final int fromKeyAt = -(seg.indexOf(fromKey)) - 1;

        // obtain the successor of the key (next possible triple prefix).
        final byte[] toKey = SuccessorUtil.successor(fromKey.clone());

        /*
         * The index position of [toKey] in the segment.
         * 
         * Note: The [toKey] is a triple and the index contains quads so
         * indexOf() will return an insertion point which we convert to an
         * index. That index will be a quad actually present in the B+Tree
         * unless it is beyond the end of the B+Tree.
         */
        final int toKeyAt = -(seg.indexOf(toKey)) - 1;

        /*
         * [splitAt] is the index of a quad.  fromKey must be LTE that quad.
         * toKey must be GT that quad.
         */
        assert fromKeyAt <= splitAt;
        assert splitAt < toKeyAt;
        
        final int nbefore = splitAt - fromKeyAt;

        final int nafter = toKeyAt - splitAt;

        final boolean useFromKey = nbefore < nafter && fromKeyAt >= fromIndex;
        
        final boolean useToKey = toKeyAt < toIndex;

        final byte[] separatorKey = useFromKey ? fromKey : useToKey ? toKey
                : null;

//        System.err.println("Given: {splitAt=" + splitAt + ", fromIndex="
//                + fromIndex + ", toIndex=" + toIndex + "}");
//        System.err.println("Found: {fromKeyAt=" + fromKeyAt + ", useFromKey="
//                + useFromKey + ", toKeyAt=" + toKeyAt + ", useToKey="
//                + useToKey + "}");
//        System.err.println("splitAt: index=" + splitAt + ", key="
//                + BytesUtil.toString(key));
//        System.err.println("fromKey: index=" + fromKeyAt + ", key="
//                + BytesUtil.toString(fromKey));
//        System.err.println("toKey  : index=" + toKeyAt + ", key="
//                + BytesUtil.toString(toKey));
//        System.err.println("chosen : index="
//                + (useFromKey ? fromKeyAt : useToKey ? toKeyAt : "n/a")
//                + ", key=" + BytesUtil.toString(separatorKey));
//        for (int i = splitAt - 10; i < splitAt + 10; i++) {
//            System.err.println("tuples : index=" + i + ", key="
//                    + BytesUtil.toString(seg.keyAt(i)));
//        }

        if (separatorKey == null)
            log.error("Could not split shard: name="
                    + seg.getIndexMetadata().getName()
                    + ", partitionId="
                    + seg.getIndexMetadata().getPartitionMetadata()
                            .getPartitionId());

        return separatorKey;

    }

}
