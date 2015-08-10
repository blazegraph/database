package com.bigdata.btree;

import com.bigdata.sparse.SparseRowStore;

/**
 * Interface allows an application to constrain the choice of the separator
 * key when an index partition is split.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 */
public interface ISimpleSplitHandler {

    /**
     * Return a separator key <em>close</em> to the recommended separator key
     * (<i>splitAt</i>) which is acceptable to the constraints imposed by the
     * application on the index. This method is invoked iteratively starting
     * with the index of first addressable tuple in the index segment as the
     * <i>fromIndex</i> and the index of the last addressable tuple as the
     * <i>toIndex</i>. Each time a non-<code>null</code> separator key is
     * returned, the <i>fromIndex</i> is advanced to the first key GTE the
     * separator key. This process continues until no more splits may be formed,
     * whether because there are not enough tuples remaining to justify a split
     * or because the application has refused a split for the key range by
     * returning <code>null</code> from this method. To accept the recommended
     * separator key, the application should do <code>
     * return seg.keyAt(splitAt)</code>
     * <p>
     * An application may apply a constraint when it provides some guarantee
     * which would otherwise not be obtained. For example, the
     * {@link SparseRowStore} uses this mechanism to constrain splits such that
     * a logical row is never split, which gives the {@link SparseRowStore} the
     * ability to offer ACID operations on logical rows using only local
     * locking.
     * <p>
     * Applications can implement this constraint in many different ways. For
     * example: (A) applications can scan forward or backward using an
     * {@link ITupleCursor} looking for an acceptable separatorKey near to the
     * recommended separatorKey; (B) applications can use the
     * {@link ILinearList} API to probe for a suitable key; or (C) applications
     * can truncate the recommended separator key in order to obtain a prefix
     * which will respect some application constraint such as all RDF statements
     * sharing a common subject.
     * <p>
     * If the implementation returns <code>null</code> then it is asserting that
     * there is no separator key in the half-open range which is acceptable and
     * therefore that the key-range CAN NOT be split. All tuples in that key
     * range will therefore go into a single split.
     * 
     * @param seg
     *            The {@link IndexSegment} containing the data to be split.
     * @param fromIndex
     *            The index of the first tuple which may be considered
     *            (inclusive lower bound).
     * @param toIndex
     *            The index of the last tuple which may be considered (exclusive
     *            upper bound).
     * @param splitAt
     *            The index of the recommended separator key.
     * 
     * @return The desired separator key. If no acceptable separator key could
     *         be found or constructed then return <code>null</code>.
     */
    byte[] getSeparatorKey(IndexSegment seg, int fromIndex, int toIndex,
            int splitAt);

}
