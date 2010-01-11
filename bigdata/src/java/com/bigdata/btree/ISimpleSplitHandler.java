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
     * Return a separator key close to the recommended separator key which
     * is acceptable to the constraints imposed by the application on the
     * index. An application may apply a constraint when it provides some
     * guarantee which would otherwise not be obtained. For example, the
     * {@link SparseRowStore} uses this mechanism to constrain splits such
     * that a logical row is never split, which gives the
     * {@link SparseRowStore} the ability to offer ACID operations on
     * logical rows using only local locking.
     * <p>
     * Applications can implement this constraint in many different ways.
     * For example: (A) applications can scan forward or backward using an
     * {@link ITupleCursor} looking for an acceptable separatorKey near to
     * the recommended separatorKey; (B) applications can use the
     * {@link ILinearList} API to probe for a suitable key; or (C)
     * applications can truncate the recommended separator key in order to
     * obtain a prefix which will respect some application constraint such
     * as all RDF statements sharing a common subject.
     * 
     * @param seg
     *            The {@link IndexSegment} containing the data to be split.
     * @param fromIndex
     *            The index of the first tuple which may be considered
     *            (inclusive lower bound).
     * @param toIndex
     *            The index of the last tuple which may be considered
     *            (exclusive upper bound).
     * @param splitAt
     *            The index of the recommended separator key.
     * 
     * @return The desired separator key. If no acceptable separator key
     *         could be found or constructed then return <code>null</code>
     *         and the remainder of the data will go into the next split.
     */
    byte[] getSeparatorKey(IndexSegment seg, int fromIndex, int toIndex, int splitAt);
    
}