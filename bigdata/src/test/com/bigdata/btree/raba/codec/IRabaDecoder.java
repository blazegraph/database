package com.bigdata.btree.raba.codec;

import java.nio.ByteBuffer;

import com.bigdata.btree.raba.IRandomAccessByteArray;

/**
 * Interface for an encoded byte[][]. Instances of this interface are
 * created when the data are encoded. The interface supports random access
 * decoding of the encoded byte[]s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 */
public interface IRabaDecoder extends IRandomAccessByteArray {

    /**
     * The encoded data.
     */
    ByteBuffer data();

    /**
     * Search for the given <i>searchKey</i> in the key buffer (optional
     * operation). Whether or not search is supported depends on whether the
     * logical byte[][] is <em>ordered</em>. However, the efficiency of search,
     * where supported, depends on the implementation. Some implementations
     * support binary search of the coded byte[] values. Others may require a
     * mixture of binary and linear search, etc.
     * <p>
     * 
     * <pre>
     * entryIndex = -entryIndex - 1
     * </pre>
     * 
     * or just
     * 
     * <pre>
     * entryIndex = -entryIndex
     * </pre>
     * 
     * if you are looking for the first key after the searchKey.
     * </p>
     * 
     * @param searchKey
     *            The search key.
     * 
     * @return index of the search key, if it is found; otherwise,
     *         <code>(-(insertion point) - 1)</code>. The insertion point is
     *         defined as the point at which the key would be inserted. Note
     *         that this guarantees that the return value will be >= 0 if and
     *         only if the key is found.
     * 
     * @exception IllegalArgumentException
     *                if the searchKey is null.
     * 
     * @throws UnsupportedOperationException
     *             if search is not supported.
     */
    int search(byte[] searchKey);

    /**
     * Return <code>true</code> if the implementation supports the optional
     * {@link #search(byte[])} method.
     */
    boolean isSearchable();
    
    /**
     * Return <code>true</code> if the implementation allows <code>null</code>
     * byte[] values to be stored.
     * @return
     */
    boolean isNullAllowed();
    
}
