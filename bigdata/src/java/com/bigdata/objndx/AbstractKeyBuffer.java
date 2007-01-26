package com.bigdata.objndx;

/**
 * Class with implementations supporting mutable and immutable variable length
 * byte[] keys.
 * 
 * @todo supporting a scalar int32 key natively would provide better performance
 *       for an object index. once it is up to int64, the byte[] approach could
 *       in fact be better on 32-bit hardware.
 * 
 * @todo explore the use of sparse buffers that minimize copying for more
 *       efficient management of large #s of keys - how would search work for
 *       such buffers?
 * 
 * @todo explore use of interpolated search. certainly we should be able to
 *       estimate the distribution of the keys when creating an immutable key
 *       buffer and choose binary vs interpolated vs linear search based on
 *       that.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractKeyBuffer implements IKeyBuffer {


    /**
     * Test the search key against the leading prefix shared by all bytes in the
     * key buffer.
     * 
     * @param prefixLength
     *            The length of the prefix shared by all keys in the buffer.
     * 
     * @param searchKey
     *            The search key.
     * 
     * @return Zero iff all bytes match and otherwise the insert position for
     *         the search key in the buffer. The insert position will be before
     *         the first key iff the search key is less than the prefix (-1) and
     *         will be after the last key iff the search key is greater than the
     *         prefix (-(nkeys)-1).
     */
    abstract protected int _prefixMatchLength(final int prefixLength,
            final byte[] searchKey);
    
    /**
     * Linear search.
     */
    abstract protected int _linearSearch(final int searchKeyOffset, final byte[] searchKey);

    /**
     * Binary search.
     */
    abstract protected int _binarySearch(final int searchKeyOffset, final byte[] searchKey);

}
