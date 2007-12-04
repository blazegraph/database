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
package com.bigdata.btree;

/**
 * A compact and efficient representation of immutable keys. The largest common
 * prefix for the keys is factored out into the start of an internal buffer and
 * is followed by the remainder of each key in key order.
 * <p>
 * Note: This class needs to know the maximum #of keys as well as the #of keys
 * so that the branching factor of a node or leaf does not change as a result of
 * serializing an immutable key buffer and then converting it to a mutable key
 * buffer. It is essential that we maintain this information since the branching
 * factor of all nodes in a given level of the tree must be the same in order
 * for the btree split and join operations to work correctly.
 * 
 * @todo use a delta from key to key specifying how much to be reused - works
 *       well if deserializing keys or for a linear key scan, but not for binary
 *       search on the raw node/leaf record.
 * 
 * @todo verify that keys are monotonic during constructor from mutable keys and
 *       during de-serialization.
 * 
 * @todo add nkeys to the abstract base class and keep a final (read-only) copy
 *       on this class. if the value in the base class is changed then it is an
 *       error but this allows direct access to the field in the base class for
 *       the mutable key buffer implementation.
 * 
 * @todo even more compact serialization of sorted byte[]s is doubtless
 *       possible, perhaps as a patricia tree, trie, or other recursive
 *       decomposition of the prefixes. figure out what decomposition is the
 *       most compact, easy to compute from sorted byte[]s, and supports fast
 *       search on the keys to identify the corresponding values.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ImmutableKeyBuffer extends AbstractKeyBuffer {

    /**
     * The #of defined keys.
     */
    final int nkeys;
    
    /**
     * The maximum #of defined keys.  This value is required in order to be able
     * to recreate a {@link MutableKeyBuffer} with the correct capacity.  It is
     * essentially that the buffer capacity remain unchanged since split and merge
     * operations require all nodes at the same level of the tree to have the same
     * branching factor.
     */
    final int maxKeys;

    /**
     * The offset of each key remainder in {@link #buf}. The shared prefix is
     * stored before any key remainder and <code>offsets[0]</code> therefore
     * gives the length of the shared prefix. The length of each key remainder
     * is computed as offsets[index+1] - offsets[index], except for the last key
     * remainder where the length is computed as buf.length - offsets[index].
     */
    final int[] offsets;
    
    /**
     * The key buffer. The first {@link #prefixLength} bytes containing the
     * shared leading prefix for the keys. The subsequent bytes containing
     * the remaining bytes from each key in key order. {@link #offsets}
     * gives the index of the start of each remainder of each key in key
     * order.
     */
    final byte[] buf;
    
    final public int getKeyCount() {
        
        return nkeys;
        
    }
    
    final public int getPrefixLength() {
        
        if( nkeys == 0 ) return 0;
        
        return offsets[0];
        
    }
    
    public int getMaxKeys() {
        
        return maxKeys;
        
    }
    
    public boolean isFull() {
        
        return nkeys == maxKeys;
        
    }

    /**
     * Creates an immutable key buffer from a mutable one.
     */
    public ImmutableKeyBuffer(MutableKeyBuffer kbuf) {
        
        this(kbuf.nkeys, kbuf.keys.length, kbuf.keys);
        
    }
    
    /**
     * Isolates the leading prefix in the keys and builds an index into a byte[]
     * containing the ordered contatenation of the part of the keys that follow
     * the leading prefix.
     * 
     * @param nkeys
     *            The #of keys in <i>keys</i> that are currenty defined
     *            (indices [0:nkeys-1] are defined).
     * @param maxKeys
     *            The maximum #of keys that would be allowed into the keys
     *            buffer if the keys buffer were mutable. (This information is
     *            required in order to preserve the branching factor of nodes
     *            and leaves as their keys are converted to immutable form and
     *            then back to mutable form. The split and join operations for
     *            the btree require all nodes in the same level to have the same
     *            branching factor, so it is essential to maintain this
     *            information.)
     * @param keys
     *            An array of variable length byte[] keys in sorted order.
     */
    public ImmutableKeyBuffer(int nkeys, int maxKeys, byte[][] keys) {
        
        if (nkeys < 0) {

            throw new IllegalArgumentException("nkeys negative");

        }

        if( maxKeys < nkeys ) {
            
            throw new IllegalArgumentException("maxKeys less than nkeys");
            
        }
        
        if( keys == null ) {
            
            throw new IllegalArgumentException("keys is null");
            
        }
        
        // assert nkeys >= (AbstractBTree.MIN_BRANCHING_FACTOR+1)/2;
        assert keys != null;
        assert keys.length >= nkeys;

        // the #of defined keys.
        this.nkeys = nkeys;
        
        // the maximum #of keys allowed.
        this.maxKeys = maxKeys;
        
        // length of the shared prefix.
        int prefixLength = nkeys == 0 ? 0 : BytesUtil.getPrefixLength(keys[0],
                keys[nkeys - 1]);
        
        // offsets into the key buffer.
        this.offsets = new int[nkeys];
        
        // total length of the key buffer.
        int bufsize = prefixLength;
        
        for(int i=0; i<nkeys; i++) {
            
            // offset to the remainder of the ith key in the buffer.
            offsets[i] = bufsize;
            
            int remainder = keys[i].length - prefixLength;
            
            assert remainder >= 0;
            
            bufsize += remainder;
            
        }
        
        /* allocate buffer and copy in the prefix followed by the remainder
         * of each key in turn.
         */
        buf = new byte[bufsize];

        if (nkeys > 0) {

            System.arraycopy(keys[0], 0, buf, 0, prefixLength);

            int offset = prefixLength;

            for (int i = 0; i < nkeys; i++) {

                int remainder = keys[i].length - prefixLength;

                System.arraycopy(keys[i], prefixLength, buf, offset, remainder);

                offset += remainder;

            }
            
        }
        
    }

    /**
     * De-serialization constructor.
     * 
     * @param nkeys
     * @param offsets
     * @param buf
     */
    public ImmutableKeyBuffer(int nkeys, int maxKeys, int[] offsets, byte[] buf ) {
        
        assert nkeys >= 0;
//      assert nkeys >= AbstractBTree.MIN_BRANCHING_FACTOR;
        
        assert maxKeys >= nkeys;
        
        assert offsets != null;
        
        assert offsets.length == nkeys;
        
        assert buf != null;
        
//        if( nkeys >0 ) {
//            if( buf.length <= offsets[nkeys-1]) {
//            System.err.println("nkeys=" + nkeys + ", maxKeys=" + maxKeys
//                    + ", offsets.length=" + offsets.length + ", buf.length="
//                    + buf.length + ", offsets=" + Arrays.toString(offsets)
//                    + ", buf=" + BytesUtil.toString(buf));
//            throw new AssertionError("buf.length=" + buf.length
//                    + ", offsets[nkeys=" + nkeys + "]=" + offsets[nkeys-1]);
//            }
//        }
        assert nkeys == 0 || buf.length >= offsets[nkeys - 1];
        
        this.nkeys = nkeys;
        
        this.maxKeys = maxKeys;
        
        this.offsets = offsets;
        
        this.buf = buf;
        
    }
    
    /**
     * Return a copy of the shared leading prefix.
     */
    final public byte[] getPrefix() {
        
        int prefixLength = (nkeys == 0 ? 0 : offsets[0]);
        
        byte[] prefix = new byte[prefixLength];
        
        System.arraycopy(buf, 0, prefix, 0, prefixLength);
        
        return prefix;
        
    }

    /**
     * Returns an array of variable length keys suitable for managment as part
     * of a mutable node or leaf. The keys in the returned array are full length
     * (the prefix is replicated for each key).
     * 
     * @return The mutable keys. The keys array will have {@link #getMaxKeys()}
     *         elements. The first {@link #getKeyCount()} elements will be
     *         non-null.
     */
    final public byte[][] toKeyArray() {

        byte[][] keys = new byte[maxKeys][];

        for (int i = 0; i < nkeys; i++) {

            keys[i] = getKey(i);

        }

        return keys;

    }

    /**
     * Return the full key.
     * 
     * @param index
     *            The key index in [0:nkeys-1].
     *            
     * @return The full key (prefix plus remainder).
     */
    final public byte[] getKey(int index) {
        
        assert index >= 0 && index <= nkeys;
        
        final int remainderLength = getRemainderLength(index);

        final int prefixLength = offsets[0];
        
        final int len = prefixLength + remainderLength;

        final byte[] key = new byte[len];
        
        System.arraycopy(buf, 0, key, 0, prefixLength);

        System.arraycopy(buf, offsets[index], key, prefixLength,
                remainderLength);
        
        return key;
        
    }

    /**
     * Return the remainder of the key (the key less the prefix).
     * 
     * @param index
     *            The key index in [0:nkeys-1].
     *            
     * @return A new byte[] containing a copy of the remainder of the key.
     */
    public byte[] getRemainder(int index) {

        final int len = getRemainderLength(index);
        
        final byte[] remainder = new byte[len];

        System.arraycopy(buf, offsets[index], remainder, 0, len);

        return remainder;
        
    }
    
    /**
     * Return the length of the remainder of the key (does not count the
     * prefix length).
     * 
     * @param index
     *            The key index in [0:nkeys-1].
     *            
     * @return The length of the remainder of the key.
     */
    final protected int getRemainderLength(int index) {
        
        assert index >=0 && index < nkeys;
        
        int offset = offsets[index];
        
        int next_offset = index==nkeys-1 ? buf.length : offsets[index+1];

        return next_offset - offset;
        
    }

    /**
     * Return a human readable representation of the prefix and the remainder
     * for each key.
     */
    public String toString() {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append("nkeys="+nkeys+", prefix="+BytesUtil.toString(getPrefix()));
        
        sb.append(", remainders=[");
        
        for( int i=0; i<nkeys; i++) {

            if( i > 0 ) sb.append(", ");
            
            sb.append(BytesUtil.toString(getRemainder(i)));
            
        }
        
        sb.append("]");

        return sb.toString();
        
    }
    
    public MutableKeyBuffer toMutableKeyBuffer() {

        return new MutableKeyBuffer(this);
        
    }

    final public int search(final byte[] searchKey) {

        if (searchKey == null)
            throw new IllegalArgumentException("searchKey is null");

        if( nkeys == 0 ) {
            
            /*
             * If there are no keys in the buffer, then any key would be
             * inserted at the first buffer position.
             */
            
            return -1;
            
        }

        /* 
         * Length of the prefix shared by all keys in the buffer.
         */
        final int prefixLength = offsets[0];

        /*
         * Attempt to match the shared prefix.  If we can not then return the
         * insert position, which is either before the first key or after the
         * last key in the buffer.
         */
        final int insertPosition = _prefixMatchLength(prefixLength, searchKey);
        
        if( insertPosition < 0 ) {
            
            return insertPosition;
            
        }
        
        if (nkeys < 16) {

            return _linearSearch(prefixLength, searchKey);

        } else {

            return _binarySearch(prefixLength, searchKey);

        }

    }

    final protected int _prefixMatchLength(final int prefixLength,
            final byte[] searchKey) {

        final int searchKeyLen = searchKey.length;

        /*
         * Do not compare more bytes than remain in either the search key or the
         * prefix, e.g., compareLen := min(searchKeyLen, prefixLen).
         */
        final int compareLen = (searchKeyLen <= prefixLength) ? searchKeyLen
                : prefixLength;

        int ret = BytesUtil.compareBytesWithLenAndOffset(//
                0, compareLen, searchKey,//
                0, compareLen, buf//
                );

        if (ret < 0) {
        
            /* insert before the first key. */
            
            return -1;

        } else  if (ret > 0) {
            
            /* insert after the last key. */
            
            return -(nkeys) - 1;
            
        } else {

            /*
             * For the case when the search key is _shorter_ than the prefix,
             * matching on all bytes of the search key means that the search key
             * will be ordered before all keys in the buffer.
             */
            if (searchKeyLen < prefixLength)
                return -1;
            
            /*
             * entire prefix matched, continue to search the remainder for each
             * key.
             */
            
            return 0;
            
        }
        
    }
    
    final protected int _linearSearch(final int searchKeyOffset, final byte[] searchKey) {

        // verify that the offset is exactly the prefix length.
        assert searchKeyOffset == offsets[0];

        // #of bytes to search in the search key after the prefix match.
        final int searchKeyLen = searchKey.length - searchKeyOffset;

        // searching zero or more bytes in the search key after the prefix match.
        assert searchKeyLen >= 0;
        
        for (int i = 0; i < nkeys; i++) {

            final int keyOff = offsets[i];

            // compare remainder against remainder of search key.
            final int keyLen = getRemainderLength(i);
            assert keyLen >= 0;

            // skip the first offset bytes, then compare no more bytes than
            // remain in the key.
            final int ret = BytesUtil.compareBytesWithLenAndOffset(//
                    keyOff, keyLen, buf,//
                    searchKeyOffset, searchKeyLen, searchKey //
                    );

            if (ret == 0)
                return i;

            if (ret > 0)
                return -(i + 1);

        }

        return -(nkeys + 1);

    }

    final protected int _binarySearch(final int searchKeyOffset, final byte[] searchKey) {

        // #of bytes to search in the search key after the prefix match.
        final int searchKeyLen = searchKey.length - searchKeyOffset;

        // searching zero or more bytes in the search key after the prefix match.
        assert searchKeyLen >= 0;

        int low = 0;

        int high = nkeys - 1;

        while (low <= high) {

            final int mid = (low + high) >> 1;

            final int keyOff = offsets[mid];

            // #of bytes remaining in this key after the prefix is removed.
            final int keyLen = getRemainderLength(mid);
            assert keyLen >= 0; // paranoia - true since all keys at least prefixLength long.

            /*
             * compare remainder of key against remainder of search key.
             */
            final int ret = BytesUtil.compareBytesWithLenAndOffset(//
                    keyOff, keyLen, buf,//
                    searchKeyOffset, searchKeyLen, searchKey //
                    );

            if (ret < 0) {

                low = mid + 1;

            } else if (ret > 0) {

                high = mid - 1;

            } else {

                // Found: return offset.

                return mid;

            }

        }

        // Not found: return insertion point.

        return -(low + 1);

    }

}
