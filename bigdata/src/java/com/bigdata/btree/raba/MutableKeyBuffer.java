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
package com.bigdata.btree.raba;

import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;

import com.bigdata.btree.BytesUtil;

/**
 * A flyweight mutable implementation exposing the backing byte[][] and
 * supporting search.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MutableKeyBuffer extends AbstractKeyBuffer {

    /**
     * The #of defined keys.
     */
    public int nkeys;
    
    /**
     * An array containing the keys. The size of the array is the maximum
     * capacity of the key buffer.
     */
    final public byte[][] keys;

    /**
     * Allocate a mutable key buffer capable of storing <i>capacity</i> keys.
     * 
     * @param capacity
     *            The capacity of the key buffer.
     */
    public MutableKeyBuffer(final int capacity) {

        nkeys = 0;
        
        keys = new byte[capacity][];
        
    }

    /**
     * Constructor wraps an existing byte[][].
     * 
     * @param nkeys
     *            The #of defined keys in the array.
     * @param keys
     *            The array of keys.
     */
    public MutableKeyBuffer(final int nkeys, final byte[][] keys ) {
        
        assert nkeys >= 0; // allow deficient root.
        
        assert keys != null;
        
        assert keys.length >= nkeys;
        
        this.nkeys = nkeys;
        
        this.keys = keys;
        
    }
    
    /**
     * Creates a new instance using a new array of keys but sharing the key
     * references with the provided {@link MutableKeyBuffer}.
     * 
     * @param src
     *            An existing instance.
     */
    public MutableKeyBuffer(final MutableKeyBuffer src) {

        assert src != null;

        // assert capacity > src.nkeys;

        this.nkeys = src.nkeys;

        // note: dimension to the capacity of the source.
        this.keys = new byte[src.keys.length][];

        // copy the defined keys.
        for (int i = 0; i < nkeys; i++) {

            // Note: copies the reference.
            this.keys[i] = src.keys[i];

        }

    }

    /**
     * Builds a mutable key buffer.
     * 
     * @param capacity
     *            The capacity of the new instance (this is based on the
     *            branching factor for the B+Tree).
     * @param src
     *            The source data.
     * 
     * @throws IllegalArgumentException
     *             if the capacity is LT the {@link IRaba#size()} of the
     *             <i>src</i>.
     * @throws IllegalArgumentException
     *             if the source is <code>null</code>.
     */
    public MutableKeyBuffer(final int capacity, final IRaba src) {

        if (src == null)
            throw new IllegalArgumentException();

        if (capacity < src.capacity())
            throw new IllegalArgumentException();
        
        nkeys = src.size();

        assert nkeys >= 0; // allows deficient root.

        keys = new byte[capacity][];

        int i = 0;
        for (byte[] a : src) {

            keys[i++] = a;

        }
        
    }

    /**
     * Returns a reference to the key at that index.
     */
    final public byte[] get(final int index) {

        /*
         * @todo nkeys is not always updated before using this method by the
         * btree code so the range check causes errors.
         */
        
//        assert index >= 0 && index < nkeys;

        return keys[index];

    }

    final public int length(final int index) {

        assert index >= 0 && index < nkeys;

        byte[] tmp = keys[index];
        
        if(tmp==null) throw new NullPointerException();
        
        return tmp.length;

    }
    
    final public int copy(final int index, final OutputStream out) {

        assert index >= 0 && index < nkeys : "index="+index+" not in [0:"+nkeys+"]";

        final byte[] tmp = keys[index];

        try {
            
            out.write(tmp, 0, tmp.length);
            
        } catch (IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
        return tmp.length;
        
    }

    final public boolean isNull(final int index) {
        
        assert index >= 0 && index < keys.length;
        
        return index >= nkeys;
                
    }
    
    final public boolean isEmpty() {
        
        return nkeys == 0;
        
    }
    
    final public int size() {

        return nkeys;

    }

    /**
     * The maximum #of keys that may be held in the buffer (its capacity).
     */
    final public int capacity() {

        return keys.length;
        
    }

    /**
     * True iff the key buffer can not contain another key.
     */
    final public boolean isFull() {
        
        return nkeys == keys.length;
        
    }
    
    /**
     * Mutable.
     */
    final public boolean isReadOnly() {
        
        return false;
        
    }
    
    /**
     * Instances are searchable and do not allow <code>null</code>s.
     */
    final public boolean isKeys() {

        return true;
        
    }

    /*
     * Mutation api. The contents of individual keys are never modified. Some of
     * the driver logic in Leaf and Node uses loops where nkeys is being
     * decremented while keys are being processed from, e.g., a split point to
     * the last key position. This has the effect that an assert such as
     * 
     * index < nkeys
     * 
     * will fail. Those looping constructs are not wrong as originally written
     * but a move to encapsulate the key buffer puts their treatment of nkeys at
     * odds with index testing since nkeys is temporarily inconsistent with the
     * keys[].
     * 
     * @todo maintain the prefix length. A trivial example of shortening the
     * shared prefix occurs when keys are inserted into the root leaf. Consider
     * that we first insert <code>4,5,6</code>. Since there is only one key
     * in the root leaf, the length of the prefix is the same as the length of
     * the key. If we then insert <code>4,5,6,1</code> the prefix does not
     * change. However, if we then insert <code>4,5,2</code> the prefix is now
     * shortened to <code>4,5</code>. If we insert <code>5</code>, then
     * the prefix is shortened to an empty byte[]. Prefix shortening can also
     * occur in trees with more than one level whenever a key is inserted into a
     * leaf and becomes either the first or last key in that leaf. Likewise, it
     * is possible for the prefix length to either grow when a leaf overflows
     * and keys are redistributed.
     */
    
    /**
     * Set the key at the specified index.
     * 
     * @param index
     *            The index in [0:nkeys-1].
     * @param key
     *            The key (non-null).
     * 
     * @todo Who uses this? Track prefixLength?
     */
    final public void set(final int index, final byte[] key) {
        
        assert index >= 0 && index < nkeys;
        
        keys[index] = key;
        
    }
    
//    /**
//     * Set the key at the specified index to <code>null</code>. This is used
//     * to clear elements of {@link #keys} that are no longer defined. The caller
//     * is responsible for updating {@link #nkeys} when using this method.
//     * 
//     * @param index
//     *            The key index in [0:maxKeys-1];
//     */
//    final public void zeroKey(int index) {
//        
////        assert index >= 0 && index < nkeys;
//        
//        keys[index] = null;
//        
//    }
    
    final public int add(final byte[] key) {
        
        assert nkeys < keys.length;
        
//        assert key != null;
        
        keys[nkeys++] = key;
        
        return nkeys;
        
    }

    final public int add(byte[] key, int off, int len) {
        
        assert nkeys < keys.length;
        
//        assert key != null;
        
        byte[] b = new byte[len];
        
        for(int i=0; i<len; i++) {
            
            b[i] = key[off+i];
            
        }
        
        keys[nkeys++] = b;
        
        return nkeys;
        
    }

    public int add(DataInput in, int len) throws IOException {

        assert nkeys < keys.length;
      
        final byte[] b = new byte[len];
        
        in.readFully(b, 0, len);
        
        keys[nkeys++] = b;
      
        return nkeys;

    }
    
    /**
     * Insert a key into the buffer at the specified index, incrementing the #of
     * keys in the buffer by one and moving down all keys from that index on
     * down by one (towards the end of the array).
     * 
     * @param index
     *            The index in [0:nkeys] (you are allowed to append using this
     *            method).
     * @param key
     *            The key.
     * 
     * @return The #of keys in the buffer.
     * 
     * @todo if index==0 || index==nkeys-1 then update prefixLength, lazily
     *       compute prefix.
     */
    final public int insert(final int index, final byte[] key) {
        
        assert index >= 0 && index <= nkeys;

        if( index == nkeys ) {
            
            // append
            
            return add(key);
            
        }
        
        /* index = 2;
         * nkeys = 6;
         * 
         * [ 0 1 2 3 4 5 ]
         *       ^ index
         * 
         * count = keys - index = 4;
         */
        final int count = nkeys - index;
        
        assert count >= 1;
        
        System.arraycopy(keys, index, keys, index+1, 1);
        
        keys[index] = key;
        
        return ++nkeys;
        
    }
    
    /**
     * Remove a key in the buffer at the specified index, decrementing the #of
     * keys in the buffer by one and moving up all keys from that index on down
     * by one (towards the start of the array).
     * 
     * @param index
     *            The index in [0:nkeys-1].
     * @param key
     *            The key.
     * 
     * @return The #of keys in the buffer.
     * 
     * @todo if index==0 || index==nkeys-1 then update prefixLength, lazily
     *       compute prefix (requires that the application never directly
     *       modifies keys).
     */
    final public int remove(final int index) {

        assert index >= 0 && index < nkeys;
        
        /*
         * Copy down to cover up the hole.
         */
        final int length = nkeys - index - 1;

        if(length > 0) {

            System.arraycopy(keys, index + 1, keys, index, length);
            
        }
        
        keys[--nkeys] = null;
        
        return nkeys;
                
    }
    
    public String toString() {

        return AbstractRaba.toString(this);
        
//        final StringBuilder sb = new StringBuilder();
//
//        sb.append(getClass().getName());
//        sb.append("{ nkeys=" + nkeys);
//        sb.append(", maxKeys=" + keys.length);
//        sb.append(", prefix=" + BytesUtil.toString(getPrefix()));
//        sb.append(", [\n");
//
//        for (int i = 0; i < nkeys/*keys.length*/; i++) {
//
//            if (i > 0)
//                sb.append(",\n");
//
//            final byte[] key = keys[i];
//
//            if (key == null) {
//
//                sb.append("null");
//
//            } else {
//
//                sb.append(BytesUtil.toString(key));
//
//            }
//
//        }
//
//        sb.append("]}");
//
//        return sb.toString();

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
         * The length of the prefix shared by all keys in the buffer.
         */

        final int prefixLength = getPrefixLength();

        /*
         * Attempt to match the shared prefix.  If we can not then return the
         * insert position, which is either before the first key or after the
         * last key in the buffer.
         */
        
        final int insertPosition = _prefixMatchLength(prefixLength, searchKey);
        
        if( insertPosition < 0 ) {
            
            return insertPosition;
            
        }
        
        /*
         * Search keys, but only bytes from prefixLength on in each key.
         */

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
                0, compareLen, keys[0]//
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
    
    final protected int _linearSearch(final int searchKeyOffset,
            final byte[] searchKey) {

        // #of bytes to search in the search key after the prefix match.
        final int searchKeyLen = searchKey.length - searchKeyOffset;

        // searching zero or more bytes in the search key after the prefix match.
        assert searchKeyLen >= 0;
        
        for (int i = 0; i < nkeys; i++) {

            final byte[] key = keys[i];
            
            final int keyLen = key.length - searchKeyOffset;
            assert keyLen >= 0;

            // skip the first offset bytes, then compare no more bytes than
            // remain in the key.
            final int ret = BytesUtil.compareBytesWithLenAndOffset(//
                    searchKeyOffset, keyLen, key,//
                    searchKeyOffset, searchKeyLen, searchKey//
                    );

            if (ret == 0)
                return i;

            if (ret > 0)
                return -(i + 1);

        }

        return -(nkeys + 1);

    }

    final protected int _binarySearch(final int searchKeyOffset,
            final byte[] searchKey) {

        final int searchKeyLen = searchKey.length - searchKeyOffset;
        
        assert searchKeyLen >= 0;

        int low = 0;

        int high = nkeys - 1;

        while (low <= high) {

            final int mid = (low + high) >> 1;

            final byte[] key = keys[mid];

            final int keyLen = key.length - searchKeyOffset;

            assert keyLen >= 0;

            // skip the first offset bytes, then compare no more bytes than
            // remain in the key.
            final int ret = BytesUtil.compareBytesWithLenAndOffset(//
                    searchKeyOffset, keyLen, key,//
                    searchKeyOffset, searchKeyLen, searchKey//
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

    /**
     * Verifies that the keys are in sort order and that undefined keys are
     * [null].
     */
    public final void assertKeysMonotonic() {

        for (int i = 1; i < nkeys; i++) {

            if (BytesUtil.compareBytes(keys[i], keys[i - 1]) <= 0) {

                throw new AssertionError("Keys out of order at index=" + i
                        + ", keys=" + this.toString());

            }

        }

        for (int i = nkeys; i < keys.length; i++) {

            if (keys[i] != null) {

                throw new AssertionError("Expecting null at index=" + i);

            }
            
        }

    }

    /**
     * Computes the length of the prefix by computed by counting the #of leading
     * bytes that match for the first and last key in the buffer.
     */
    public int getPrefixLength() {

        if( nkeys == 0 ) return 0;
        
        if( nkeys == 1 ) return keys[0].length;
        
        return BytesUtil.getPrefixLength(keys[0], keys[nkeys - 1]);
       
    }

    /**
     * Computes the #of leading bytes shared by all keys and returns a new
     * byte[] containing those bytes.
     */
    public byte[] getPrefix() {
        
        if( nkeys == 0 ) return EMPTY_PREFIX;
        
        if( nkeys == 1) return keys[0];
        
        return BytesUtil.getPrefix(keys[0], keys[nkeys-1]);
        
    }
    
    private static final transient byte[] EMPTY_PREFIX = new byte[]{};

}
