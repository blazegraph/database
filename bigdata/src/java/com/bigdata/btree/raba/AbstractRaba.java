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
 * Created on Aug 11, 2009
 */

package com.bigdata.btree.raba;

import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.bigdata.btree.BytesUtil;

/**
 * Abstract base class implements mutation operators and search. A concrete
 * subclass need only indicate if it is mutable, searchable, or allows nulls
 * by overriding the appropriate methods.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRaba implements IRandomAccessByteArray {

    /**
     * The inclusive lower bound of the view.
     */
    protected final int fromIndex;

    /**
     * The exclusive upper bound of the view.
     * <p>
     * Note: This field is NOT final since it is modified by a subclass which
     * permits mutation.
     */
    protected int toIndex;

    /**
     * The maximum #of elements in the view of the backing array.
     */
    protected final int capacity;

    /**
     * The backing array.
     */
    protected final byte[][] a;

    /**
     * Create a view of a byte[][]. All elements in the array are visible in the
     * view.
     * 
     * @param a
     *            The backing byte[][].
     */
    public AbstractRaba(final byte[][] a) {

        this(0/* fromIndex */, a.length/* toIndex */, a.length/* capacity */, a);

    }

    /**
     * Create a view from a slice of a byte[][].
     * 
     * @param fromIndex
     *            The index of the first element in the byte[][] which is
     *            visible in the view (inclusive lower bound).
     * @param toIndex
     *            The index of the first element in the byte[][] beyond the view
     *            (exclusive upper bound).
     * @param capacity
     *            The #of elements which may be used in the view.
     * @param a
     *            The backing byte[][].
     */
    public AbstractRaba(final int fromIndex, final int toIndex,
            final int capacity, final byte[][] a) {

        if (a == null)
            throw new IllegalArgumentException();

        if (fromIndex < 0)
            throw new IllegalArgumentException();

        if (fromIndex > toIndex)
            throw new IllegalArgumentException();

        if (toIndex > a.length)
            throw new IllegalArgumentException();

        if (capacity < toIndex - fromIndex)
            throw new IllegalArgumentException();

        this.fromIndex = fromIndex;

        this.toIndex = toIndex;
        
        this.capacity = capacity;
        
        this.a = a;
        
    }
    
    final public int size() {

        return (toIndex - fromIndex);
        
    }

    final public boolean isEmpty() {
        
        return toIndex == fromIndex;
        
    }
    
    final public boolean isFull() {

        return size() == capacity();
        
    }
    
    final public int capacity() {
        
        return capacity;
        
    }

    final protected boolean rangeCheck(final int index)
            throws IndexOutOfBoundsException {

        if (index < 0 || index >= (toIndex - fromIndex)) {

            throw new IndexOutOfBoundsException("index=" + index
                    + ", fromIndex=" + fromIndex + ", toIndex=" + toIndex);

        }

        return true;

    }
    
    final public byte[] get(final int index) {

        assert rangeCheck(index);
        
        return a[fromIndex + index];
        
    }

    final public int length(final int index) {

        assert rangeCheck(index);

        final byte[] tmp = a[fromIndex + index];

        if (tmp == null)
            throw new NullPointerException();

        return tmp.length;

    }

    final public boolean isNull(final int index) {

        assert rangeCheck(index);

        return a[fromIndex + index] == null;

    }
    
    final public int copy(final int index, final OutputStream out) {

        assert rangeCheck(index);

        final byte[] tmp = a[fromIndex + index];

        if (tmp == null)
            throw new NullPointerException();

        try {

            out.write(tmp, 0, tmp.length);
            
        } catch (IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
        return tmp.length;
        
    }
    
    final public Iterator<byte[]> iterator() {

        return new Iterator<byte[]>() {

            int i = fromIndex;

            public boolean hasNext() {

                return i < toIndex;

            }

            public byte[] next() {

                if (!hasNext())
                    throw new NoSuchElementException();

                return a[i++];

            }

            public void remove() {

                if (isReadOnly())
                    throw new UnsupportedOperationException();

                // @todo support remove on the iterator when mutable.
               throw new UnsupportedOperationException();
                
            }

        };

    }
    
    /**
     * @throws UnsupportedOperationException
     *             if the view is read-only.
     */
    protected void assertNotReadOnly() {
        
        if(isReadOnly())
            throw new UnsupportedOperationException();
        
    }

    /**
     * @throws IllegalStateException
     *             unless there is room to store another value.
     */
    protected void assertNotFull() {

        if(toIndex >= fromIndex + a.length) {
            
            throw new IllegalStateException();
            
        }

    }
    
    /**
     * @throws IllegalArgumentException
     *             if the <i>key</i> is <code>null</code> and the implementation
     *             does not permit <code>null</code>s to be stored.
     */
    protected void assertNullAllowed(final byte[] key) {
        
        if (key == null && !isNullAllowed()) {

            throw new IllegalArgumentException();
            
        }
        
    }
    
    public void set(final int index, final byte[] key) {
        
        assertNotReadOnly();
        
        assert rangeCheck(index);

        assertNullAllowed(key);

        a[fromIndex + index] = key;
        
    }

    public int add(final byte[] key) {

        assertNotReadOnly();

        assertNotFull();

        assertNullAllowed(key);
        
        assert toIndex < fromIndex + capacity;

        a[toIndex++] = key;
        
        return (toIndex - fromIndex);
        
    }
    
    public int add(final byte[] key, final int off, final int len) {

        assertNotReadOnly();        

        assertNotFull();

        assertNullAllowed(key);

        final byte[] b = new byte[len];

        System.arraycopy(key, off, b, 0, len);
        
//        for (int i = 0; i < len; i++) {
//
//            b[i] = key[off + i];
//
//        }

        a[toIndex++] = b;

        return (toIndex - fromIndex);

    }
    
    public int add(final DataInput in, final int len) throws IOException {

        assertNotReadOnly();
        
        assertNotFull();

        final byte[] b = new byte[len];

        in.readFully(b, 0, len);

        a[toIndex++] = b;

        return (toIndex - fromIndex);

    }

    public int search(final byte[] searchKey) {

        if (!isSearchable())
            throw new UnsupportedOperationException();

        if (isNullAllowed()) {
         
            // implementations that allow search must not allow nulls.
            throw new AssertionError();
            
        }

        return BytesUtil.binarySearch(a, 0/* base */, size()/* nmem */,
                searchKey);
        
    }
    
    public String toString() {

        return getClass() + "{size=" + size() + ",capacity=" + capacity
                + ",readOnly=" + isReadOnly() + "}";
        
    }

}
