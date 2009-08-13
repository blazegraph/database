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
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A flyweight mutable implementation exposing the backing byte[][], permitting
 * <code>null</code>s and not supporting search. It is assumed that caller
 * maintains a dense byte[][] in the sense that all entries in [0:nvalues] are
 * defined, even if some of entries are null. The implementation is NOT
 * thread-safe for mutation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MutableValueBuffer implements IRandomAccessByteArray {

    /**
     * The #of entries with valid data.
     */
    public int nvalues;
    
    /**
     * The backing array.
     */
    public final byte[][] values;

    /**
     * Mutable.
     */
    final public boolean isReadOnly() {
        return false;
    }

    /**
     * <code>null</code>s are allowed.
     */
    public boolean isNullAllowed() {
        return true;
    }

    /**
     * Not searchable.
     */
    final public boolean isSearchable() {
        return false;
    }

    /**
     * Create a view of a byte[][]. All elements in the array are visible in the
     * view.
     * 
     * @param nvalues
     *            The #of entries in the array with valid data.
     * @param values
     *            The backing byte[][].
     */
    public MutableValueBuffer(final int nvalues, final byte[][] values) {

        if (values == null)
            throw new IllegalArgumentException();

        if (nvalues < 0 || nvalues >= values.length)
            throw new IllegalArgumentException();
        
        this.nvalues = nvalues;

        this.values = values;

    }

    /**
     * Builds a mutable values buffer.
     * 
     * @param src
     *            The source data.
     */
    public MutableValueBuffer(final IRandomAccessByteArray src) {

        if (src == null)
            throw new IllegalArgumentException();
        
        nvalues = src.size();

        assert nvalues >= 0; // allows deficient root.

        values = new byte[src.capacity()][];

        int i = 0;
        for (byte[] a : src) {

            values[i++] = a;

        }
        
    }

    final public int size() {

        return nvalues;
        
    }

    final public boolean isEmpty() {
        
        return nvalues == 0;
        
    }
    
    final public boolean isFull() {

        return nvalues == values.length;
        
    }
    
    final public int capacity() {
        
        return values.length;
        
    }

    final protected boolean rangeCheck(final int index)
            throws IndexOutOfBoundsException {

        if (index < 0 || index >= nvalues) {

            throw new IndexOutOfBoundsException("index=" + index
                    + ", capacity=" + nvalues);

        }

        return true;

    }
    
    final public byte[] get(final int index) {

        assert rangeCheck(index);
        
        return values[index];
        
    }

    final public int length(final int index) {

        assert rangeCheck(index);

        final byte[] tmp = values[index];

        if (tmp == null)
            throw new NullPointerException();

        return tmp.length;

    }

    final public boolean isNull(final int index) {

        assert rangeCheck(index);

        return values[index] == null;

    }
    
    final public int copy(final int index, final OutputStream out) {

        assert rangeCheck(index);

        final byte[] tmp = values[index];

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

            int i = 0;

            public boolean hasNext() {

                return i < nvalues;

            }

            public byte[] next() {

                if (!hasNext())
                    throw new NoSuchElementException();

                return values[i++];

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

        if (nvalues >= values.length) {

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

        values[index] = key;
        
    }

    public int add(final byte[] key) {

        assertNotReadOnly();

        assertNotFull();

        assertNullAllowed(key);
        
        values[nvalues++] = key;
        
        return nvalues;
        
    }
    
    public int add(final byte[] key, final int off, final int len) {

        assertNotReadOnly();        

        assertNotFull();

        assertNullAllowed(key);

        final byte[] b = new byte[len];

        System.arraycopy(key, off, b, 0, len);
        
        values[nvalues++] = b;

        return nvalues;

    }
    
    public int add(final DataInput in, final int len) throws IOException {

        assertNotReadOnly();
        
        assertNotFull();

        final byte[] b = new byte[len];

        in.readFully(b, 0, len);

        values[nvalues++] = b;

        return nvalues;

    }

    final public int search(final byte[] searchKey) {

        throw new UnsupportedOperationException();
       
    }
    
    public String toString() {

        return getClass() + "{size=" + nvalues + ",capacity=" + values.length
                + "}";

    }
 
}
