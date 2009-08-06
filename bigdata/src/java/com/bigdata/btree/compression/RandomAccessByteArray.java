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
 * Created on Mar 6, 2008
 */

package com.bigdata.btree.compression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.bigdata.btree.MutableKeyBuffer;
import com.bigdata.btree.proc.IKeyArrayIndexProcedure;

/**
 * Flyweight implementation for wrapping a <code>byte[][]</code> with
 * fromIndex and toIndex.
 * <p>
 * Note: This implementation is used when we split an
 * {@link IKeyArrayIndexProcedure} based on a key-range partitioned index. The
 * {@link MutableKeyBuffer} will not work for this case since it is not aware of
 * a fromIndex and a toIndex.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RandomAccessByteArray implements IRandomAccessByteArray {

    private final int fromIndex;
    private int toIndex;
    private final byte[][] a;

    public RandomAccessByteArray(byte[][] a) {

        this(0, a.length, a);
        
    }

    public RandomAccessByteArray(int fromIndex, int toIndex, byte[][] a) {
        
        assert a != null;
        assert fromIndex >= 0;
        assert fromIndex <= toIndex;
        assert toIndex <= a.length;

        this.fromIndex = fromIndex;
        this.toIndex = toIndex;
        this.a = a;
        
    }
    
    final public int getKeyCount() {

        return (toIndex - fromIndex);
        
    }

    public int getMaxKeys() {
        
        return a.length;
        
    }

    public boolean isReadOnly() {
        
        return false;
        
    }

    final private boolean rangeCheck(int index) {
        
        assert index >= 0 && index < (toIndex - fromIndex) : "index=" + index
                + ", fromIndex=" + fromIndex + ", toIndex=" + toIndex;

        return true;
        
    }
    
    public void setKey(int index, byte[] key) {
        
        assert rangeCheck(index);
        
        a[fromIndex + index] = key;
        
    }

    public byte[] getKey(int index) {

        assert rangeCheck(index);
        
        return a[fromIndex + index];
        
    }

    public int getLength(int index) {
        
        assert rangeCheck(index);
        
        byte[] tmp = a[fromIndex + index];

        if(tmp==null) throw new NullPointerException();
        
        return tmp.length;
        
    }
    
    public boolean isNull(final int index) {

        assert rangeCheck(index);

        return a[fromIndex + index] == null;
        
    }
    
    public int copyKey(int index, DataOutput out) throws IOException {
        
        assert rangeCheck(index);

        byte[] tmp = a[fromIndex + index];

        if (tmp == null)
            throw new NullPointerException();
        
        out.write(tmp, 0, tmp.length);
        
        return tmp.length;
        
    }
    
    public int add(byte[] key) {
        
        assert toIndex < a.length;

        a[toIndex++] = key;
        
        return (toIndex - fromIndex);
        
    }
    
    public int add(byte[] key, int off, int len) {
        
        assert toIndex < a.length;

        byte[] b = new byte[len];
        
        for(int i=0; i<len; i++) {
            
            b[i] = key[off+i];
            
        }
        
        a[toIndex++] = b;
        
        return (toIndex - fromIndex);
        
    }
    
    public int add(DataInput in, int len) throws IOException {

        assert toIndex < a.length;

        final byte[] b = new byte[len];
        
        in.readFully(b, 0, len);
        
        a[toIndex++] = b;
        
        return (toIndex - fromIndex);

    }
    
    public Iterator<byte[]> iterator() {
        
        return new Iterator<byte[]>() {

            int i = fromIndex;
            
            public boolean hasNext() {
                
                return i < toIndex;
                
            }

            public byte[] next() {

                if (!hasNext())
                    throw new NoSuchElementException();

                return a[ i++ ];
                
            }

            public void remove() {

                throw new UnsupportedOperationException();
                
            }
            
        };

    }

    /**
     * Resize the buffer, copying the references to the existing data into the
     * new buffer.
     * <p>
     * Note: the new buffer will be dense (fromIndex will be zero).
     * 
     * @param n
     *            The size of the new buffer.
     * 
     * @return The new buffer.
     */
    public RandomAccessByteArray resize(int n) {
        
        if (n < 0)
            throw new IllegalArgumentException();

        // #of entries in the source.
        final int m = getKeyCount();

        // #of entries to be copied into the new buffer.
        final int p = Math.min(m, n);
        
        // new backing array sized to [n].
        final byte[][] b = new byte[n][];

        // copy references to the new buffer.
        System.arraycopy(a, fromIndex, b, 0, p);
        
        return new RandomAccessByteArray(0,p,a);
        
    }

}
