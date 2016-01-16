/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.htree.raba;

import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import com.bigdata.btree.raba.AbstractRaba;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.htree.HTree;

/**
 * A flyweight mutable implementation for an {@link HTree} bucket page using a
 * backing <code>byte[][]</code>. Unlike the values in a B+Tree, the
 * {@link HTree} values need not be dense. Further, each bucket page is
 * logically divided into a set of buddy hash buckets. All operations therefore
 * take place within a buddy bucket. The buddy bucket is identified by its
 * offset and its extent is identified by the global depth of the bucket page.
 * <p>
 * Note: Because the slots are divided logically among the buddy buckets any
 * slot may have a non-<code>null</code> value and the {@link IRaba} methods as
 * implemented by this class DO NOT range check the index against
 * {@link #size()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Test suite. This should be pretty much a clone of the test
 *          suite for the {@link MutableKeyBuffer} in this package.
 */
public class MutableValueBuffer implements IRaba {

    /**
     * The #of defined values across the entire bucket page. 
     */
    public int nvalues;

    /**
     * An array containing the values. The size of the array is the maximum
     * capacity of the value buffer, which is <code>2^addressBits</code>.
     */
    public final byte[][] values;

    /**
     * Must be <code>2^n</code> where <code>n</code> GT ZERO (0).
     */
    private static void checkCapacity(final int capacity) {
     
        if (capacity <= 1 || (capacity & -capacity) != capacity)
            throw new IllegalArgumentException();

    }
    
    /**
     * Allocate a mutable value buffer capable of storing <i>capacity</i>
     * values.
     * 
     * @param capacity
     *            The capacity of the value buffer.
     */
    public MutableValueBuffer(final int capacity) {

        checkCapacity(capacity);

        nvalues = 0;
        
        values = new byte[capacity][];
        
    }

    /**
     * Constructor wraps an existing byte[][].
     * 
     * @param nvalues
     *            The #of defined values in the array.
     * @param values
     *            The array of values.
     */
    public MutableValueBuffer(final int nvalues, final byte[][] values) {

        if (values == null)
            throw new IllegalArgumentException();

        if (nvalues < 0 || nvalues > values.length)
            throw new IllegalArgumentException();

        checkCapacity(values.length);

        this.nvalues = nvalues;

        this.values = values;

    }

    /**
     * Creates a new instance using a new array of values but sharing the value
     * references with the provided {@link MutableValueBuffer}.
     * 
     * @param src
     *            An existing instance.
     */
    public MutableValueBuffer(final MutableValueBuffer src) {

        if(src == null)
            throw new IllegalArgumentException();
        
        checkCapacity(src.capacity());

        this.nvalues = src.nvalues;

        // note: dimension to the capacity of the source.
        this.values = new byte[src.values.length][];

        // copy the values.
        for (int i = 0; i < values.length; i++) {

            // Note: copies the reference.
            this.values[i] = src.values[i];

        }

    }

    /**
     * Builds a mutable value buffer.
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
    public MutableValueBuffer(final int capacity, final IRaba src) {

        if (src == null)
            throw new IllegalArgumentException();

        checkCapacity(capacity);
        
        if (capacity < src.capacity())
            throw new IllegalArgumentException();
        
        nvalues = src.size();

        values = new byte[capacity][];

        int i = 0;
        for (byte[] a : src) {

            values[i++] = a;

        }
        
    }

    @Override
    public String toString() {

        return AbstractRaba.toString(this);
        
    }

    /**
     * Returns a reference to the value at that index.
     */
    @Override
    final public byte[] get(final int index) {

        return values[index];

    }

    @Override
    final public int length(final int index) {

        final byte[] tmp = values[index];

        if (tmp == null)
            throw new NullPointerException();

        return tmp.length;

    }
    
    @Override
    final public int copy(final int index, final OutputStream out) {

        final byte[] tmp = values[index];

        try {
            
            out.write(tmp, 0, tmp.length);
            
        } catch (IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
        return tmp.length;
        
    }

    /**
     * {@inheritDoc}
     * 
     * @return <code>true</code> iff the value at that index is <code>null</code>.
     */
    @Override
    final public boolean isNull(final int index) {
        
        return values[index] == null;
                
    }
    
    @Override
    final public boolean isEmpty() {
        
        return nvalues == 0;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: This is the #of values in the bucket page (across all buddy buckets
     * on that page). Unless there is only one buddy bucket on the page, you
     * MUST explicitly scan a buddy bucket to determine the #of values in a
     * buddy bucket on the page.
     */
    @Override
    final public int size() {

        return nvalues;

    }

    @Override
    final public int capacity() {

        return values.length;
        
    }

    @Override
    final public boolean isFull() {
        
        return nvalues == values.length;
        
    }
    
    /**
     * Mutable.
     */
    @Override
    final public boolean isReadOnly() {
        
        return false;
        
    }

    /**
     * Instances are NOT searchable. Duplicates and <code>null</code>s ARE
     * permitted.
     * 
     * @returns <code>false</code>
     */
    @Override
    final public boolean isKeys() {

        return false;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * This iterator visits all values on the bucket page, including
     * <code>null</code>s.
     */
    @Override
    public Iterator<byte[]> iterator() {

        return new Iterator<byte[]>() {

            int i = 0;
            
            @Override
            public boolean hasNext() {
                return i < size();
            }

            @Override
            public byte[] next() {
                return get(i++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
            
        };

    }

    /*
     * Mutation api. The contents of individual values are never modified. 
     */
    
    @Override
    final public void set(final int index, final byte[] value) {

        assert value != null;
        assert values[index] == null;
        assert nvalues < values.length;
        
        values[index] = value;
        
        nvalues++;
        
    }
    
    final public void insert(final int index, final byte[] value) {

        if (index < nvalues) {
        	// shift "upper" keys - check there is room!
        	assert values[nvalues] == null;
        	System.arraycopy(values, index, values, index+1, nvalues-index);
        }
        
        values[index] = value;
        
        nvalues++;
    }

    /**
     * Remove a value in the buffer at the specified index, decrementing the #of
     * value in the buffer by one.
     * 
     * @param index
     *            The index in [0:{@link #capacity()}-1].
     * 
     * @return The #of values in the buffer.
     */
    final public int remove(final int index) {

        assert values[index] != null;
        assert nvalues > 0;
        
		System.arraycopy(values, index+1, values, index, nvalues-index-1);

		values[nvalues-1] = null;
        
        
        return --nvalues;
      
    }
    
    /**
     * This method is not supported. Values must be inserted into a specific buddy
     * bucket. This requires the caller to specify the index at which the value
     * will be stored using {@link #set(int, byte[])}.
     * 
     * @throws UnsupportedOperationException
     */
    @Override
    final public int add(final byte[] value) {

        throw new UnsupportedOperationException();

    }

    /**
     * This method is not supported. Values must be inserted into a specific buddy
     * bucket. This requires the caller to specify the index at which the value
     * will be stored using {@link #set(int, byte[])}.
     * 
     * @throws UnsupportedOperationException
     */
    @Override
    final public int add(byte[] value, int off, int len) {

        throw new UnsupportedOperationException();

    }

    /**
     * This method is not supported. Values must be inserted into a specific buddy
     * bucket. This requires the caller to specify the index at which the value
     * will be stored using {@link #set(int, byte[])}.
     * 
     * @throws UnsupportedOperationException
     */
    @Override
    public int add(DataInput in, int len) throws IOException {

        throw new UnsupportedOperationException();

    }

    /**
     * This method is not supported. 
     * 
     * @throws UnsupportedOperationException
     */
    @Override
    public int search(byte[] searchKey) {
        
        throw new UnsupportedOperationException();
        
    }

}
