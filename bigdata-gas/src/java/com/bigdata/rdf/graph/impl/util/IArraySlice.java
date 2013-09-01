/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.graph.impl.util;

/**
 * Interface for a slice of a backing array.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: IByteArraySlice.java 4548 2011-05-25 19:36:34Z thompsonbry $
 */
public interface IArraySlice<T> extends Iterable<T> {

    /**
     * The backing array. This method DOES NOT guarantee that the backing array
     * reference will remain constant. Some implementations use an extensible
     * backing array and will replace the reference when the backing buffer is
     * extended.
     */
    T[] array();

    /**
     * The start of the slice in the {@link #array()}.
     */
    int off();

    /**
     * The length of the slice in the {@link #array()}.
     */
    int len();

    /**
     * Return a copy of the data in the slice.
     * 
     * @return A new array containing data in the slice.
     */
    T[] toArray();

    /**
     * Return a slice of the backing buffer. The slice will always reference the
     * current backing {@link #array()}, even when the buffer is extended and
     * the array reference is replaced.
     * 
     * @param off
     *            The starting offset into the backing buffer of the slice.
     * @param len
     *            The length of that slice.
     * 
     * @return The slice.
     */
    IArraySlice<T> slice(final int off, final int len);

    /**
     * Absolute put of a value at an index.
     * 
     * @param pos
     *            The index.
     * @param v
     *            The value.
     */
    void put(int pos, T v);

    /**
     * Absolute get of a value at an index.
     * 
     * @param pos
     *            The index.
     *            
     * @return The value.
     */
    T get(int pos);
    
    /**
     * Absolute bulk <i>put</i> copies all values in the caller's array into
     * this buffer starting at the specified position within the slice defined
     * by this buffer.
     * 
     * @param pos
     *            The starting position within the slice defined by this buffer.
     * @param src
     *            The source data.
     */
    void put(int pos, T[] src);

    /**
     * Absolute bulk <i>put</i> copies the specified slice of values
     * from the caller's array into this buffer starting at the specified
     * position within the slice defined by this buffer.
     * 
     * @param dstoff
     *            The offset into the slice to which the data will be copied.
     * @param src
     *            The source data.
     * @param srcoff
     *            The offset of the 1st value in the source data to
     *            be copied.
     * @param srclen
     *            The #of values to be copied.
     */
    void put(int dstoff, T[] src, int srcoff, int srclen);

    /**
     * Absolute bulk <i>get</i> copies <code>dst.length</code> values
     * from the specified offset into the slice defined by this buffer into the
     * caller's array.
     * 
     * @param srcoff
     *            The offset into the slice of the first value to be copied.
     * @param dst
     *            The array into which the data will be copied.
     */
    void get(final int srcoff, final T[] dst);

    /**
     * Absolute bulk <i>get</i> copies the specified slice of values
     * from this buffer into the specified slice of the caller's array.
     * 
     * @param srcoff
     *            The offset into the slice defined by this buffer of the first
     *            value to be copied.
     * @param dst
     *            The array into which the data will be copied.
     * @param dstoff
     *            The offset of the first value in that array onto
     *            which the data will be copied.
     * @param dstlen
     *            The #of values to be copied.
     */
    void get(final int srcoff, final T[] dst, final int dstoff, final int dstlen);

}
