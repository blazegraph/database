/*
 * fastutil: Fast & compact type-specific collections for Java
 *
 * Copyright (C) 2002, 2003, 2004, 2005, 2006 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

package it.unimi.dsi.fastutil.bytes;


import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.objects.AbstractObjectList;
import it.unimi.dsi.fastutil.objects.AbstractObjectListIterator;
import it.unimi.dsi.fastutil.objects.ObjectListIterator;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.bigdata.btree.BytesUtil;
import com.bigdata.io.ByteArrayBuffer;

/**
 * Compact storage of lists of arrays using front coding.
 * 
 * <P>
 * This class stores immutably a list of arrays in a single large array using
 * front coding (of course, the compression will be reasonable only if the list
 * is sorted lexicographically&mdash;see below). It implements an immutable
 * type-specific list that returns the <var>i</var>-th array when calling
 * {@link #get(int) get(<var>i</var>)}. The returned array may be freely
 * modified.
 * 
 * <P>
 * Front coding is based on the idea that if the <var>i</var>-th and the
 * (<var>i</var>+1)-th array have a common prefix, we might store the length of
 * the common prefix, and then the rest of the second array.
 * 
 * <P>
 * This approach, of course, requires that once in a while an array is stored
 * entirely. The <def>ratio</def> of a front-coded list defines how often this
 * happens (once every {@link #ratio()} arrays). A higher ratio means more
 * compression, but means also a longer access time, as more arrays have to be
 * probed to build the result. Note that we must build an array every time
 * {@link #get(int)} is called, but this class provides also methods that
 * extract one of the stored arrays in a given array, reducing garbage
 * collection. See the documentation of the family of <code>get()</code>
 * methods.
 * 
 * <P>
 * By setting the ratio to 1 we actually disable front coding: however, we still
 * have a data structure storing large list of arrays with a reduced overhead
 * (just one integer per array, plus the space required for lengths).
 * 
 * <P>
 * Note that the typical usage of front-coded lists is under the form of
 * serialized objects; usually, the data that has to be compacted is processed
 * offline, and the resulting structure is stored permanently. Since the pointer
 * array is not stored, the serialized format is very small.
 * 
 * <H2>Implementation Details</H2>
 * 
 * <P>
 * All arrays are stored in a large array. A separate array of pointers indexes
 * arrays whose position is a multiple of the ratio: thus, a higher ratio means
 * also less pointers.
 * 
 * <P>
 * More in detail, an array whose position is a multiple of the ratio is stored
 * as the array length, followed by the elements of the array. The array length
 * is coded by a simple variable-length list of <var>k</var>-1 bit blocks, where
 * <var>k</var> is the number of bits of the underlying primitive type. All
 * other arrays are stored as follows: let <code>common</code> the length of the
 * maximum common prefix between the array and its predecessor. Then we store
 * the array length decremented by <code>common</code>, followed by
 * <code>common</code>, followed by the array elements whose index is greater
 * than or equal to <code>common</code>. For instance, if we store
 * <samp>foo</samp>, <samp>foobar</samp>, <samp>football</samp> and
 * <samp>fool</samp> in a front-coded character-array list with ratio 3, the
 * character array will contain
 * 
 * <pre>
 * &lt;b&gt;3&lt;/b&gt; f o o &lt;b&gt;3&lt;/b&gt; &lt;b&gt;3&lt;/b&gt; b a r &lt;b&gt;5&lt;/b&gt; &lt;b&gt;3&lt;/b&gt; t b a l l &lt;b&gt;4&lt;/b&gt; f o o l
 * </pre>
 * 
 * <H2>Limitations</H2>
 * 
 * <P>
 * All arrays are stored in a large array: thus, the compressed list must not
 * exceed {@link java.lang.Integer#MAX_VALUE} elements. Moreover, iterators are
 * less efficient when they move back, as
 * {@link java.util.ListIterator#previous() previous()} cannot build
 * incrementally the previous array (whereas (
 * {@link java.util.ListIterator#next() next()} can).
 * 
 * <h3>Modifications</h3>
 * 
 * This class was derived from
 * <code>it.unimi.dsi.fastutil.bytes.ArrayFrontCodedList</code>, which is part
 * of fastutils. The folowing changes were made:
 * <ul>
 * <li>The name of the class has been changed to prevent classpath problems.</li>
 * <li>The class has a new {@link #serialVersionUID} and the serialization logic
 * has been modified to allow serialization against {@link DataOutput} by
 * defining {@link #getArray()} and a new ctor which accepts the backing array.</li>
 * <li>The test code from main() has been isolated in a junit test suite.</li>
 * <li>The backing <code>byte[] array</code> has been replaced by an interface
 * suitable for wrapping either a <code>byte[]</code> or a {@link ByteBuffer}.
 * This was done in order to permit access to the front-coded representation
 * without "de-serializing" the data and a suitable constructor was added for
 * the {@link ByteBuffer} case.</li>
 * <li>Make the {@link Collection} and {@link Iterator} ctors strongly typed.</li>
 * </ul>
 */

public class CustomByteArrayFrontCodedList extends AbstractObjectList<byte[]>
        implements Serializable, Cloneable {

    /*
     * New interfaces and their implementations.
     */
            
    /**
     * Abstraction allowing different implementations of the backing buffer.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public interface BackingBuffer extends Cloneable, Serializable {
        /**
         * Return the byte value at the specified index.
         * 
         * @param i
         *            The index.
         * @return The byte.
         */
        public byte get(int i);

        /**
         * Reads a coded length.
         * 
         * @param pos
         *            The starting position.
         *            
         * @return The length coded at <code>pos</code>.
         */
        public int readInt(int pos);

        /**
         * Copy data from the backing buffer into the caller's array.
         * 
         * @param pos
         *            The starting position in the backing buffer.
         * @param dest
         *            The caller's array.
         * @param destPos
         *            The starting position in the caller's array.
         * @param len
         *            The #of bytes to copy.
         */
        public void arraycopy(int pos, byte[] dest, int destPos, int len);

        /**
         * The size of the backing buffer in bytes.
         */
        public int size();
        
        /**
         * Return a copy of the data in the backing buffer.
        */
        public byte[] toArray();

        /**
         * Write the data on the output stream.
         * 
         * @param out
         *            The output stream.
         * 
         * @return The #of bytes written.
         */
        public int writeOn(OutputStream out) throws IOException;

        /**
         * Write <i>len</i> bytes starting at <i>off</i> onto the caller's
         * stream.
         * 
         * @param out
         *            The output stream.
         * @param off
         *            The index of the first byte to be written.
         * @param len
         *            The #of bytes to be written.
         * 
         * @return The #of bytes written.
         */
        public int writeOn(OutputStream out, int off, int len) throws IOException;

        /**
         * Clone the backing buffer.
         */
        public BackingBuffer clone();
        
    }

    /**
     * Implementation for a <code>byte[]</code>.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class BackingByteArray implements BackingBuffer {
        
        private static final long serialVersionUID = 1L;
        
        private final byte[] a;
        private final int off;
        private final int len;
        
        public BackingByteArray(final byte[] a) {
            this(a,0,a.length);
        }

        public BackingByteArray(final byte[] a, final int off, final int len) {
            this.a = a;
            this.off = off;
            this.len = len;
        }
    
        public int size() {
            return len;
        }
        
        public byte get(final int i) {
         
            return a[off + i];
            
        }
        
        public void arraycopy(final int pos, final byte[] dest,
                final int destPos, final int len) {

            if (pos < 0) // check starting pos.
                throw new IllegalArgumentException();
            
            if (pos + len > this.len) // check run length.
                throw new IllegalArgumentException();
            
            System.arraycopy(a/* src */, off + pos, dest, destPos, len);

        }
        
        public int writeOn(final OutputStream dos) throws IOException {

            dos.write(a, off, len);
            
            return len;
            
        }
        
        public int writeOn(final OutputStream dos, final int aoff,
                final int alen) throws IOException {

            if (aoff < 0) // check starting pos.
                throw new IllegalArgumentException();
            
            if (aoff + alen > this.len) // check run length.
                throw new IllegalArgumentException();

            dos.write(a, off + aoff, alen);

            return len;
            
        }
        
        public int readInt(int pos) {
            pos += off;
            if (a[pos] >= 0)
                return a[pos];
            if (a[pos + 1] >= 0)
                return (-a[pos] - 1) << 7 | a[pos + 1];
            if (a[pos + 2] >= 0)
                return (-a[pos] - 1) << 14 | (-a[pos + 1] - 1) << 7 | a[pos + 2];
            if (a[pos + 3] >= 0)
                return (-a[pos] - 1) << 21 | (-a[pos + 1] - 1) << 14
                        | (-a[pos + 2] - 1) << 7 | a[pos + 3];
            return (-a[pos] - 1) << 28 | (-a[pos + 1] - 1) << 21
                    | (-a[pos + 2] - 1) << 14 | (-a[pos + 3] - 1) << 7 | a[pos + 4];
        }

        public byte[] toArray() {

            final byte[] b = new byte[len];

            System.arraycopy(a, off, b, 0, len);

            return b;

        }
        
        public BackingByteArray clone() {
            
            return new BackingByteArray(toArray());
            
        }

    }

    /**
     * Implementation with a backing {@link ByteBuffer}.
     * <p>
     * Note: Methods which interact with a ByteBuffer MUST NOT change its
     * position or limit. If they do then ALL methods which touch the buffer
     * need to be synchronized so NONE of them can have a concurrent read during
     * which the position/limit has been transiently modified. The culprits here
     * are the bulk byte transfer methods ByteBuffer#get() and ByteBuffer#put().
     * This is really a huge limitation on the use of a ByteBuffer for
     * concurrent access to a read-only data structure.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * 
     * @deprecated The {@link ByteBuffer} is too slow.
     */
    private static class BackingByteBuffer implements BackingBuffer {

        private static final long serialVersionUID = 1L;
        
        private final ByteBuffer b;

        /**
         * 
         * @param b
         *            The data. All bytes in view are used (from zero through
         *            the capacity of the array). The limit and position of the
         *            buffer are ignored.
         */
        public BackingByteBuffer(final ByteBuffer b) {
            this.b = b;
        }
        
        public int size() {
            return b.capacity();
        }
        
        public byte get(final int i) {
            synchronized(b) {
                return b.get(i);
            }
        }

        // @todo tweak by extracting values that are reused into tmp vars.
        public int readInt(final int pos) {
            synchronized(b) {
            if (get(pos) >= 0)
                return get(pos);
            if (get(pos + 1) >= 0)
                return (-get(pos) - 1) << 7 | get(pos + 1);
            if (get(pos + 2) >= 0)
                return (-get(pos) - 1) << 14 | (-get(pos + 1) - 1) << 7 | get(pos + 2);
            if (get(pos + 3) >= 0)
                return (-get(pos) - 1) << 21 | (-get(pos + 1) - 1) << 14
                        | (-get(pos + 2) - 1) << 7 | get(pos + 3);
            return (-get(pos) - 1) << 28 | (-get(pos + 1) - 1) << 21
                    | (-get(pos + 2) - 1) << 14 | (-get(pos + 3) - 1) << 7 | get(pos + 4);
            }
        }

        public byte[] toArray() {
            /*
             * Note: synchronized to prevent concurrent modification to the
             * pos/limit. The pos/limit are restored as a postcondition using
             * clear().
             */
            synchronized (b) {
                final byte[] a = new byte[b.capacity()];
                b.clear();
                b.get(a);
                b.clear();
                return a;
            }
        }
        
        public void arraycopy(final int pos, final byte[] dest,
                final int destPos, final int len) {
            /*
             * Note: synchronized to prevent concurrent modification to the
             * pos/limit. The pos/limit are restored as a postcondition using
             * clear().
             */
            synchronized (b) {
                b.limit(pos + len);
                b.position(pos);
                b.get(dest, destPos, len);
                b.clear();
            }
        }

        public int writeOn(final OutputStream dos) throws IOException {

            final byte[] a = toArray();
            
            dos.write(a, 0/* off */, a.length/* len */);

            return a.length;
            
        }

        public int writeOn(final OutputStream dos, final int off, final int len)
                throws IOException {

            final byte[] a = new byte[len];

            arraycopy(off, a, 0/*destPos*/, len);
            
            dos.write(a, 0/* off */, a.length/* len */);

            return a.length;
            
        }

        public BackingByteBuffer clone() {

            return new BackingByteBuffer(ByteBuffer.wrap(toArray()));
            
        }

    }
    
    /**
     * 
     */
    private static final long serialVersionUID = -2532468860579334765L;

    // The value for the original impl.
    // public static final long serialVersionUID = -7046029254386353130L;

    /** The number of arrays in the list. */
    protected int n;

    /** The ratio of this front-coded list. */
    protected int ratio;

//    /** The array containing the compressed arrays. */
//    protected byte[] array;
    /** A view on the compressed arrays. */
    private BackingBuffer bb;

    /** The pointers to entire arrays in the list. */
    transient protected int[] p;

    private void assertRatio(final int ratio) {

        if (ratio < 1)
            throw new IllegalArgumentException("Illegal ratio (" + ratio + ")");

    }

    /**
     * Creates a new front-coded list containing the arrays returned by the
     * given iterator.
     * 
     * @param arrays
     *            an iterator returning arrays.
     * @param ratio
     *            the desired ratio.
     */

    public CustomByteArrayFrontCodedList(final Iterator<byte[]> arrays, final int ratio) {

        assertRatio(ratio);
//        if (ratio < 1)
//            throw new IllegalArgumentException("Illegal ratio (" + ratio + ")");

        byte[] array = ByteArrays.EMPTY_ARRAY;
        int[] p = IntArrays.EMPTY_ARRAY;

        byte[][] a = new byte[2][];
        int curSize = 0, b = 0, common, length, minLength;

        while (arrays.hasNext()) {
            a[b] = (byte[]) arrays.next();
            length = a[b].length;

            if (n % ratio == 0) {
                p = IntArrays.grow(p, n / ratio + 1);
                p[n / ratio] = curSize;

                array = ByteArrays.grow(array,
                        curSize + count(length) + length, curSize);
                curSize += writeInt(array, length, curSize);
                System.arraycopy(a[b], 0, array, curSize, length);
                curSize += length;
            } else {
                minLength = a[1 - b].length;
                if (length < minLength)
                    minLength = length;
                for (common = 0; common < minLength; common++)
                    if (a[0][common] != a[1][common])
                        break;
                length -= common;

                array = ByteArrays.grow(array, curSize + count(length)
                        + count(common) + length, curSize);
                curSize += writeInt(array, length, curSize);
                curSize += writeInt(array, common, curSize);
                System.arraycopy(a[b], common, array, curSize, length);
                curSize += length;
            }

            b = 1 - b;
            n++;
        }

        this.ratio = ratio;
//      this.array = ByteArrays.trim(array, curSize);
        this.bb = new BackingByteArray( ByteArrays.trim(array, curSize) );
//        this.bb = new BackingByteBuffer( ByteBuffer.wrap(ByteArrays.trim(array, curSize) ));
        this.p = IntArrays.trim(p, (n + ratio - 1) / ratio);

    }

    /**
     * Creates a new front-coded list containing the arrays in the given
     * collection.
     * 
     * @param c
     *            a collection containing arrays.
     * @param ratio
     *            the desired ratio.
     */

    public CustomByteArrayFrontCodedList(final Collection<byte[]> c, final int ratio) {
        this(c.iterator(), ratio);
    }

//    /**
//     * Reads a coded length.
//     * 
//     * @param a
//     *            the data array.
//     * @param pos
//     *            the starting position.
//     * @return the length coded at <code>pos</code>.
//     */
//    private static int readInt(final byte a[], int pos) {
//        if (a[pos] >= 0)
//            return a[pos];
//        if (a[pos + 1] >= 0)
//            return (-a[pos] - 1) << 7 | a[pos + 1];
//        if (a[pos + 2] >= 0)
//            return (-a[pos] - 1) << 14 | (-a[pos + 1] - 1) << 7 | a[pos + 2];
//        if (a[pos + 3] >= 0)
//            return (-a[pos] - 1) << 21 | (-a[pos + 1] - 1) << 14
//                    | (-a[pos + 2] - 1) << 7 | a[pos + 3];
//        return (-a[pos] - 1) << 28 | (-a[pos + 1] - 1) << 21
//                | (-a[pos + 2] - 1) << 14 | (-a[pos + 3] - 1) << 7 | a[pos + 4];
//    }

    /**
     * Computes the number of elements coding a given length.
     * 
     * @param length
     *            the length to be coded.
     * @return the number of elements coding <code>length</code>.
     */
//    @SuppressWarnings("unused")
    private static int count(final int length) {
        if (length < (1 << 7))
            return 1;
        if (length < (1 << 14))
            return 2;
        if (length < (1 << 21))
            return 3;
        if (length < (1 << 28))
            return 4;
        return 5;
    }

    /**
     * Writes a length.
     * 
     * @param a
     *            the data array.
     * @param length
     *            the length to be written.
     * @param pos
     *            the starting position.
     * @return the number of elements coding <code>length</code>.
     */
    private static int writeInt(final byte a[], int length, int pos) {
        final int count = count(length);
        a[pos + count - 1] = (byte) (length & 0x7F);

        if (count != 1) {
            int i = count - 1;
            while (i-- != 0) {
                length >>>= 7;
                a[pos + i] = (byte) (-(length & 0x7F) - 1);
            }
        }

        return count;
    }

    /**
     * Returns the ratio of this list.
     * 
     * @return the ratio of this list.
     */

    public int ratio() {
        return ratio;
    }

    /**
     * Computes the length of the array at the given index.
     * 
     * <P>
     * This private version of {@link #arrayLength(int)} does not check its
     * argument.
     * 
     * @param index
     *            an index.
     * @return the length of the <code>index</code>-th array.
     */
    private int length(final int index) {
//        final byte[] array = this.array;
        final BackingBuffer bb = this.bb;
        final int delta = index % ratio; // The index into the p array, and the
                                         // delta inside the block.

        int pos = p[index / ratio]; // The position into the array of the first
                                    // entire word before the index-th.
//        int length = readInt(array, pos);
        int length = bb.readInt(pos);

        if (delta == 0)
            return length;

        // First of all, we recover the array length and the maximum amount of
        // copied elements.
        int common;
        pos += count(length) + length;
//        length = readInt(array, pos);
//        common = readInt(array, pos + count(length));
        length = bb.readInt(pos);
        common = bb.readInt(pos + count(length));

        for (int i = 0; i < delta - 1; i++) {
            pos += count(length) + count(common) + length;
//            length = readInt(array, pos);
//            common = readInt(array, pos + count(length));
            length = bb.readInt(pos);
            common = bb.readInt(pos + count(length));
        }

        return length + common;
    }

    /**
     * Computes the length of the array at the given index.
     * 
     * @param index
     *            an index.
     * @return the length of the <code>index</code>-th array.
     */
    public int arrayLength(final int index) {
        ensureRestrictedIndex(index);
        return length(index);
    }

    /**
     * Extracts the array at the given index.
     * 
     * @param index
     *            an index.
     * @param a
     *            the array that will store the result (we assume that it can
     *            hold the result).
     * @param offset
     *            an offset into <code>a</code> where elements will be store.
     * @param length
     *            a maximum number of elements to store in <code>a</code>.
     * @return the length of the extracted array.
     */
    private int extract(final int index, final byte a[], final int offset,
            final int length) {
        final BackingBuffer bb = this.bb;
        final int delta = index % ratio; // The delta inside the block.
        final int startPos = p[index / ratio]; // The position into the array of
                                               // the first entire word before
                                               // the index-th.
//        int pos, arrayLength = readInt(array, pos = startPos), prevArrayPos, currLen = 0, actualCommon;
        int pos, prevArrayPos, currLen = 0, actualCommon;
        int arrayLength = bb.readInt(pos = startPos);

        if (delta == 0) {
            pos = p[index / ratio] + count(arrayLength);
//            System.arraycopy(array, pos, a, offset, Math.min(length,
//                    arrayLength));
            bb.arraycopy(pos, a, offset, Math.min(length,
                    arrayLength));
            return arrayLength;
        }

        int common = 0;

        for (int i = 0; i < delta; i++) {
            prevArrayPos = pos + count(arrayLength)
                    + (i != 0 ? count(common) : 0);
            pos = prevArrayPos + arrayLength;

//            arrayLength = readInt(array, pos);
//            common = readInt(array, pos + count(arrayLength));
            arrayLength = bb.readInt(pos);
            common = bb.readInt(pos + count(arrayLength));

            actualCommon = Math.min(common, length);
            if (actualCommon <= currLen)
                currLen = actualCommon;
            else {
//                System.arraycopy(array, prevArrayPos, a, currLen + offset,
//                        actualCommon - currLen);
                bb.arraycopy(prevArrayPos, a, currLen + offset,
                        actualCommon - currLen);
                currLen = actualCommon;
            }
        }

        if (currLen < length)
//            System.arraycopy(array, pos + count(arrayLength) + count(common),
//                    a, currLen + offset, Math
//                            .min(arrayLength, length - currLen));
            bb.arraycopy(pos + count(arrayLength) + count(common),
                    a, currLen + offset, Math
                            .min(arrayLength, length - currLen));

        return arrayLength + common;
    }

    public byte[] get(final int index) {
        return getArray(index);
    }

    /**
     * @see #get(int)
     */

    public byte[] getArray(final int index) {
        ensureRestrictedIndex(index);
        final int length = length(index);
        final byte a[] = new byte[length];
        extract(index, a, 0, length);
        return a;
    }

    /**
     * Write the specified byte[] onto a stream.
     * 
     * @param os
     *            The stream.
     * @param index
     *            The index of the byte[].
     * 
     * @return The #of bytes written on the stream.
     * 
     * @throws IOException
     * 
     * @todo Optimize this to avoid the byte[] allocation.
     * 
     * @todo An alternative optimization would be to specify a variant of
     *       {@link #get(int)} which accepts a {@link ByteArrayBuffer} that is
     *       automatically extended to have sufficient capacity.
     */
    public int writeOn(final OutputStream os, final int index)
            throws IOException {

        final byte[] a = get(index);
        
        os.write(a);

        return a.length;

    }

    /**
     * Stores in the given array elements from an array stored in this
     * front-coded list.
     * 
     * @param index
     *            an index.
     * @param a
     *            the array that will store the result.
     * @param offset
     *            an offset into <code>a</code> where elements will be store.
     * @param length
     *            a maximum number of elements to store in <code>a</code>.
     * @return if <code>a</code> can hold the extracted elements, the number of
     *         extracted elements; otherwise, the number of remaining elements
     *         with the sign changed.
     */
    public int get(final int index, final byte[] a, final int offset,
            final int length) {
        ensureRestrictedIndex(index);
        ByteArrays.ensureOffsetLength(a, offset, length);

        final int arrayLength = extract(index, a, offset, length);
        if (length >= arrayLength)
            return arrayLength;
        return length - arrayLength;
    }

    /**
     * Stores in the given array an array stored in this front-coded list.
     * 
     * @param index
     *            an index.
     * @param a
     *            the array that will store the content of the result (we assume
     *            that it can hold the result).
     * @return if <code>a</code> can hold the extracted elements, the number of
     *         extracted elements; otherwise, the number of remaining elements
     *         with the sign changed.
     */
    public int get(final int index, final byte[] a) {
        return get(index, a, 0, a.length);
    }

    public int size() {
        return n;
    }

    public ObjectListIterator<byte[]> listIterator(final int start) {
        ensureIndex(start);

        return new AbstractObjectListIterator<byte[]>() {
            byte a[] = ByteArrays.EMPTY_ARRAY;

            int i = 0, pos = 0;

            boolean inSync; // Whether the current value in a is the string just
                            // before the next to be produced.

            {
                if (start != 0) {
                    if (start == n)
                        i = start; // If we start at the end, we do nothing.
                    else {
                        pos = p[start / ratio];
                        int j = start % ratio;
                        i = start - j;
                        while (j-- != 0)
                            next();
                    }
                }
            }

            public boolean hasNext() {
                return i < n;
            }

            public boolean hasPrevious() {
                return i > 0;
            }

            public int previousIndex() {
                return i - 1;
            }

            public int nextIndex() {
                return i;
            }

            public byte[] next() {
                int length, common;

                if (!hasNext())
                    throw new NoSuchElementException();

                final BackingBuffer bb = CustomByteArrayFrontCodedList.this.bb;
                if (i % ratio == 0) {
                    pos = p[i / ratio];
//                    length = readInt(array, pos);
                    length = bb.readInt(pos);
                    a = ByteArrays.ensureCapacity(a, length, 0);
//                    System.arraycopy(array, pos + count(length), a, 0, length);
                    bb.arraycopy(pos + count(length), a, 0, length);
                    pos += length + count(length);
                    inSync = true;
                } else {
                    if (inSync) {
//                        length = readInt(array, pos);
//                        common = readInt(array, pos + count(length));
                        length = bb.readInt(pos);
                        common = bb.readInt(pos + count(length));
                        a = ByteArrays.ensureCapacity(a, length + common,
                                common);
//                        System.arraycopy(array, pos + count(length)
//                                + count(common), a, common, length);
                        bb.arraycopy(pos + count(length)
                                + count(common), a, common, length);
                        pos += count(length) + count(common) + length;
                        length += common;
                    } else {
                        a = ByteArrays.ensureCapacity(a, length = length(i), 0);
                        extract(i, a, 0, length);
                    }
                }
                i++;
                return ByteArrays.copy(a, 0, length);
            }

            public byte[] previous() {
                if (!hasPrevious())
                    throw new NoSuchElementException();
                inSync = false;
                return getArray(--i);
            }
        };
    }

    /**
     * Returns a copy of this list.
     * 
     * @return a copy of this list.
     */

    public Object clone() {
        CustomByteArrayFrontCodedList c;
        try {
            c = (CustomByteArrayFrontCodedList) super.clone();
        } catch (CloneNotSupportedException cantHappen) {
            throw new InternalError();
        }
//        c.array = array.clone();
        c.bb = bb.clone();
        c.p = p.clone();
        return c;
    }

    /**
     * Modified to dump internal record metadata and to show the byte[]s as
     * unsigned values.
     */
    public String toString() {
        final StringBuffer s = new StringBuffer();
        s.append("{ratio=" + ratio + ", size=" + n + ", p[]="
                + java.util.Arrays.toString(p));
        s.append("[\n");
        for (int i = 0; i < n;) {
            int pos = p[i/ratio];
            for (int j = 0; j < ratio && i < n; j++, i++) {
                final int delta = i % ratio;
                final int pos0 = pos; // pos @ rlen.
                final int rlen = bb.readInt(pos);
                pos += count(rlen);
                final int clen;
                if (delta == 0) {
                    clen = 0;
                } else {
                    clen = bb.readInt(pos);
                    pos += count(clen);
                }
                final byte[] a = get(i);
                s.append("index=" + i + ", delta=" + delta + ", p["
                        + (i / ratio) + "]=" + p[i / ratio] + ", pos@rlen="
                        + pos0 + ", rlen=" + rlen + ", clen=" + clen
                        + ", pos@remainder=" + pos + " :: "
                        + BytesUtil.toString(a) + "\n");
                pos += rlen;
            }
//        for(int i=0; i<n; i++) {
//            if (i != 0)
//                s.append(", ");
//            s.append(ByteArrayList.wrap(getArray(i)).toString());
        }
        s.append("]}");
        return s.toString();
    }

    private void readObject(java.io.ObjectInputStream s)
            throws java.io.IOException, ClassNotFoundException {

        s.defaultReadObject();

        rebuildPointerArray();

    }

    /*
     * New ctors and new methods.
     */

    /**
     * Reconsitute an instance from just the coded byte[], the #of elements in
     * the array, and the ratio.
     * 
     * @param n
     *            The #of elements in the array.
     * @param ratio
     *            The ratio of this front-coded list.
     * @param array
     *            The array containing the compressed arrays.
     */
    public CustomByteArrayFrontCodedList(final int n, final int ratio,
            final byte[] array) {

        this(n, ratio, array, 0, array.length);
    }

    /**
     * Reconsitute an instance from a slice byte[] containing the coded data,
     * the #of elements in the array, and the ratio.
     * 
     * @param n
     *            The #of elements in the array.
     * @param ratio
     *            The ratio of this front-coded list.
     * @param array
     *            The array containing the compressed arrays.
     */
    public CustomByteArrayFrontCodedList(final int n, final int ratio,
            final byte[] array, final int off, final int len) {

        assertRatio(ratio);

        this.n = n;

        this.ratio = ratio;

//        this.array = array;
        this.bb = new BackingByteArray(array, off, len);

        rebuildPointerArray();

    }

    /**
     * Reconsitute an instance from just a {@link ByteBuffer} view onto the
     * coded byte[], the #of elements in the array, and the ratio.
     * 
     * @param n
     *            The #of elements in the array.
     * @param ratio
     *            The ratio of this front-coded list.
     * @param b
     *            The view onto the compressed arrays.
     */
    public CustomByteArrayFrontCodedList(final int n, final int ratio,
            final ByteBuffer b) {

        assertRatio(ratio);
        
        this.n = n;

        this.ratio = ratio;

        this.bb = new BackingByteBuffer(b);

        rebuildPointerArray();

    }

    /**
     * Return the backing buffer.
     */
    public BackingBuffer getBackingBuffer() {

//        return array;
        return bb;

    }

    /**
     * Rebuild pointer array from the packed byte {@link #array}, the #of
     * elements in that array {@link #n}, and the {@link #ratio()}.
     */
    private void rebuildPointerArray() {

        final int[] p = new int[(n + ratio - 1) / ratio];
//        final byte a[] = array;
        final BackingBuffer bb = this.bb;
        int i = 0, pos = 0, length, common;

        for (i = 0; i < n; i++) {
//            length = readInt(a, pos);
            length = bb.readInt(pos);
            if (i % ratio == 0) {
                p[i / ratio] = pos;
                pos += count(length) + length;
            } else {
//                common = readInt(a, pos + count(length));
                common = bb.readInt(pos + count(length));
                pos += count(length) + count(common) + length;
            }
        }

        this.p = p;

    }

    /**
     * Search for the index of the value having the same data. The results are
     * meaningless if the list is not ordered. A binary search is performed
     * against each of the entries that is coded as a full length value. If
     * there is a match, the index of that entry is returned directly. Otherwise
     * a linear scan is performed starting with the insertion point as
     * identified by the binary search. The combination of a binary search
     * followed by a linear scan implies that search is fastest when ratio is
     * small. However, the compression is highest when the ratio is equal to the
     * #of entries in the list. Therefore, search performance is traded off
     * against compression.
     * <p>
     * Note: The full length entry is coded every [i/ratio] entries. However,
     * the subsequent entries code their common length with respect to the
     * previous front-coded entry NOT to the full length entry. This means that
     * the length of the common prefix can increase or decrease as we scan a
     * bucket and the #of already matched bytes can increase or decrease as
     * well. Consider the following example, when coded with a ratio of 8.
     * 
     * <pre>
     * [121, 59, 18, 79, 99, 112, 24, 116], // #0 rlen=8, clen=0 (new bucket)
     * [121, 59, 18, 79, 99, 112, 43, 68],  // #1 rlen=2, clen=6
     * [121, 59, 18, 79, 99, 112, 46, 78],  // #2 rlen=2, clen=6
     * [121, 59, 18, 79, 99, 112, 54, 48],  // #3 rlen=2, clen=6
     * [121, 59, 18, 79, 99, 112, 54, 108], // #4 rlen=1, clen=7 (***)
     * [121, 59, 18, 79, 99, 112, 55, 81],  // #5 rlen=2, clen=6
     * [121, 59, 18, 79, 99, 112, 62, 85],  // #6 rlen=2, clen=6
     * [121, 59, 18, 79, 99, 112, 63, 110], // #7 rlen=8, clen=0 (new bucket)
     * [121, 59, 18, 79, 99, 112, 71, 124], // #8 ...
     * [121, 59, 18, 79, 99, 112, 73, 49]   // #9 ...
     * </pre>
     * 
     * The common length grows for entry #4 because the [54] in the next to last
     * byte in the array already appears in the same position in the previous
     * front-coded entry. However, the common length decreases again for the
     * next entry (#5).
     * <p>
     * The following rules guide the linear search of the bucket identified by
     * the binary search.
     * <ol>
     * <li>If <code>clen GT mlen</code>, then skip to the next entry in the
     * bucket as no match is possible (the common prefix was demonstrated to be
     * longer than the matched prefix on a previous entry).</li>
     * <li>Compare the remaining bytes in the search probe with the remainder
     * for the current entry.</li>
     * <li>If the search probe is EQ to the bucket entry, then halt. The probe
     * key was found.</li>
     * <li>If the search probe is LT the bucket entry, then halt. The probe key
     * was not found.</li>
     * <li>Otherwise, <code>mlen += prefixLength</code>, where prefixLength is
     * the length of the matched prefix from step 2.
     * </ol>
     * 
     * @param a
     *            The search probe, which is interpreted as an
     *            <em>unsigned byte[]</code>
     * 
     * @return index of the search key, if it is found; otherwise,
     *         <code>(-(insertion point) - 1)</code>. The insertion point is
     *         defined as the point at which the key would be inserted. Note
     *         that this guarantees that the return value will be >= 0 if and
     *         only if the key is found.
     */
    public int search(final byte[] a) {

        /*
         * We can efficiently test each at each index which is an even multiple
         * of the ratio. For those indices, we do not have to copy the data out
         * of its compressed format. Therefore we first perform a binary search
         * and locate the greatest index that is a multiple of the ratio whose
         * value is LTE to the probe key.
         */
        final int pret = binarySearch(a);

        if (pret >= 0) {

            // An exact match on a full length entry in the backing buffer.
            return pret * ratio;
            
        }
        
        if (pret == -1) {

            /*
             * The key would be inserted before the first entry in the
             * front-coded array.
             */
            return -1;
            
        }

        /*
         * Next we do a linear scan of up to [ratio-1] entries, returning the
         * first entry whose common bytes and remainder would reconstruct the
         * probe key. If we find an entry which would be ordered GT the probe
         * key, then we return the insertion point instead.
         */

        /*
         * Convert the insertion point into the index of the set of up to ratio
         * front-coded byte[]s to be searched. Note that we always search the
         * bucket before the insertion point since we are looking for a key
         * which might exist in that bucket.
         */
        final int poffset = (-pret - 1) - 1;

        // The corresponding index into the list.
        final int offset = poffset * ratio;

        /*
         * This is starting position in the backing buffer of the full length
         * entry corresponding to the insertion point.
         */
        int pos = p[poffset];

        /*
         * The #of bytes in the full length entry which match the probe key.
         * Note: This is NOT a fixed value. When we scan the front-coded entries
         * in the same bucket, the common length with respect to the previous
         * entry can actually increase -or- decrease, in which case the matched
         * length may change as well.
         */
        int mlen;
        {

            // The #of bytes in the full length byte[] at the insertion point.
            final int blen = bb.readInt(pos);

            // Skip the #of bytes required to code that length.
            pos += count(blen);

            // Count matching bytes.
            int i;
            for (i = 0; i < a.length && i < blen; i++, pos++) {

                if (a[i] != bb.get(pos)) {
                    
                    break;

                }

            }
            
            // #of matching bytes.
            mlen = i;

            // skip over the remainder of the full length coded entry.
            pos += (blen - mlen);
            
        }

//        if(mlen == 0) {
//            
//            /*
//             * The search key does not match anything in the full-length entry
//             * for this bucket.
//             */
//            
//            assert pret < 0; // must have been a miss on the binary search.
//            
//            // return the insertion point.
//            return pret;
//            
//        }
        
        /*
         * Scan up to ratio-1 entries or the last entry, whichever comes first.
         */
        final int limit = Math.min(n - (offset + 1), ratio - 1);

        int delta;
        for (delta = 0; delta < limit; delta++) {

            // length of the remainder for this entry.
            final int rlen = bb.readInt(pos);

            // skip past rlen field.
            pos += count(rlen);

            // length of the common prefix (shared with the entry @ the ptr).
            final int clen = bb.readInt(pos);

            // skip past clen field.
            pos += count(clen);

            if (clen > mlen) {
                /*
                 * No match is possible while the common prefix length with the
                 * prior entry is GT the matched length with the probe key.
                 */
                pos += rlen;
                continue;
            }

            /*
             * Compare the remaining bytes in the search probe to the remainder
             * of the current entry.
             */
            
            assert mlen == clen : "mlen=" + mlen + ", clen=" + clen
                    + ", delta=" + delta + ", pret=" + pret + ", poffset="
                    + poffset;
            
            final int ret = compareBytes(a, mlen, a.length - mlen, bb, pos,
                    rlen);

            if (ret == 0) {

                // Found by linear scan of front-coded entries.
                return offset + delta + 1;

            }
            
            if (ret < 0) {

                // The current entry is GT the probe key. Halt (not found).
                break;
                
            }

            /*
             * Update the matched length by the length of the matched pefix from
             * the last comparison test.
             */
            final int prefixLength = Math.abs(ret) - 1;
            mlen += prefixLength;
            
            // skip past the remainder and keep looking.
            pos += rlen;
            
        }
        
        // The insert point into the list.
        return (-(offset + delta + 1) - 1);
        
    }

    /**
     * Binary search against the entries in the backing buffer that are coded as
     * their full length values.
     * 
     * @param key
     *            The key for the search.
     * 
     * @return index of the search key into the pointer array, if it is an exact
     *         match with any of the full length values whose offsets are stored
     *         in the pointer array; otherwise,
     *         <code>(-(insertion point) - 1)</code>. The insertion point is
     *         defined as the point at which the key would be inserted into the
     *         list. Note that this guarantees that the return value will be >=
     *         0 if and only if the key is matched by any of the full length
     *         coded entries.
     */
    private int binarySearch(final byte[] key) {

        final int base = 0;
        
        final BackingBuffer bb = this.bb;

        /*
         * We will test each entry having an index that is an even multiple of
         * the ratio. The offset into the backing buffer of each such entry is
         * given by p[]. The data at p[i] is the length of the fully coded value
         * followed by the value itself.
         */
        final int nmem = p.length;
        
        int low = 0;

        int high = nmem - 1;

        while (low <= high) {

            final int mid = (low + high) >> 1;

            final int offset = base + mid;

            /*
             * Compare the probe with the full length byte[] at index [mid].
             */
            final int tmp;
            {

                // The index into the backing buffer of index [mid].
                int pos = p[mid];
                
                // The #of bytes in the full length byte[] at index [mid].
                final int blen = bb.readInt(pos);
                
                // Skip the #of bytes required to code that length.
                pos += count(blen);

                // Compare key vs actual (in buffer).
                tmp = compareBytes(key, 0, key.length, bb, pos, blen);

            }

            if (tmp > 0) {

                // Actual GT probe, restrict lower bound and try again.
                low = mid + 1;

            } else if (tmp < 0) {

                // Actual LT probe, restrict upper bound and try again.
                high = mid - 1;

            } else {

                // Found: return offset.

                return offset;

            }

        }

        // Not found: return insertion point.

        final int offset = (base + low);

        return -(offset + 1);

    }

    /**
     * Compare up to <i>len</i> bytes in <i>a</i> interpreted as unsigned bytes
     * against the bytes in the {@link BackingBuffer} starting at offset
     * <i>off</i> in the {@link BackingBuffer}.
     * 
     * @param a
     *            The caller's probe.
     * @param bb
     *            The {@link BackingBuffer}.
     * @param boff
     *            The offset of the first byte to be compared in the
     *            {@link BackingBuffer}.
     * @param blen
     *            The length of the byte[] at that offset in the
     *            {@link BackingBuffer}.
     * 
     * @return The return is a negative integer, zero, or a positive integer if
     *         the first argument is less than, equal to, or greater than the
     *         byte[] in the {@link BackingBuffer} at the specified offset. The
     *         return value also codes the length of the shared prefix, which
     *         may be computed as <code>Math.abs(ret)-1</code>.
     */
    private int compareBytes(final byte[] a, final int aoff, final int alen,
            final BackingBuffer bb, final int boff, final int blen) {

        int mlen = 0;
        // Compare bytes(probe,entry) (negative iff probe < entry)
        for (int i = aoff, j = boff; i < aoff + alen && j < boff + blen; i++, j++, mlen++) {

            // promotes to signed integers in [0:255] for comparison.
            final int ret = (a[i] & 0xff) - (bb.get(j) & 0xff);

            if (ret != 0) {

                return ret < 0 ? -(mlen + 1) : (mlen + 1);
                
            }

        }

        return alen == blen ? 0 : (alen - blen) < 0 ? -(mlen + 1) : (mlen + 1);

    }

}
