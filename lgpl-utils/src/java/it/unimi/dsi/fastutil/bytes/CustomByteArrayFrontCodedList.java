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

import it.unimi.dsi.fastutil.objects.AbstractObjectListIterator;
import it.unimi.dsi.fastutil.objects.AbstractObjectList;
import it.unimi.dsi.fastutil.objects.ObjectListIterator;
import it.unimi.dsi.fastutil.ints.IntArrays;

import java.io.DataOutput;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Collection;
import java.util.NoSuchElementException;

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
 * <P>
 * Note: This class was derived from <code>ArrayFrontCodedList</code>, which is
 * part of fastutils. The name of the class has been changed to prevent
 * classpath problems. The class has a new {@link #serialVersionUID} and the
 * serialization logic has been modified to allow serialization against
 * {@link DataOutput} by defining {@link #getArray()} and a new ctor which
 * accepts the backing array. The test code from main() has been isolated in a
 * junit test suite.
 */

public class CustomByteArrayFrontCodedList extends AbstractObjectList<byte[]> implements Serializable, Cloneable {

    /**
     * 
     */
    private static final long serialVersionUID = -2532468860579334765L;
    
    // The value for the original impl.
//	public static final long serialVersionUID = -7046029254386353130L;

	/** The number of arrays in the list. */
	protected int n;
	/** The ratio of this front-coded list. */
	protected int ratio;
	/** The array containing the compressed arrays. */
	protected byte[] array;
	/** The pointers to entire arrays in the list. */
	transient protected int[] p;

	/** Creates a new front-coded list containing the arrays returned by the given iterator.
	 * 
	 * @param arrays an iterator returning arrays.
	 * @param ratio the desired ratio.
	 */

	public CustomByteArrayFrontCodedList( final Iterator arrays, final int ratio ) {

		if ( ratio < 1 ) throw new IllegalArgumentException( "Illegal ratio (" + ratio + ")" );

		byte[] array = ByteArrays.EMPTY_ARRAY;
		int[] p = IntArrays.EMPTY_ARRAY;

		byte[][] a = new byte[ 2 ][];
		int curSize = 0, b = 0, common, length, minLength;

		while( arrays.hasNext() ) {
			a[ b ] = (byte[])arrays.next();
			length = a[ b ].length;
			
			if ( n % ratio == 0 ) {
				p = IntArrays.grow( p, n / ratio + 1 );
				p[ n / ratio ] = curSize;

				array = ByteArrays.grow( array, curSize + count( length ) + length, curSize );
				curSize += writeInt( array, length, curSize );
				System.arraycopy( a[ b ], 0, array, curSize, length );
				curSize += length;
			}
			else {
				minLength = a[ 1 - b ].length;
				if ( length < minLength ) minLength = length;
				for( common = 0; common < minLength; common++ ) if ( a[ 0 ][ common ] != a[ 1 ][ common ] ) break;
				length -= common;

				array = ByteArrays.grow( array, curSize + count( length ) + count( common ) + length, curSize );
				curSize += writeInt( array, length, curSize );
				curSize += writeInt( array, common, curSize );
				System.arraycopy( a[ b ], common, array, curSize, length );
				curSize += length;
			}

			b = 1 - b;
			n++;
		}

		this.ratio = ratio;
		this.array = ByteArrays.trim( array, curSize );
		this.p = IntArrays.trim( p, ( n + ratio - 1 ) / ratio );

	}

	/** Creates a new front-coded list containing the arrays in the given collection.
	 * 
	 * @param c a collection containing arrays.
	 * @param ratio the desired ratio.
	 */

	public CustomByteArrayFrontCodedList( final Collection c, final int ratio ) {
		this( c.iterator(), ratio );
	}

	/** Reads a coded length.
	 * @param a the data array.
	 * @param pos the starting position.
	 * @return the length coded at <code>pos</code>.
	 */
	private static int readInt( final byte a[], int pos ) {
		if ( a[ pos ] >= 0 ) return a[ pos ];
		if ( a[ pos + 1 ] >= 0 ) return ( - a[ pos ] - 1 ) << 7 | a[ pos + 1 ];
		if ( a[ pos + 2 ] >= 0 ) return ( - a[ pos ] - 1 ) << 14 | ( - a[ pos + 1 ] - 1 ) << 7 | a[ pos + 2 ];
		if ( a[ pos + 3 ] >= 0 ) return ( - a[ pos ] - 1 ) << 21 | ( - a[ pos + 1 ] - 1 ) << 14 | ( - a[ pos + 2 ] - 1 ) << 7 | a[ pos + 3 ];
		return ( - a[ pos ] - 1 ) << 28 | ( - a[ pos + 1 ] - 1 ) << 21 | ( - a[ pos + 2 ] - 1 ) << 14 | ( - a[ pos + 3 ] - 1 ) << 7 | a[ pos + 4 ];
	}

	/** Computes the number of elements coding a given length.
	 * @param length the length to be coded.
	 * @return the number of elements coding <code>length</code>.
	 */
	@SuppressWarnings("unused")
	private static int count( final int length ) {
		if ( length < ( 1 << 7 ) ) return 1;
		if ( length < ( 1 << 14 ) ) return 2;
		if ( length < ( 1 << 21 ) ) return 3;
		if ( length < ( 1 << 28 ) ) return 4;
		return 5;
	}

	/** Writes a length.
	 * @param a the data array.
	 * @param length the length to be written.
	 * @param pos the starting position.
	 * @return the number of elements coding <code>length</code>.
	 */
	private static int writeInt( final byte a[], int length, int pos ) {
		final int count = count( length );
		a[ pos + count - 1 ] = (byte)( length & 0x7F );

		if ( count != 1 ) {
			int i = count - 1;
			while( i-- != 0 ) {
				length >>>= 7;
				a[ pos + i ] = (byte)( - ( length  & 0x7F ) - 1 );
			}
		}

		return count;
	}



	/** Returns the ratio of this list.
	 *
	 * @return the ratio of this list.
	 */

	public int ratio() {
		return ratio;
	}


	/** Computes the length of the array at the given index.
	 *
	 * <P>This private version of {@link #arrayLength(int)} does not check its argument.
	 *
	 * @param index an index.
	 * @return the length of the <code>index</code>-th array.
	 */
	private int length( final int index ) {
		final byte[] array = this.array;
		final int delta = index % ratio; // The index into the p array, and the delta inside the block.

		int pos = p[ index / ratio ]; // The position into the array of the first entire word before the index-th.
		int length = readInt( array, pos );

		if ( delta == 0 ) return length;
		
		// First of all, we recover the array length and the maximum amount of copied elements.
		int common;
		pos += count( length ) + length;
		length = readInt( array, pos );
		common = readInt( array, pos + count( length ) );

		for( int i = 0; i < delta - 1; i++ ) {
			pos += count( length ) + count( common ) + length;
			length = readInt( array, pos );
			common = readInt( array, pos + count( length ) );
		}

		return length + common;
	}


	/** Computes the length of the array at the given index.
	 *
	 * @param index an index.
	 * @return the length of the <code>index</code>-th array.
	 */
	public int arrayLength( final int index ) {
		ensureRestrictedIndex( index );
		return length( index );
	}

	/** Extracts the array at the given index.
	 *
	 * @param index an index.
	 * @param a the array that will store the result (we assume that it can hold the result).
	 * @param offset an offset into <code>a</code> where elements will be store.
	 * @param length a maximum number of elements to store in <code>a</code>.
	 * @return the length of the extracted array.
	 */
	private int extract( final int index, final byte a[], final int offset, final int length ) {
		final int delta = index % ratio; // The delta inside the block.
		final int startPos = p[ index / ratio ]; // The position into the array of the first entire word before the index-th.
		int pos, arrayLength = readInt( array, pos = startPos ), prevArrayPos, currLen = 0, actualCommon;

		if ( delta == 0 ) {
			pos = p[ index / ratio ] + count( arrayLength );
			System.arraycopy( array, pos, a, offset, Math.min( length, arrayLength ) );
			return arrayLength;
		}
		
		int common = 0;

		for( int i = 0; i < delta; i++ ) {
			prevArrayPos = pos + count( arrayLength ) + ( i != 0 ? count( common ) : 0 );
			pos = prevArrayPos + arrayLength;

			arrayLength = readInt( array, pos );
			common = readInt( array, pos + count( arrayLength ) );

			actualCommon = Math.min( common, length );
			if ( actualCommon <= currLen ) currLen = actualCommon;
			else {
				System.arraycopy( array, prevArrayPos, a, currLen + offset, actualCommon - currLen );
				currLen = actualCommon;
			}
		}

		if ( currLen < length ) System.arraycopy( array, pos + count( arrayLength ) + count( common ), a, currLen + offset, Math.min( arrayLength, length - currLen ) );

		return arrayLength + common;
	}

	public byte[] get( final int index ) {
		return getArray( index );
	}

	/** 
	 * @see #get(int)
	 */

	public byte[] getArray( final int index ) {
		ensureRestrictedIndex( index );
		final int length = length( index );
		final byte a[] = new byte[ length ];
		extract( index, a, 0, length );
		return a;
	}

	/** Stores in the given array elements from an array stored in this front-coded list.
	 *
	 * @param index an index.
	 * @param a the array that will store the result.
	 * @param offset an offset into <code>a</code> where elements will be store.
	 * @param length a maximum number of elements to store in <code>a</code>.
	 * @return if <code>a</code> can hold the extracted elements, the number of extracted elements;
	 * otherwise, the number of remaining elements with the sign changed.
	 */
	public int get( final int index, final byte[] a, final int offset, final int length ) {
		ensureRestrictedIndex( index );
		ByteArrays.ensureOffsetLength( a, offset, length );

		final int arrayLength = extract( index, a, offset, length );
		if ( length >= arrayLength ) return arrayLength;
		return length - arrayLength;
	}

	/** Stores in the given array an array stored in this front-coded list.
	 *
	 * @param index an index.
	 * @param a the array that will store the content of the result (we assume that it can hold the result).
	 * @return if <code>a</code> can hold the extracted elements, the number of extracted elements;
	 * otherwise, the number of remaining elements with the sign changed.
	 */
	public int get( final int index, final byte[] a ) {
		return get( index, a, 0, a.length );
	}

	public int size() {
		return n;
	}

	public ObjectListIterator<byte[]> listIterator( final int start ) {
		ensureIndex( start );

		return new AbstractObjectListIterator<byte[]>() {
				byte a[] = ByteArrays.EMPTY_ARRAY;
				int i = 0, pos = 0;
				boolean inSync; // Whether the current value in a is the string just before the next to be produced.

				{
					if ( start != 0 ) {
						if ( start == n ) i = start; // If we start at the end, we do nothing.
						else {
							pos = p[ start / ratio ];
							int j = start % ratio;
							i = start - j;
							while( j-- != 0 ) next();
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

					if ( ! hasNext() ) throw new NoSuchElementException();

					if ( i % ratio == 0 ) {
						pos = p[ i / ratio ];
						length = readInt( array, pos );
						a = ByteArrays.ensureCapacity( a, length, 0 );
						System.arraycopy( array, pos + count( length ), a, 0, length );
						pos += length + count( length );
						inSync = true;
					}
					else {
						if ( inSync ) {
							length = readInt( array, pos );
							common = readInt( array, pos + count( length ) );
							a = ByteArrays.ensureCapacity( a, length + common, common );
							System.arraycopy( array, pos + count( length ) + count ( common ), a, common, length );
							pos += count( length ) + count( common ) + length;
							length += common;
						}
						else {
							a = ByteArrays.ensureCapacity( a, length = length( i ), 0 );
							extract( i, a, 0, length );
						}
					}
					i++;
					return ByteArrays.copy( a, 0, length );
				}

				public byte[] previous() {
					if ( ! hasPrevious() ) throw new NoSuchElementException();
					inSync = false;
					return getArray( --i );
				}
			};
	}


	/** Returns a copy of this list. 
	 *
	 *  @return a copy of this list.
	 */

	public Object clone() {
		CustomByteArrayFrontCodedList c;
		try {
			c = (CustomByteArrayFrontCodedList)super.clone();
		}
		catch( CloneNotSupportedException cantHappen ) {
			throw new InternalError();
		}
		c.array = array.clone();
		c.p = p.clone();
		return c;
	}


	public String toString() {
		final StringBuffer s = new StringBuffer();
		s.append( "[ " );
		for( int i = 0; i < n; i++ ) {
			if ( i != 0 ) s.append( ", " );
			s.append( ByteArrayList.wrap( getArray( i ) ).toString() );
		}
		s.append( " ]" );
		return s.toString();
	}

	
	private void readObject(java.io.ObjectInputStream s)
            throws java.io.IOException, ClassNotFoundException {
        
        s.defaultReadObject();

        rebuildPointerArray();
        
	}

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
    public CustomByteArrayFrontCodedList(int n, int ratio, byte[] array) {

        if ( ratio < 1 ) throw new IllegalArgumentException( "Illegal ratio (" + ratio + ")" );

        this.n = n;
        
        this.ratio = ratio;
        
        this.array = array;
        
        rebuildPointerArray();
        
    }
    
    /**
     * Return the backing byte[].
     */
    public byte[] getArray() {

        return array;
        
    }
    
    /**
     * Rebuild pointer array from the packed byte {@link #array}, the #of
     * elements in that array {@link #n}, and the {@link #ratio()}.
     */
    private void rebuildPointerArray() {

        final int[] p = new int[(n + ratio - 1) / ratio];
        final byte a[] = array;
        int i = 0, pos = 0, length, common;

        for( i = 0; i < n; i++ ) {
            length = readInt( a, pos );
            if ( i % ratio == 0 ) {
                p[ i / ratio ] = pos;
                pos += count( length ) + length;
            }
            else {
                common = readInt( a, pos + count( length ) );
                pos += count( length ) + count ( common ) + length;
            }
        }

        this.p = p;

    }
    
}
