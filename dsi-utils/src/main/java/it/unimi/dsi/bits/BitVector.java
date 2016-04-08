package it.unimi.dsi.bits;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2007-2009 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 */

import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.longs.LongSortedSet;
import it.unimi.dsi.util.LongBigList;

import java.util.List;
import java.util.RandomAccess;

/** A vector of bits, a&#46;k&#46;a&#46; bit sequence, bit string, binary word, etc.
 * 
 * <P>This interface define several operations on finite sequences of bits.
 * Efficient implementations, such as {@link LongArrayBitVector},
 * use approximately one bit of memory for each bit in the vector, but this is not enforced.
 * 
 * <P>Operation of a bit vector are partially of boolean nature 
 * (e.g., logical operations between vectors),
 * partially of language-theoretical nature (e.g., concatenation), and 
 * partially of set-theoretical nature (e.g., asking which bits are set to one). 
 * To accomodate all these points of view, this interface extends  
 * {@link it.unimi.dsi.fastutil.booleans.BooleanList}, but also provides an
 * {@link #asLongSet()} method that exposes a {@link java.util.BitSet}-like view
 * and a {@link #asLongBigList(int)} method that provides integer-like access to
 * blocks of bits of given width.
 * 
 * <P>Most, if not all, classical operations on bit vectors can be seen as standard
 * operations on these two views: for instance, the number of bits set to one is just
 * the number of elements of the set returned by {@link #asLongSet()} (albeit a direct {@link #count()} method
 * is provided, too). The standard {@link java.util.Collection#addAll(java.util.Collection)} method
 * can be used to concatenate bit vectors, and {@linkplain java.util.List#subList(int, int) sublist views}
 * make it easy performing any kind of logical operation on subvectors.
 * 
 * <P>The only <i>caveat</i> is that sometimes the standard interface naming clashes slightly
 * with standard usage: for instance, {@link #clear(long)} will <em>not</em> set to zero
 * all bits (use {@link #fill(int) fill(0)} for that purpose), but rather will set the
 * vector length to zero. Also, {@link #add(long, int)} will not add logically a value at
 * the specified index, but rather will insert a new bit with the specified value at the specified
 * position.
 * 
 * <P>The {@link AbstractBitVector} class provides a fairly complete
 * abstract implementation that provides all methods except for the most
 * basic operations. Of course, the methods of {@link AbstractBitVector} are
 * very inefficient, but implementations such as {@link LongArrayBitVector}
 * have their own optimised implementations. 
 */
public interface BitVector extends RandomAccess, BooleanList {

	/** Sets a bit in this bit vector (optional operation). 
	 * @param index the index of a bit.
	 */
	public void set( long index );
	
	/** Clears a bit in this bit vector (optional operation). 
	 * @param index the index of a bit.
	 */
	public void clear( long index );

	/** Flips a bit in this bit vector (optional operation). 
	 * @param index the index of a bit.
	 */
	public void flip( long index );

	/** Fills a range of bits in this bit vector (optional operation). 
	 * @param from the first index (inclusive).
	 * @param to the last index (not inclusive).
	 * @param value the value (true or false).
	 */
	public void fill( long from, long to, boolean value );
	
	/** Clears a range of bits in this bit vector (optional operation). 
	 * @param from the first index (inclusive).
	 * @param to the last index (not inclusive).
	 * @param value the value (zero or nonzero).
	 */
	public void fill( long from, long to, int value );
	
	/** Sets all bits this bit vector to the given boolean value (optional operation). */
	public void fill( boolean value );
	
	/** Sets all bits this bit vector to the given integer value (optional operation). */
	public void fill( int value );
	
	/** Flips a range of bits in this bit vector (optional operation). 
	 * @param from the first index (inclusive).
	 * @param to the last index (not inclusive).
	 */
	public void flip( long from, long to );

	/** Flips all bits in this bit vector (optional operation). */
	public void flip();

	/** Replaces the content of this bit vector with another bit vector.
	 * 
	 * @param bitVector a bit vector.
	 * @return this bit vector.
	 */
	public BitVector replace( BitVector bitVector );
	
	/** Returns a subvector view specified by initial and final index. 
	 * 
	 * <p>The object returned by this method is a bit vector representing a <em>view</em> of this
	 * bit vector restricted to the given indices. Changes to the subvector
	 * will be reflected in the main vector.
	 * 
	 * @param from the first index (inclusive).
	 * @param to the last index (not inclusive).
	 */
	public BitVector subVector( long from, long to );

	/** Returns a subvector view specified by initial index and running up to the end of this vector.
	 * 
	 * @param from the first index (inclusive).
	 * @see #subVector(long, long)
	 */
	public BitVector subVector( long from );

	/** Returns a view of this bit vector as a sorted set of long integers.
	 * 
	 * <P>More formally, this bit vector is infinitely extended to the
	 * left with zeros (e.g., all bits beyond {@link #length(long)} are
	 * considered zeroes). The resulting infinite string is interpreted as the
	 * characteristic function of a set of integers.
	 * 
	 * <P>Note that, in particular, the resulting string representation is
	 * exactly that of a {@link java.util.BitSet}.
	 *
	 */
	public LongSortedSet asLongSet();
	
	/** Returns a view of this bit vector as a list of nonnegative integers of specified width.
	 * 
	 * <P>More formally, {@link LongBigList#getLong(long) getLong(p)} will return
	 * the nonnegative integer defined by the bits starting at <code>p * width</code> (bit 0, inclusive)
	 * and ending at <code>(p + 1) * width</code> (bit <code>width</code> &minus; 1, exclusive). 
	 */
	public LongBigList asLongBigList( int width );
	
	/** Returns the value of the specified bit.
	 * 
	 * <P>This method is semantically equivalent to {@link BooleanList#getBoolean(int)},
	 * but it gives access to long indices.
	 * 
	 * @param index the index of a bit.
	 * @return the value of the specified bit.
	 */
	public boolean getBoolean( long index );

	/** Returns the value of the specified bit as an integer.
	 * 
	 * <P>This method is a useful synonym for {@link #getBoolean(long)}.
	 * 
	 * @param index the index of a bit.
	 * @return the value of the specified bit as an integer (0 or 1).
	 */
	public int getInt( long index );

	/** Returns the specified bit range as a long.
	 * 
	 * <P>Note that bit 0 of the returned long will be bit <code>from</code>
	 * of this bit vector.
	 * 
	 * <P>Implementations are invited to provide high-speed implementations for
	 * the case in which <code>from</code> is a multiple of {@link Long#SIZE}
	 * and <code>to</code> is <code>from</code> + {@link Long#SIZE} (or less,
	 * in case the vector length is exceeded). This behaviour make it possible to
	 * implement high-speed hashing, copies, etc.
	 * 
	 * @param from the starting bit (inclusive).
	 * @param to the ending bit (exclusive).
	 * @return the long value contained in the specified bits.
	 */
	public long getLong( long from, long to );

	/** Sets the value of the specified bit (optional operation).
	 * 
	 * <P>This method is semantically equivalent to {@link BooleanList#set(int,boolean)},
	 * but it gives access to long indices.
	 * 
	 * @param index the index of a bit.
	 * @param value the new value.
	 */
	public boolean set( long index, boolean value );
	
	/** Sets the value of the specified bit as an integer (optional operation).
	 * 
	 * <P>This method is a useful synonym for {@link #set(long, boolean)}.
	 * 
	 * @param index the index of a bit.
	 * @param value the new value (any nonzero integer for setting the bit, zero for clearing the bit).
	 */
	public void set( long index, int value );
	
	/** Adds a bit with specified value at the specified index (optional operation).
	 * 
	 * <P>This method is semantically equivalent to {@link BooleanList#add(int,boolean)},
	 * but it gives access to long indices.
	 * 
	 * @param index the index of a bit.
	 * @param value the value that will be inserted at position <code>index</code>.
	 */
	public void add( long index, boolean value );
	
	/** Removes a bit with specified index (optional operation).
	 * 
	 * <P>This method is semantically equivalent to {@link BooleanList#removeBoolean(int)},
	 * but it gives access to long indices.
	 * 
	 * @param index the index of a bit.
	 * @return the previous value of the bit.
	 */
	public boolean removeBoolean( long index );
	
	/** Adds a bit with specified integer value at the specified index (optional operation).
	 * 
	 * <P>This method is a useful synonym for {@link #add(long, boolean)}.
	 * 
	 * @param index the index of a bit.
	 * @param value the value that will be inserted at position <code>index</code> (any nonzero integer for a true bit, zero for a false bit).
	 */
	public void add( long index, int value );
	
	/** Adds a bit with specified value at the end of this bit vector.
	 * 
	 * <P>This method is a useful synonym for {@link BooleanList#add(boolean)}.
	 * 
	 * @param value the new value (any nonzero integer for a true bit, zero for a false bit).
	 */

	public void add( int value );

	/** Appends the less significant bits of a long integer to this bit vector.
	 * 
	 * @param value a value to be appended
	 * @param k the number of less significant bits to be added to this bit vector.
	 * @return this bit vector.
	 */

	public BitVector append( long value, int k );

	/** Appends another bit vector to this bit vector.
	 * 
	 * @param bitVector a bit vector to be appended.
	 * @return this bit vector.
	 */

	public BitVector append( BitVector bitVector );

	/** Returns the number of bits in this bit vector.
	 *
	 * <p>If the number of bits in this vector is smaller than or equal to {@link Integer#MAX_VALUE}, this
	 * method is semantically equivalent to {@link List#size()}. 
	 *
	 * @return the number of bits in this bit vector. 
	 */
	public long length();
	
	/** Sets the number of bits in this bit vector.
	 *
	 * <p>It is expected that this method will try to allocate exactly
	 * the necessary space. 
	 *
	 * <p>If the number of bits in this vector is smaller than 
	 * or equal to {@link Integer#MAX_VALUE}, this
	 * method is semantically equivalent to {@link BooleanList#size(int)}.
	 *  
	 * @return this bit vector.
	 */
	public BitVector length( long newLength );
	
	/** Counts the number of bits set to true in this bit vector.
	 *
	 * @return the number of bits set to true in this bit vector. 
	 */
	public long count();
	
	/** Performs a logical and between this bit vector and another one, leaving the result in this vector.
	 * 
	 * @param v a bit vector.
	 * @return this bit vector.
	 */
	public BitVector and( BitVector v );

	/** Performs a logical or between this bit vector and another one, leaving the result in this vector.
	 * 
	 * @param v a bit vector.
	 * @return this bit vector.
	 */
	public BitVector or( BitVector v );

	/** Performs a logical xor between this bit vector and another one, leaving the result in this vector.
	 * 
	 * @param v a bit vector.
	 * @return this bit vector.
	 */
	public BitVector xor( BitVector v );

	/** Returns the position of the first bit set in this vector.
	 *
	 * @return the first bit set, or -1 for a vector of zeroes. 
	 */
	public long firstOne();

	/** Returns the position of the last bit set in this vector.
	 *
	 * @return the last bit set, or -1 for a vector of zeroes. 
	 */
	public long lastOne();

	/** Returns the position of the first bit set after the given position.
	 *
	 * @return the first bit set after position <code>index</code> (inclusive), or -1 if no such bit exists. 
	 */
	public long nextOne( long index );

	/** Returns the position of the first bit set before or at the given position.
	 *
	 * @return the first bit set before or at the given position, or -1 if no such bit exists. 
	 */
	public long previousOne( long index );

	/** Returns the position of the first bit unset in this vector.
	 *
	 * @return the first bit unset, or -1 for a vector of ones. 
	 */
	public long firstZero();

	/** Returns the position of the last bit unset in this vector.
	 *
	 * @return the last bit unset, or -1 for a vector of ones. 
	 */
	public long lastZero();

	/** Returns the position of the first bit unset after the given position.
	 *
	 * @return the first bit unset after position <code>index</code> (inclusive), or -1 if no such bit exists. 
	 */
	public long nextZero( long index );

	/** Returns the position of the first bit unset before or at the given position.
	 *
	 * @return the first bit unset before or at the given position, or -1 if no such bit exists. 
	 */
	public long previousZero( long index );

	/** Returns the length of the greatest common prefix between this and the specified vector.
	 *
	 * @param v a bit vector.
	 * @return the length of the greatest common prefix.
	 */
	public long longestCommonPrefixLength( BitVector v );

	/** Returns a copy of a part of this bit vector.
	 *
	 * @param from the starting bit, inclusive.
	 * @param to the ending bit, not inclusive.
	 * @return a copy of the part of this bit vector going from bit <code>from</code> (inclusive) to bit <code>to</code>
	 * (not inclusive)
	 */
	public BitVector copy( long from, long to );

	/** Returns a copy of this bit vector.
	 *
	 * @return a copy of this bit vector. 
	 */
	public BitVector copy();
	
	/** Returns the bits in this bit vector as an array of longs, not to be modified.
	 * 
	 * @return an array of longs whose first {@link #length()} bits contain the bits of
	 * this bit vector. The array cannot be modified.
	 */
	public long[] bits();
	
	/** Returns a hash code for this bit vector. 
	 * 
	 * <p>Hash codes for bit vectors are defined as follows:
	 * 
	 * <pre>
	 * final long length = length();
	 * long fullLength = length - length % Long.SIZE;
	 * long h = 0x9e3779b97f4a7c13L ^ length;	
	 * for( long i = 0; i &lt; fullLength; i += Long.SIZE ) h ^= ( h &lt;&lt; 5 ) + getLong( i, i + Long.SIZE ) + ( h >>> 2 );
	 * if ( length != fullLength ) h ^= ( h &lt;&lt; 5 ) + getLong( fullLength, length ) + ( h >>> 2 );
	 * (int)( ( h >>> 32 ) ^ h );
	 * </pre>
	 * 
	 * <p>The last value is the hash code of the bit vector. This hashing is based on shift-add-xor hashing
	 * (M.V. Ramakrishna and Justin Zobel, &ldquo;Performance in practice of string hashing functions&rdquo;, 
	 * <i>Proc. of the Fifth International Conference on Database Systems for Advanced Applications</i>, 1997, pages 215&minus;223).
	 * 
	 * <p>The returned value is not a high-quality hash such as 
	 * <a href="http://sux4j.dsi.unimi.it/docs/it/unimi/dsi/sux4j/mph/Hashes.html#jenkins(it.unimi.dsi.bits.BitVector)">Jenkins's</a>,
	 * but it can be computed very quickly; in any case, 32 bits are too few for a high-quality hash to be used in large-scale applications. 
	 * 
	 * <p><strong>Important</strong>: all bit vector implementations are required to return the value defined here.
	 * The simplest way to obtain this result is to subclass {@link AbstractBitVector}.
	 * 
	 * @return a hash code for this bit vector.
	 */
	public int hashCode();
	
	/** Returns a fast version of this bit vector.
	 * 
	 * <p>Different implementations of this interface might provide different level of efficiency.
	 * For instance, <em>views</em> on other data structures (e.g., strings) might implement
	 * {@link #getLong(long, long)} efficiently on multiples of {@link Long#SIZE}, but might
	 * fail to provide a generic, truly efficient random access.
	 * 
	 * <p>This method returns a (possibly immutable) bit vector with the same content as
	 * that of this bit vector. However, the returned bit vector is guaranteed to provide fast random access.
	 * 
	 * @return a fast version of this bit vector.
	 */
	public BitVector fast();
}
