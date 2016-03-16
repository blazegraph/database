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
 *  along with this  program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 */

import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.util.LongBigList;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/** A bit vector implementation based on arrays of longs.
 * 
 * <P>The main goal of this class is to be fast and flexible. It implements a lightweight, 
 * fast, open, optimized, reuse-oriented version of bit vectors. Instances of this class
 * represent a bit vector an array of longs that is enlarged as needed when new entries
 * are created (by dividing the current length by the golden ratio), but is
 * <em>never</em> made smaller (even on a {@link #clear()}). Use 
 * {@link #trim()} for that purpose.
 * 
 * <p>Besides usual methods for setting and getting bits, this class provides <em>views</em>
 * that make it possible to access comfortably the bit vector in different ways: for instance,
 * {@link #asLongBigList(int)} provide access as a list of longs, whereas
 * {@link #asLongSet()} provides access in setwise form.
 * 
 * <p>When enlarging the underlying array (e.g., for {@link #append(long, int)} operations or
 * add operations on the {@linkplain #asLongBigList(int) big list view}), or when
 * invoking {@link #ensureCapacity(long)}, this class calls
 * {@link LongArrays#grow(long[], int, int)}, which could enlarge the array more than
 * expected. On the contrary, {@link #length(long)} (and the corresponding method in the
 * {@linkplain #asLongBigList(int) big list view}) size the underlying array in an exact manner.
 * 
 * <P>Bit numbering follows the right-to-left convention: bit <var>k</var> (counted from the
 * right) of word <var>w</var> is bit 64<var>w</var> + <var>k</var> of the overall bit vector.
 *
 * <P>If {@link #CHECKS} is true at compile time, boundary checks for all bit operations
 * will be compiled in. For maximum speed, you may want to recompile this class with {@link #CHECKS}
 *  set to false. {@link #CHECKS} is public, so you can check from your code whether you're
 * being provided a version with checks or not.
 * 
 * <p><strong>Warning</strong>: A few optional methods have still to be implemented (e.g.,
 * adding an element at an arbitrary position using the list view).
 * 
 * <p><strong>Warning</strong>: In some cases, you might want to cache locally the result
 * of {@link #bits()} to speed up computations on immutable bit vectors (this is what happens, for instance,
 * in <a href="http://sux.dsi.unimi.it/docs/it/unimi/dsi/sux4j/bits/Rank.html">static ranking structures</a>). This class, however, does its own serialisation
 * of the bit vector: as a result, all cached references to the result of {@link #bits()}
 * must be marked as transient and rebuilt at deserialisation
 * time, or you will end up saving the bits twice. 
 */

// TODO: take care that unused bits are zeroes

// TODO: implement subVector properly

public class LongArrayBitVector extends AbstractBitVector implements Cloneable, Serializable {
	private static final long serialVersionUID = 1L;
	public final static int LOG2_BITS_PER_WORD = 6;
	public final static int BITS_PER_WORD = 1 << LOG2_BITS_PER_WORD;
	public final static int WORD_MASK = BITS_PER_WORD - 1;
	public final static int LAST_BIT = BITS_PER_WORD - 1;
	public final static long ALL_ONES = 0xFFFFFFFFFFFFFFFFL;
	public final static long LAST_BIT_MASK = 1L << LAST_BIT;
	
	/** Whether this class has been compiled with index checks or not. */
	public  final static boolean CHECKS = true;
	private static final boolean ASSERTS = false;
	
	/** The number of bits in this vector. */
	protected long length;
	/** The backing array of this vector. Bit 0 of the first element contains bit 0 of the bit vector, 
	 * bit 1 of the second element contains bit {@link #BITS_PER_WORD} of the bit vector and so on. */
	protected transient long[] bits;

	/** Returns the number of words that are necessary to hold the given number of bits.
	 * 
	 * @param size a number of bits.
	 * @return the number of words that are necessary to hold the given number of bits.
	 */
	
	protected final static int numWords( final long size ) {
		if ( ASSERTS ) assert ( size + WORD_MASK ) >>> LOG2_BITS_PER_WORD <= Integer.MAX_VALUE;
		return (int)( ( size + WORD_MASK ) >>> LOG2_BITS_PER_WORD );
	}

	/** Return the index of the word that holds a bit of specified index.
	 * 
	 * @param index the index of a bit, or -1.
	 * @return the index of the word that holds the bit of given index, or -1 
	 * if <code>index</code> is -1.
	 */
	protected final static int word( final long index ) {
		if ( ASSERTS ) assert index >>> LOG2_BITS_PER_WORD <= Integer.MAX_VALUE;
		return (int)( index >> LOG2_BITS_PER_WORD );
	}

	/** Returns the inside-word index of the bit that would hold the bit of specified index.
	 * 
	 * <P>Note that bit 0 is positioned in word 0, index 0, bit 1 in word 0, index 1, &hellip;,
	 * bit {@link #BITS_PER_WORD} in word 0, index 0, bit {@link #BITS_PER_WORD} + 1 in word 1, index 1,
	 * and so on.
	 * 
	 * @param index the index of a bit.
	 * @return the inside-word index of the bit that would hold the bit of specified index.
	 */
	protected final static int bit( final long index ) {
		return (int)( index & WORD_MASK );
	}

	/** Returns a mask having a 1 exactly at the bit {@link #bit(long) bit(index)}.
	 * 
	 * @param index the index of a bit
	 * @return a mask having a 1 exactly at the bit {@link #bit(long) bit(index)}.
	 */
	
	protected final static long mask( final long index ) {
		return 1L << ( index & WORD_MASK );
	}

	protected LongArrayBitVector( final long capacity ) {
		this.bits = capacity > 0 ? new long[ numWords( capacity ) ] : LongArrays.EMPTY_ARRAY;
	}

	/** Creates a new empty bit vector of given capacity. The 
	 * resulting vector will be able to contain <code>capacity</code>
	 * bits without reallocations of the backing array.
	 * 
	 * <P>Note that this constructor creates an <em>empty</em> bit vector.
	 * If you want a cleared bit vector of a specified size, please
	 * use the {@link #ofLength(long)} factory method.
	 * 
	 * @param capacity the capacity (in bits) of the new bit vector.
	 * @return a new bit vector of given capacity.
	 */
	public static LongArrayBitVector getInstance( final long capacity ) {
		return new LongArrayBitVector( capacity );
	}

	/** Creates a new empty bit vector. No allocation is actually performed. 
	 * @return a new bit vector with no capacity.
	 */
	public static LongArrayBitVector getInstance() {
		return new LongArrayBitVector( 0 );
	}
	
	/** Creates a new empty bit vector of given length.
	 * 
	 * @param length the size (in bits) of the new bit vector.
	 */
	public static LongArrayBitVector ofLength( final long length ) {
		return new LongArrayBitVector( length ).length( length );
	}
	
	/** Creates a new bit vector with given bits. 
	 * 
	 * @param bit a list of bits that will be set in the newly created bit vector. 
	 */
	public static LongArrayBitVector of( final int... bit ) {
		final LongArrayBitVector bitVector = new LongArrayBitVector( bit.length );
		for( int b : bit ) bitVector.add( b );
		return bitVector;
	}
	
	public long[] bits() {
		return bits;
	}
	
	public long length() {
		return length;
	}
	
	/** Ensures that this bit vector can hold the specified number of bits.
	 * 
	 * <p>This method uses {@link LongArrays#grow(long[], int, int)} to
	 * ensure that there is enough space for the given number of bits. As a
	 * consequence, the actual length of the long array allocated might be
	 * larger than expected.
	 * 
	 * @param numBits the number of bits that this vector must be able to contain. 
	 * @return this bit vector.
	 */
	
	public LongArrayBitVector ensureCapacity( final long numBits ) {
		bits = LongArrays.grow( bits, numWords( numBits ), numWords( length ) );
		return this;
	}

	public LongArrayBitVector length( final long newLength ) {
		bits = LongArrays.ensureCapacity( bits, numWords( newLength ), numWords( length ) );
		final long oldLength = length;
		if ( newLength < oldLength ) fill( newLength, oldLength, false );
		length = newLength;
		return this;
	}

	@Override
	public void fill( final boolean value ) {
		final int fullWords = (int)( length / Long.SIZE );
		LongArrays.fill( bits, 0, fullWords, value ? 0xFFFFFFFFFFFFFFFFL : 0L );
		if ( length % Long.SIZE != 0 ) {
			if ( value ) bits[ fullWords ] = ( 1L << length % Long.SIZE ) - 1;
			else bits[ fullWords ] = 0;
		}
	}

	@Override
	public void fill( final long from, final long to, final boolean value ) {
		if ( to / Long.SIZE == from / Long.SIZE ) {
			if ( value ) bits[ (int)( from / Long.SIZE ) ] |= ( 1L << to - from ) - 1 << from; 
			else bits[ (int)( from / Long.SIZE ) ] &= ~( ( 1L << to - from ) - 1 << from );
			return;
		}
		LongArrays.fill( bits, (int)( ( from + Long.SIZE - 1 ) / Long.SIZE ), (int)( to / Long.SIZE ), value ? -1L : 0L );
		if ( from % Long.SIZE != 0 ) {
			if ( value ) bits[ (int)( from / Long.SIZE ) ] |= -1L << from % Long.SIZE;
			else bits[ (int)( from / Long.SIZE ) ] &= ( 1L << from % Long.SIZE ) - 1;
		}

		if ( to % Long.SIZE != 0 ) {
			if ( value ) bits[ (int)( to / Long.SIZE ) ] |= ( 1L << to % Long.SIZE ) - 1;
			else bits[ (int)( to / Long.SIZE ) ] &= -1L << to % Long.SIZE;
		}
	}

	@Override
	public void flip() {
		final int fullWords = (int)( length / Long.SIZE );
		for( int i = fullWords; i-- != 0; ) bits[ i ] ^= 0xFFFFFFFFFFFFFFFFL;
		if ( length % Long.SIZE != 0 ) bits[ fullWords ] ^= ( 1L << length % Long.SIZE ) - 1;
	}

	/** Reduces as must as possible the size of the backing array.
	 * 
	 * @return true if some trimming was actually necessary.
	 */
	
	public boolean trim() {
		if ( bits.length == numWords( length ) ) return false;
		bits = LongArrays.setLength( bits, numWords( length ) );
		return true;
	}

	/** Sets the size of this bit vector to 0.
	 * <P>Note that this method does not try to reallocate that backing array.
	 * If you want to force that behaviour, call {@link #trim()} afterwards.
	 */
	public void clear() {
		LongArrays.fill( bits, 0, word( length - 1 ) + 1, 0 );
		length = 0;
	}

	public LongArrayBitVector copy( final long from, final long to ) {
		BitVectors.ensureFromTo( length, from, to );

		final LongArrayBitVector copy = new LongArrayBitVector( to - from );
		if ( ( copy.length = to - from ) == 0 ) return copy;
		
		final int numWords = numWords( to - from );
		final int startWord = word( from );
		final int startBit = bit( from );

		if ( startBit == 0 ) {
			// If we're copying from the first bit, we just copy the array. 
			System.arraycopy( bits, startWord, copy.bits, 0, numWords );
			final int endBit = bit( to );
			if ( endBit > 0 ) copy.bits[ numWords - 1 ] &= ( 1L << endBit ) - 1;
		}
		else if ( startWord == word( to - 1 ) ) {
			// Same word, startBit > 0
			copy.bits[ 0 ] = bits[ startWord ] >>> startBit & ( ( 1L << to - from ) - 1 );
		}
		else {
			final int bitsPerWordMinusStartBit = BITS_PER_WORD - startBit;
			final long[] bits = this.bits;
			final long[] copyBits = copy.bits;
			
			copyBits[ 0 ] = bits[ startWord ] >>> startBit;
			
			for( int word = 1; word < numWords; word++ ) {
				copyBits[ word - 1 ] |= bits[ word + startWord ] << bitsPerWordMinusStartBit;
				copyBits[ word ] = bits[ word + startWord ] >>> startBit;
			}
			final int endBit = bit( to - from );

			if ( endBit == 0 ) copyBits[ numWords - 1 ] |= bits[ numWords + startWord ] << bitsPerWordMinusStartBit;
			else {
				if ( endBit > bitsPerWordMinusStartBit ) copyBits[ numWords - 1 ] |= bits[ numWords + startWord ] << bitsPerWordMinusStartBit;
				copyBits[ numWords - 1 ] &= ( 1L << endBit ) - 1;
			}
		}
	
		return copy;
	}

	public LongArrayBitVector copy() {
		LongArrayBitVector copy = new LongArrayBitVector( length );
		copy.length = length;
		System.arraycopy( bits, 0, copy.bits, 0, numWords( length ) );
		return copy;
	}

	/** Returns this bit vector.
	 * 
	 * @return this bit vector.
	 */
	public LongArrayBitVector fast() {
		return this;
	}
	
	/** Returns a copy of the given bit vector.
	 * 
	 * <p>This method uses {@link BitVector#getLong(long, long)} on {@link Long#SIZE} boundaries to copy at high speed.
	 * 
	 * @param bv a bit vector.
	 * @return an instance of this class containing a copy of the given vector.
	 */
	public static LongArrayBitVector copy( final BitVector bv ) {
		final long length = bv.length();
		final LongArrayBitVector copy = new LongArrayBitVector( length );
		final long fullBits = length - length % Long.SIZE;
		for( long i = 0; i < fullBits; i += Long.SIZE ) copy.bits[ (int)( i / Long.SIZE ) ] = bv.getLong( i, i + Long.SIZE );
		if ( length % Long.SIZE != 0 ) copy.bits[ (int)( fullBits / Long.SIZE ) ] = bv.getLong( fullBits, length );
		copy.length = length;
		return copy;
	}
	
	public boolean getBoolean( final long index ) {
		if ( CHECKS ) ensureRestrictedIndex( index );
		return ( bits[ word( index ) ] & mask( index ) ) != 0;  
	}

	public boolean set( final long index, final boolean value ) {
		if ( CHECKS ) ensureRestrictedIndex( index );
		final int word = word( index );
		final long mask = mask( index );
		final boolean oldValue = ( bits[ word ] & mask ) != 0;
		if ( value ) bits[ word ] |= mask;
		else bits[ word ] &= ~mask;
		return oldValue != value;
	}

	public void set( final long index ) {
		if ( CHECKS ) ensureRestrictedIndex( index );
		bits[ word( index ) ] |= mask( index ); 
	}

	public void clear( final long index ) {
		if ( CHECKS ) ensureRestrictedIndex( index );
		bits[ word( index ) ] &= ~mask( index ); 
	}
	
	public void add( final long index, final boolean value ) {
		if ( CHECKS ) ensureIndex( index );
		if ( length == bits.length << LOG2_BITS_PER_WORD ) bits = LongArrays.grow( bits, numWords( length + 1 ) );
		
		length++;

		if ( index == length - 1 ) set( index, value );
		else {
			final int word = word( index );
			final int bit = bit( index );
			boolean carry = ( bits[ word ] & LAST_BIT_MASK ) != 0, nextCarry;
			long t = bits[ word ];
			if ( bit == LAST_BIT ) t &= ~LAST_BIT_MASK;
			else t = ( t & - ( 1L << bit ) ) << 1 | t & ( 1L << bit ) - 1;
			if ( value ) t |= 1L << bit;
			bits[ word ] = t;
			final int numWords = numWords( length );
			for( int i = word + 1; i < numWords; i++ ) {
				nextCarry = ( bits[ i ] & LAST_BIT_MASK ) != 0;
				bits[ i ] <<= 1;
				if ( carry ) bits[ i ] |= 1;
				carry = nextCarry;
			}
		}
			
		return;
	}
	
	public boolean removeBoolean( final long index ) {
		if ( CHECKS ) ensureRestrictedIndex( index );
		final boolean oldValue = getBoolean( index );
		final long[] bits = this.bits;

		final int word = word( index );
		final int bit = bit( index );
		bits[ word ] = ( bits[ word ] & - ( 1L << bit ) << 1 ) >>> 1 | bits[ word ] & ( 1L << bit ) - 1;
		final int numWords = numWords( length-- );
		for( int i = word + 1; i < numWords; i++ ) {
			if ( ( bits[ i ] & 1 ) != 0 ) bits[ i - 1 ] |= LAST_BIT_MASK;
			bits[ i ] >>>= 1;
		}

		return oldValue;
	}

	public LongArrayBitVector append( long value, int width ) {
		if ( width == 0 ) return this;
		if ( CHECKS && width < Long.SIZE && ( value & -1L << width ) != 0 ) throw new IllegalArgumentException( "The specified value (" + value + ") is larger than the maximum value for the given width (" + width + ")" );
		final long length = this.length;
		final int startWord = word( length );
		final int startBit = bit( length );
		ensureCapacity( length + width );

		if ( startBit + width <= Long.SIZE ) bits[ startWord ] |= value << startBit;
		else {
			bits[ startWord ] |= value << startBit;
			bits[ startWord + 1 ] = value >>> BITS_PER_WORD - startBit;
		}
		
		this.length += width;
		return this;
	}

	public long getLong( long from, long to ) {
		if ( CHECKS ) BitVectors.ensureFromTo( length, from, to );
		final long l = Long.SIZE - ( to - from );
		final int startWord = word( from );
		final int startBit = bit( from );
		if ( l == Long.SIZE ) return 0;
		
		if ( startBit <= l ) return bits[ startWord ] << l - startBit >>> l;
		return bits[ startWord ] >>> startBit | bits[ startWord + 1 ] << Long.SIZE + l - startBit >>> l;
	}

	public long count() {
		long c = 0;
		for( int i = numWords( length ); i-- != 0; ) c += Fast.count( bits[ i ] );
		return c;
	}

	public long nextOne( final long index ) {
		if ( index >= length ) return -1; 
		final long[] bits = this.bits;
		final long words = numWords( length );
		final int from = word( index );
		final long maskedFirstWord = bits[ from ] & -( 1L << bit( index ) );
		if ( maskedFirstWord != 0 ) return from * BITS_PER_WORD + Fast.leastSignificantBit( maskedFirstWord );

		for ( int i = from + 1; i < words; i++ ) 
			if ( bits[ i ] != 0 ) return i * BITS_PER_WORD + Fast.leastSignificantBit( bits[ i ] );
		return -1;
	}
	
	public long previousOne( final long index ) {
		if ( index == 0 ) return -1;
		final long[] bits = this.bits;
		final int from = word( index - 1 );
		final long mask = 1L << bit( index - 1 );
		final long maskedFirstWord = bits[ from ] & ( mask | mask - 1 );
		if ( maskedFirstWord != 0 ) return from * BITS_PER_WORD + Fast.mostSignificantBit( maskedFirstWord );

		for ( int i = from; i-- != 0; ) 
			if ( bits[ i ] != 0 ) return i * BITS_PER_WORD + Fast.mostSignificantBit( bits[ i ] );
		return -1;
	}
	
	public long nextZero( final long index ) {
		if ( index >= length ) return -1; 
		final long[] bits = this.bits;
		final long words = numWords( length );
		final int from = word( index );
		long maskedFirstWord = bits[ from ] | ( 1L << bit( index ) ) - 1;
		if ( maskedFirstWord != 0xFFFFFFFFFFFFFFFFL ) {
			final long result = from * BITS_PER_WORD + Fast.leastSignificantBit( ~maskedFirstWord );
			return result >= length ? -1 : result;
		}

		for ( int i = from + 1; i < words; i++ ) 
			if ( bits[ i ] != 0xFFFFFFFFFFFFFFFFL ) {
				final long result = i * BITS_PER_WORD + Fast.leastSignificantBit( ~bits[ i ] );
				return result >= length ? -1 : result;
			}
		return -1;
	}
	
	public long previousZero( final long index ) {
		if ( index == 0 ) return -1;
		final long[] bits = this.bits;
		final int from = word( index - 1 );
		long maskedFirstWord = bits[ from ] | -1L << bit( index );
		if ( from == word( length - 1 ) ) maskedFirstWord |= -1L << length % Long.SIZE;
		if ( maskedFirstWord != 0xFFFFFFFFFFFFFFFFL )
			return from * BITS_PER_WORD + Fast.mostSignificantBit( ~maskedFirstWord );

		for ( int i = from; i-- != 0; ) 
			if ( bits[ i ] != 0xFFFFFFFFFFFFFFFFL )
				return i * BITS_PER_WORD + Fast.mostSignificantBit( ~bits[ i ] );
		return -1;
	}
	
	public long longestCommonPrefixLength( final BitVector v ) {
		if ( v instanceof LongArrayBitVector ) return longestCommonPrefixLength( (LongArrayBitVector)v );
		return super.longestCommonPrefixLength( v );
	}

	public long longestCommonPrefixLength( final LongArrayBitVector v ) {
		final long minLength = Math.min( v.length(), length() );
		final long words = ( minLength + BITS_PER_WORD - 1 ) >> LOG2_BITS_PER_WORD;
		final long[] bits = this.bits;
		final long[] vBits = v.bits;
		
		for ( int i = 0; i < words; i++ ) 
			if ( bits[ i ] != vBits[ i ] ) 
				return Math.min( minLength, i * BITS_PER_WORD + Fast.leastSignificantBit( bits[ i ] ^ vBits[ i ] ) );
		return minLength;
	}

	public BitVector and( final BitVector v ) {
		if ( v instanceof LongArrayBitVector ) {
			LongArrayBitVector l = (LongArrayBitVector)v;
			int words = Math.min( numWords( length() ), numWords( l.length() ) );
			while( words-- != 0 ) bits[ words ] &= l.bits[ words ];
		}
		else super.and( v );
		return this;
	}
	
	public BitVector or( final BitVector v ) {
		if ( v instanceof LongArrayBitVector ) {
			LongArrayBitVector l = (LongArrayBitVector)v;
			int words = Math.min( numWords( length() ), numWords( l.length() ) );
			while( words-- != 0 ) bits[ words ] |= l.bits[ words ];
		}
		else super.or( v );
		return this;
	}

	public BitVector xor( final BitVector v ) {
		if ( v instanceof LongArrayBitVector ) {
			LongArrayBitVector l = (LongArrayBitVector)v;
			int words = Math.min( numWords( length() ), numWords( l.length() ) );
			while( words-- != 0 ) bits[ words ] ^= l.bits[ words ];
		}
		else super.xor( v );
		return this;
	}


	
	
	
	/** Wraps the given array of longs in a bit vector for the given number of bits.
	 * 
	 * <p>Note that all bits in <code>array</code> beyond that of index
	 * <code>size</code> must be unset, or an exception will be thrown.
	 * 
	 * @param array an array of longs.
	 * @param size the number of bits of the newly created bit vector.
	 * @return a bit vector of size <code>size</code> using <code>array</code> as backing array.
	 */
	public static LongArrayBitVector wrap( final long[] array, final long size ) {
		if ( size > array.length << LOG2_BITS_PER_WORD ) throw new IllegalArgumentException( "The provided array is too short (" + array.length + " elements) for the given size (" + size + ")" );
		final LongArrayBitVector result = new LongArrayBitVector( 0 );
		result.length = size;
		result.bits = array;
		
		final int arrayLength = array.length;
		final int lastWord = (int)( size / Long.SIZE ); 
		if ( lastWord < arrayLength && ( array[ lastWord ] & ~ ( ( 1L << size % Long.SIZE ) - 1 ) ) != 0 )  throw new IllegalArgumentException( "Garbage beyond size in bit array" );
		for( int i = lastWord + 1; i < arrayLength; i++ ) if ( array[ i ] != 0 ) throw new IllegalArgumentException( "Garbage beyond size in bit array" );
		return result;
	}

	/** Wraps the given array of longs in a bit vector.
	 * 
	 * @param array an array of longs.
	 * @return a bit vector of size <code>array.length * Long.SIZE</code> using <code>array</code> as backing array.
	 */
	public static LongArrayBitVector wrap( final long[] array ) {
		return wrap( array, array.length * Long.SIZE );
	}

	/** Returns a cloned copy of this bit vector.
	 * 
	 * <P>This method is functionally equivalent to {@link #copy()},
	 * except that {@link #copy()} trims the backing array.
	 * 
	 * @return a copy of this bit vector.
	 */
	public LongArrayBitVector clone() throws CloneNotSupportedException {
		LongArrayBitVector copy = (LongArrayBitVector)super.clone();
		copy.bits = bits.clone();
		return copy;
	}

	public LongArrayBitVector replace( final LongArrayBitVector bv ) {
		ensureCapacity( bv.length );
		final long[] bits = this.bits;
		final long[] bvBits = bv.bits;
		final int bvFirstFreeWord = word( bv.length - 1 ) + 1;
		for( int i = bvFirstFreeWord; i-- != 0; ) bits[ i ] = bvBits[ i ];
		final int thisFirstFreeWord = word( length - 1 ) + 1;
		if ( bvFirstFreeWord < thisFirstFreeWord ) LongArrays.fill( this.bits, bvFirstFreeWord, thisFirstFreeWord, 0 );
		this.length = bv.length;
		return this;
	}

	@Override
	public LongArrayBitVector replace( final BitVector bv ) {
		final long bvLength = bv.length();
		ensureCapacity( bvLength );
		final long[] bits = this.bits;
		final long fullBits = bvLength - bvLength % Long.SIZE;
		for( long i = 0; i < fullBits; i += Long.SIZE ) bits[ (int)( i / Long.SIZE ) ] = bv.getLong( i, i + Long.SIZE );
		final int bvFirstFreeWord = word( bvLength - 1 ) + 1;
		final int thisFirstFreeWord = word( length - 1 ) + 1; 
		if ( bvLength % Long.SIZE != 0 ) bits[ (int)( fullBits / Long.SIZE ) ] = bv.getLong( fullBits, bvLength );
		if ( bvFirstFreeWord < thisFirstFreeWord ) LongArrays.fill( this.bits, bvFirstFreeWord, thisFirstFreeWord, 0 );
		this.length = bvLength;
		return this;
	}

	public boolean equals( final Object o ) {
		if ( o instanceof LongArrayBitVector ) return equals( (LongArrayBitVector) o );
		return super.equals( o );
	}

	public boolean equals( final LongArrayBitVector v ) {
		if ( length != v.length() ) return false;
		int i = numWords( length );
		while( i-- != 0 ) if ( bits[ i ] != v.bits[ i ] ) return false;
		return true;
	}

	public int hashCode() {
		long h = 0x9e3779b97f4a7c13L ^ length;
		final int numWords = numWords( length );
		for( int i = 0; i < numWords; i++ ) h ^= ( h << 5 ) + bits[ i ] + ( h >>> 2 );
		if ( ASSERTS ) assert (int)( ( h >>> 32 ) ^ h ) == super.hashCode();
		return (int)( ( h >>> 32 ) ^ h );
	}
	
	/** A list-of-integers view of a bit vector. 
	 * 
	 * <P>This class implements in the obvious way a view
	 * of a bit vector as a list of integers of given width. The vector is enlarged as needed (i.e., when
	 * adding new elements), but it is never shrunk.
	 */
	
	protected static class LongBigListView extends AbstractBitVector.LongBigListView {
		private static final long serialVersionUID = 1L;
		@SuppressWarnings("hiding")
		final private LongArrayBitVector bitVector;
		
		public LongBigListView( final LongArrayBitVector bitVector, final int width ) {
			super( bitVector, width );
			this.bitVector = bitVector;
		}
		
		@Override
		public boolean add( long value ) {
			bitVector.append( value, width );
			return true;
		}

		@Override
		public long getLong( long index ) {
			final long start = index * width;
			return bitVector.getLong( start, start + width );
		}

		@Override
		public void clear() {
			bitVector.clear();
		}
		
		@Override
		public long set( long index, long value ) {
			if ( width == 0 ) return 0;
			if ( width != Long.SIZE && value > fullMask ) throw new IllegalArgumentException( "Value too large: " + value );
			final long bits[] = bitVector.bits;
			final long start = index * width;
			final int startWord = word( start );
			final int endWord = word( start + width - 1 );
			final int startBit = bit( start );
			final long oldValue;

			if ( startWord == endWord ) {
				oldValue = bits[ startWord ] >>> startBit & fullMask;
				bits[ startWord ] &= ~ ( fullMask << startBit );
				bits[ startWord ] |= value << startBit;
				if ( ASSERTS ) assert value == ( bits[ startWord ] >>> startBit & fullMask );
			}
			else {
				// Here startBit > 0.
				oldValue = bits[ startWord ] >>> startBit | bits[ endWord ] << ( BITS_PER_WORD - startBit ) & fullMask;	
				bits[ startWord ] &= ( 1L << startBit ) - 1;
				bits[ startWord ] |= value << startBit;
				bits[ endWord ] &=  - ( 1L << width - BITS_PER_WORD + startBit );
				bits[ endWord ] |= value >>> BITS_PER_WORD - startBit;
				
				if ( ASSERTS ) assert value == ( bits[ startWord ] >>> startBit | bits[ endWord ] << ( BITS_PER_WORD - startBit ) & fullMask );
			}
			return oldValue;
		}
	}
	
	@Override
	public LongBigList asLongBigList( final int width ) {
		return new LongBigListView( this, width );
	}	
	
	private void writeObject( final ObjectOutputStream s ) throws IOException {
		s.defaultWriteObject();
		final int numWords = numWords( length );
		for( int i = 0; i < numWords; i++ ) s.writeLong( bits[ i ] );
	}

	private void readObject( final ObjectInputStream s ) throws IOException, ClassNotFoundException {
		s.defaultReadObject();
		final int numWords = numWords( length );
		bits = new long[ numWords ];
		for( int i = 0; i < numWords; i++ ) bits[ i ] = s.readLong();
	}

}
