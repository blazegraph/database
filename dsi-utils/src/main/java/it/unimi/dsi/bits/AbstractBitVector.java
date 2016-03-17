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

import it.unimi.dsi.fastutil.booleans.AbstractBooleanList;
import it.unimi.dsi.fastutil.longs.AbstractLongBidirectionalIterator;
import it.unimi.dsi.fastutil.longs.AbstractLongList;
import it.unimi.dsi.fastutil.longs.AbstractLongListIterator;
import it.unimi.dsi.fastutil.longs.AbstractLongSortedSet;
import it.unimi.dsi.fastutil.longs.LongBidirectionalIterator;
import it.unimi.dsi.fastutil.longs.LongComparator;
import it.unimi.dsi.fastutil.longs.LongListIterator;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;
import it.unimi.dsi.util.LongBigList;

import java.io.Serializable;
import java.util.List;
import java.util.NoSuchElementException;

/** An abstract implementation of a {@link BitVector}.
 * 
 * <P>This abstract implementation provides almost all methods: you have to provide just
 * {@link it.unimi.dsi.fastutil.booleans.BooleanList#getBoolean(int)} and
 * {@link java.util.List#size()}. No attributes are defined.
 * 
 * <P>Note that the integer-set view provided by {@link #asLongSet()} is not cached: if you
 * want to cache the result of the first call, you must do your own caching.
 * 
 * <p><strong>Warning</strong>: this class has several optimised methods
 * that assume that {@link #getLong(long, long)} is implemented efficiently when its
 * arguments are multiples of {@link Long#SIZE} (see, e.g., the implementation
 * of {@link #compareTo(BitVector)} and {@link #longestCommonPrefixLength(BitVector)}).
 * If you want speed up the processing of your own {@link BitVector} implementations,
 * just implement {@link #getLong(long, long)} so that it is fast under the above conditions. 
 */
public abstract class AbstractBitVector extends AbstractBooleanList implements BitVector {
	
    protected void ensureRestrictedIndex( final long index ) {
        if ( index < 0 )  throw new IndexOutOfBoundsException( "Index (" + index + ") is negative" );
        if ( index >= length() ) throw new IndexOutOfBoundsException( "Index (" + index + ") is greater than or equal to length (" + ( length() ) + ")" );
    }
	
    protected void ensureIndex( final long index ) {
        if ( index < 0 )  throw new IndexOutOfBoundsException( "Index (" + index + ") is negative" );
        if ( index > length() ) throw new IndexOutOfBoundsException( "Index (" + index + ") is greater than length (" + ( length() ) + ")" );
    }

    public void set( final int index ) { set( index, true ); }
	public void clear( final int index ) { set( index, false ); }
	public void flip( final int index ) { set( index, ! getBoolean( index ) ); }

	public void set( final long index ) { set( index, true ); }
	public void clear( final long index ) { set( index, false ); }
	public void flip( final long index ) { set( index, ! getBoolean( index ) ); }
	
	public void fill( final boolean value ) { for( long i = length(); i-- != 0; ) set( i, value ); }
	public void fill( final int value ) { fill( value != 0 ); }
	public void flip() { for( long i = length(); i-- != 0; ) flip( i ); }

	public void fill( final long from, final long to, final boolean value ) { BitVectors.ensureFromTo( length(), from, to ); for( long i = to; i-- != from; ) set( i, value ); }
	public void fill( final long from, final long to, final int value ) { fill( from, to, value != 0 ); }
	public void flip( final long from, final long to ) { BitVectors.ensureFromTo( length(), from, to ); for( long i = to; i-- != from; ) flip( i ); }

	public int getInt( final long index ) { return getBoolean( index ) ? 1 : 0; }
	public long getLong( final long from, final long to ) {
		if ( to - from > 64 ) throw new IllegalArgumentException( "Range too large for a long: [" + from + ".." + to + ")" );
		long result = 0;
		for( long i = from; i < to; i++ ) if ( getBoolean( i ) ) result |= 1L << i - from;
		return result;
	}
	public boolean getBoolean( final int index ) { return getBoolean( (long)index ); }

	public boolean removeBoolean( final int index ) { return removeBoolean( (long)index ); }
	public boolean set( final int index, final boolean value ) { return set( (long)index, value ); }
	public void add( final int index, final boolean value ) { add( (long)index, value ); }

	public boolean removeBoolean( final long index ) { throw new UnsupportedOperationException(); }
	public boolean set( final long index, final boolean value ) { throw new UnsupportedOperationException(); }
	public void add( final long index, final boolean value ) { throw new UnsupportedOperationException(); }
	
	public void set( final long index, final int value ) { set( index, value != 0 ); }
	public void add( final long index, final int value ) { add( index, value != 0 ); }
	public boolean add( final boolean value ) { add( length(), value ); return true; }
	public void add( final int value ) { add( value != 0 ); }

	public BitVector append( final long value, final int k ) {
		for( int i = 0; i < k; i++ ) add( ( value & 1L << i ) != 0 );
		return this;
	}

	public BitVector append( final BitVector bv ) {
		final long length = bv.length();
		final long l = length - length % Long.SIZE;
		
		int i;
		for( i = 0; i < l; i += Long.SIZE ) append( bv.getLong( i, i + Long.SIZE ), Long.SIZE );
		if ( i < length ) append( bv.getLong( i, length ), (int)( length - i ) );
		return this;
	}

	public BitVector copy() { return copy( 0, size() ); }

	public BitVector copy( final long from, final long to ) {
		BitVectors.ensureFromTo( length(), from, to );
		final long length = to - from;
		final long l = length - length % Long.SIZE;
		final long bits[] = new long[ (int)( ( length + Long.SIZE - 1 ) / Long.SIZE ) ];
		int i;
		for( i = 0; i < l; i += Long.SIZE ) bits[ i / Long.SIZE ] = getLong( from + i, from + i + Long.SIZE );
		if ( i < length ) bits[ i / Long.SIZE ] = getLong( from + i, to );
		return LongArrayBitVector.wrap( bits, length );
	}
	
	/** Returns an instance of {@link LongArrayBitVector} containing a copy of this bit vector.
	 *
	 * @return an instance of {@link LongArrayBitVector} containing a copy of this bit vector.
	 */
	
	public BitVector fast() {
		return copy();
	}
	
	public long count() {
		long c = 0;
		for( long i = length(); i-- != 0; ) c += getInt( i );
		return c;
	}
	
	public long firstOne() {
		return nextOne( 0 );
	}
	
	public long lastOne() {
		return previousOne( length() );
	}
	
	public long firstZero() {
		return nextZero( 0 );
	}
	
	public long lastZero() {
		return previousZero( length() );
	}
	
	public long nextOne( final long index ) {
		final long length = length();
		for( long i = index; i < length; i++ ) if ( getBoolean( i ) ) return i;
		return -1;
	}
	
	public long previousOne( final long index ) {
		for ( long i = index; i-- != 0; ) if ( getBoolean( i ) ) return i;
		return -1;
	}
	
	public long nextZero( final long index ) {
		final long length = length();
		for( long i = index; i < length; i++ ) if ( ! getBoolean( i ) ) return i;
		return -1;
	}
	
	public long previousZero( final long index ) {
		for ( long i = index; i-- != 0; ) if ( ! getBoolean( i ) ) return i;
		return -1;
	}
	
	public long longestCommonPrefixLength( final BitVector v ) {
		final long minLength = Math.min( length(), v.length() );
		final long l = minLength - minLength % Long.SIZE;
		long w0, w1;
		
		int i;
		for( i = 0; i < l; i += Long.SIZE ) {
			w0 = getLong( i, i + Long.SIZE );
			w1 = v.getLong( i, i + Long.SIZE );
			if ( w0 != w1 ) return i + Fast.leastSignificantBit( w0 ^ w1 );
		}

		w0 = getLong( i, minLength );
		w1 = v.getLong( i, minLength );

		if ( w0 != w1 ) return i + Fast.leastSignificantBit( w0 ^ w1 );
		return minLength;
	}	
	
	public BitVector and( final BitVector v ) {
		for( int i = Math.min( size(), v.size() ); i-- != 0; ) if ( ! v.getBoolean( i ) ) clear( i );
		return this;
	}
	
	public BitVector or( final BitVector v ) {
		for( int i = Math.min( size(), v.size() ); i-- != 0; ) if ( v.getBoolean( i ) ) set( i );
		return this;
	}

	public BitVector xor( final BitVector v ) {
		for( int i = Math.min( size(), v.size() ); i-- != 0; ) if ( v.getBoolean( i ) ) flip( i );
		return this;
	}

	public int size() {
		final long length = length();
		if ( length > Integer.MAX_VALUE ) throw new IllegalStateException( "The number of bits of this bit vector (" + length + ") exceeds Integer.MAX_INT" );
		return (int)length;
	}
	
	public void size( final int newSize ) {
		length( newSize );
	}
	
	public void clear() {
		length( 0 );
	}
	
	public BitVector replace( final BitVector bv ) {
		clear();
		final long fullBits = bv.length() - bv.length() % Long.SIZE;
		for( long i = 0; i < fullBits; i += Long.SIZE ) append( bv.getLong( i, i + Long.SIZE ), Long.SIZE );
		if ( bv.length() % Long.SIZE != 0 ) append( bv.getLong( fullBits, bv.length() ), (int)( bv.length() - fullBits ) );
		return this;
	}

	// TODO: implement using getLong()
	public boolean equals( final Object o ) {
		if ( ! ( o instanceof BitVector ) ) return false;
		BitVector v = (BitVector)o;
		long length = length();
		if ( length != v.length() ) return false;
		while( length-- != 0 ) if ( getBoolean( length ) != v.getBoolean( length ) ) return false;
		return true;
	}

	public int hashCode() {
		final long length = length();
		long fullLength = length - length % Long.SIZE;
		long h = 0x9e3779b97f4a7c13L ^ length;

		for( long i = 0; i < fullLength; i += Long.SIZE ) h ^= ( h << 5 ) + getLong( i, i + Long.SIZE ) + ( h >>> 2 );
		if ( length != fullLength ) h ^= ( h << 5 ) + getLong( fullLength, length ) + ( h >>> 2 );

		return (int)( ( h >>> 32 ) ^ h );
	}
	
	public long[] bits() {
		final long[] bits = new long[ (int)( ( length() + LongArrayBitVector.BITS_PER_WORD - 1 ) >> LongArrayBitVector.LOG2_BITS_PER_WORD ) ];
		final long length = length();
		for( int i = 0; i < length; i++ ) if ( getBoolean( i ) ) bits[ i >> LongArrayBitVector.LOG2_BITS_PER_WORD ] |= 1L << ( i & LongArrayBitVector.WORD_MASK ); 
		return bits;
	}
	
	/** An integer sorted set view of a bit vector. 
	 * 
	 * <P>This class implements in the obvious way an integer set view
	 * of a bit vector. The vector is enlarged as needed (i.e., when
	 * a one beyond the current size is set), but it is never shrunk.
	 */
	
	public static class LongSetView extends AbstractLongSortedSet implements LongSet, Serializable {

		protected final BitVector bitVector;
		private static final long serialVersionUID = 1L;
		private final long from;
		private final long to;
		
		public LongSetView( final BitVector bitVector, final long from, final long to ) {
			if ( from > to ) throw new IllegalArgumentException( "Start index (" + from + ") is greater than end index (" + to + ")" );
			this.bitVector = bitVector;
			this.from = from;
			this.to = to;
		}

		
		public boolean contains( final long index ) {
			if ( index < 0 ) throw new IllegalArgumentException( "The provided index (" + index + ") is negative" );
			if ( index < from || index >= to ) return false;
			return index < bitVector.size() && bitVector.getBoolean( index );
		}
		
		public boolean add( final long index ) {
			if ( index < 0 ) throw new IllegalArgumentException( "The provided index (" + index + ") is negative" );
			if ( index < from || index >= to ) return false;

			final int size = bitVector.size();
			if ( index >= size ) bitVector.length( index + 1 );
			final boolean oldValue = bitVector.getBoolean( index );
			bitVector.set( index );
			return ! oldValue;
		}

		public boolean remove( final long index ) {
			final int size = bitVector.size();
			if ( index >= size ) return false;
			final boolean oldValue = bitVector.getBoolean( index );
			bitVector.clear( index );
			return oldValue;
		}

		@Override
		public void clear() {
			bitVector.clear();
		}
		
		public int size() {
			// This minimisation is necessary for implementations not supporting long indices.
			final long size = bitVector.subVector( from, Math.min( to, bitVector.length() ) ).count(); 
			if ( size > Integer.MAX_VALUE ) throw new IllegalStateException( "Set is too large to return an integer size" );
			return (int)size;
		}
		
		public LongBidirectionalIterator iterator() {
			return iterator( 0 );
		}

		private final class LongSetViewIterator extends AbstractLongBidirectionalIterator {
			long pos, last = -1, nextPos = -1, prevPos = -1;

			private LongSetViewIterator( long from ) {
				pos = from;
			}

			public boolean hasNext() {
				if ( nextPos == -1 && pos < bitVector.length() ) nextPos = bitVector.nextOne( pos );
				return nextPos != -1;
			}

			public boolean hasPrevious() {
				if ( prevPos == -1 && pos > 0 ) prevPos = bitVector.previousOne( pos );
				return prevPos != -1;
			}

			public long nextLong() {
				if ( ! hasNext() ) throw new NoSuchElementException();
				last = nextPos;
				pos = nextPos + 1;
				nextPos = -1;
				return last;
			}

			public long previousLong() {
				if ( ! hasPrevious() ) throw new NoSuchElementException();
				pos = prevPos;
				prevPos = -1;
				return last = pos;	
			}

			public void remove() {
				if ( last == -1 ) throw new IllegalStateException();
				bitVector.clear( last );
			}
		}

		public LongBidirectionalIterator iterator( final long from ) {
			return new LongSetViewIterator( from );
		}
		
		public long firstLong() {
			return bitVector.nextOne( from );
		}

		public long lastLong() {
			return bitVector.previousOne( Math.min( bitVector.length(), to ) );
		}

		public LongComparator comparator() {
			return null;
		}
		
		public LongSortedSet headSet( final long to ) {
			return to < this.to ? new LongSetView( bitVector, from, to ) : this;
		}

		public LongSortedSet tailSet( final long from ) {
			return from > this.from ? new LongSetView( bitVector, from, to ) : this;
		}

		public LongSortedSet subSet( long from, long to ) {
			to = to < this.to ? to : this.to;
			from = from > this.from ? from : this.from;
			if ( from == this.from && to == this.to ) return this;
			return new LongSetView( bitVector, from, to );
		}
	}
	
	/** A list-of-integers view of a bit vector. 
	 * 
	 * <P>This class implements in the obvious way a view
	 * of a bit vector as a list of integers of given width. The vector is enlarged as needed (i.e., when
	 * adding new elements), but it is never shrunk.
	 */
	
	public static class LongBigListView extends AbstractLongList implements LongBigList, Serializable {
		private static final long serialVersionUID = 1L;
		/** The underlying bit vector. */
		protected final BitVector bitVector;
		/** The width in bit of an element of this list view. */
		protected final int width;
		/** A bit mask containing {@link #width} bits set to one. */
		protected final long fullMask;
		
		public LongBigListView( final BitVector bitVector, final int width ) {
			this.width = width;
			this.bitVector = bitVector;
			fullMask = width == Long.SIZE ? -1 : ( 1L << width ) - 1; 
		}
		
		public long length() {
			return width == 0 ? 0 : bitVector.length() / width;
		}

		public int size() {
			final long length = length();
			if ( length > Integer.MAX_VALUE ) throw new IllegalStateException( "The number of elements of this bit list (" + length + ") exceeds Integer.MAX_INT" );
			return (int)length;
		}
		
		public LongBigList length( final long newSize ) {
			bitVector.length( newSize * width );
			return this;
		}
		
		public void size( final int newSize ) {
			length( newSize );
		}
		
		private final class LongBigListIterator extends AbstractLongListIterator {
			private long pos = 0;
			public boolean hasNext() { return pos < length(); }
			public boolean hasPrevious() { return pos > 0; }

			@Override
			public long nextLong() {
				if ( ! hasNext() ) throw new NoSuchElementException();
				return getLong( pos++ ); 
			}

			@Override
			public long previousLong() {
				if ( ! hasPrevious() ) throw new NoSuchElementException();
				return getLong( --pos ); 
			}

			public int nextIndex() {
				if ( pos >= Integer.MAX_VALUE ) throw new IllegalStateException( "The current list position is larger than Integer.MAX_VALUE" );
				return (int)pos;
			}

			public int previousIndex() {
				if ( pos > Integer.MAX_VALUE + 1L ) throw new IllegalStateException( "The current list position is larger than Integer.MAX_VALUE" );
				return (int)( pos - 1 );
			}
		}

		@Override
		public LongListIterator listIterator() {
			return new LongBigListIterator();
		}

		public void add( int index, long value ) {
			add( (long)index, value );
		}

		public void add( long index, long value ) {
			if ( width != Long.SIZE && value > fullMask ) throw new IllegalArgumentException();
			for( int i = 0; i < width; i++ ) bitVector.add( ( value & 1L << i ) != 0 );
		}

		public long getLong( long index ) {
			final long start = index * width;
			return bitVector.getLong( start, start + width );
		}

		public long getLong( int index ) {
			return getLong( (long)index );
		}

		// TODO
		public long removeLong( final long index ) {
			throw new UnsupportedOperationException();
		}

		public long set( long index, long value ) {
			if ( width != Long.SIZE && value > fullMask ) throw new IllegalArgumentException();
			long oldValue = getLong( index );
			final long start = index * width;
			for( int i = width; i-- != 0; ) bitVector.set( i + start, ( value & 1L << i ) != 0 );
			return oldValue;
		}

		public long set( int index, long value ) {
			return set( (long)index, value );
		}

		public LongBigList subList( long from, long to ) {
			return bitVector.subVector( from * width, to * width ).asLongBigList( width );
		}
	}
		
	public BitVector length( long newLength ) {
		final long length = length();
		if ( length < newLength ) for( long i = newLength - length; i-- != 0; ) add( false );
		else for( long i = length; i-- != newLength; ) removeBoolean( i );
		return this;
	}

	public LongSortedSet asLongSet() {
		return new LongSetView( this, 0, Long.MAX_VALUE );
	}	
	
	public LongBigList asLongBigList( final int width ) {
		return new LongBigListView( this, width );
	}	
	
	public SubBitVector subList( final int from, final int to ) {
		return new SubBitVector( this, from, to );
	}

	public BitVector subVector( final long from, final long to ) {
		return new SubBitVector( this, from, to );
	}

	public BitVector subVector( final long from ) {
		return subVector( from, length() );
	}

	@Override
	public int compareTo( final List<? extends Boolean> list ) {
		if ( list instanceof BitVector ) return compareTo( (BitVector)list );
		return super.compareTo( list );
	}
	
	public int compareTo( final BitVector v ) {
		final long minLength = Math.min( length(), v.length() );
		final long l = minLength - minLength % Long.SIZE;
		long w0, w1, xor;
		
		int i;
		for( i = 0; i < l; i += Long.SIZE ) {

			w0 = getLong( i, i + Long.SIZE );
			w1 = v.getLong( i, i + Long.SIZE );
			xor = w0 ^ w1;
			if ( xor != 0 ) return ( xor & -xor & w0 ) == 0 ? -1 : 1;
		}

		w0 = getLong( i, minLength );
		w1 = v.getLong( i, minLength );
		xor = w0 ^ w1;
		if ( xor != 0 ) return ( xor & -xor & w0 ) == 0 ? -1 : 1;

		return Long.signum( length() - v.length() );
	}	
	

	/** Returns a string representation of this vector.
	 * 
	 * <P>Note that this string representation shows the bit of index 0 at the leftmost position.
	 * @return a string representation of this vector, with the bit of index 0 on the left.
	 */
	
	public String toString() {
		final StringBuffer s = new StringBuffer();
		final int size = size();
		for( int i = 0; i < size; i++ ) s.append( getInt( i ) );
		return s.toString();
	}

	/** A subvector of a given bit vector, specified by an initial and a final bit. */
	
	public static class SubBitVector extends AbstractBitVector implements BitVector {
		private static final long serialVersionUID = 1L;
		final protected BitVector bitVector;		
		protected long from;
		protected long to;
		
		public SubBitVector( final BitVector l, final long from, final long to ) {
			BitVectors.ensureFromTo( l.length(), from, to );
			this.from = from;
			this.to = to;
			bitVector = l;
		}
		
		public boolean getBoolean( final long index ) { return bitVector.getBoolean( from + index ); }
		public int getInt( final long index ) { return getBoolean( index ) ? 1 : 0; }
		public boolean set( final long index, final boolean value ) { return bitVector.set( from + index, value ); }
		public void set( final long index, final int value ) { set( index, value != 0 ); }
		public void add( final long index, final boolean value ) { bitVector.add( from + index, value ); to++; }
		public void add( final long index, final int value ) { add( index, value != 0 ); to++; }
		public void add( final int value ) { bitVector.add( to++, value ); }
		public boolean removeBoolean( final long index ) { to--; return bitVector.removeBoolean( from + index ); } 

		public BitVector copy( final long from, final long to ) {
			BitVectors.ensureFromTo( length(), from, to );
			return bitVector.copy( this.from + from, this.from + to );
		}
		
		public BitVector subVector( final long from, final long to ) {
			BitVectors.ensureFromTo( length(), from, to );
			return new SubBitVector( bitVector, this.from + from, this.from + to );
		}
		
		public long getLong( final long from, final long to ) {
			return bitVector.getLong( from + this.from, to + this.from );
		}
		
		public long length() {
			return to - from;
		}
	}
}
