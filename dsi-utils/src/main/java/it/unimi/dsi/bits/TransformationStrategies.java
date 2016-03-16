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


import it.unimi.dsi.fastutil.objects.AbstractObjectIterator;
import it.unimi.dsi.fastutil.objects.AbstractObjectList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.lang.MutableString;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/** A class providing static methods and objects that do useful things with transformation strategies.
 * 
 * @see TransformationStrategy
 */


public class TransformationStrategies {

	private final static TransformationStrategy<BitVector> IDENTITY = new TransformationStrategy<BitVector>() {
		private static final long serialVersionUID = 1L;

		public BitVector toBitVector( final BitVector object ) {
			return object;
		}
		
		public long numBits() { return 0; }

		public TransformationStrategy<BitVector> copy() {
			return this;
		}
		
		public Object readResolve() {
			return IDENTITY;
		}
	};
	
	
	/** A trivial transformation for data already in {@link BitVector} form. */
	@SuppressWarnings("unchecked")
	public static <T extends BitVector> TransformationStrategy<T> identity() {
		return (TransformationStrategy<T>)IDENTITY;
	}
 	
	private static final TransformationStrategy<CharSequence> UTF16 = new Utf16TransformationStrategy( false );
	
	/** A trivial transformation from strings to bit vectors that concatenates the bits of the UTF-16 representation.
	 * 
	 * <strong>Warning</strong>: bit vectors returned by this strategy are adaptors around the original string. If the string
	 * changes while the bit vector is being accessed, the results will be unpredictable.  
	 */
	@SuppressWarnings("unchecked")
	public static <T extends CharSequence> TransformationStrategy<T> utf16() {
		return (TransformationStrategy<T>)UTF16;
	}

	private static final TransformationStrategy<CharSequence> PREFIX_FREE_UTF16 = new Utf16TransformationStrategy( true );
	
	/** A trivial transformation from strings to bit vectors that concatenates the bits of the UTF-16 representation and completes
	 * the representation with an ASCII NUL to guarantee lexicographical ordering and prefix-freeness.
	 * 
	 * <p>Note that strings provided to this strategy must not contain ASCII NULs. 
	 * 
	 * <strong>Warning</strong>: bit vectors returned by this strategy are adaptors around the original string. If the string
	 * changes while the bit vector is being accessed, the results will be unpredictable.  
	 */
	@SuppressWarnings("unchecked")
	public static <T extends CharSequence> TransformationStrategy<T> prefixFreeUtf16() {
		return (TransformationStrategy<T>)PREFIX_FREE_UTF16;
	}

	private static class Utf16TransformationStrategy implements TransformationStrategy<CharSequence>, Serializable {
		private static final long serialVersionUID = 1L;
		/** Whether we should guarantee prefix-freeness by adding 0 to the end of each string. */
		private final boolean prefixFree;

		/** Creates a UTF16 transformation strategy. The strategy will map a string to its natural UTF16 bit sequence.
		 * 
		 * @param prefixFree if true, the resulting set of binary words will be made prefix free by adding 
		 */
		protected Utf16TransformationStrategy( boolean prefixFree ) {
			this.prefixFree = prefixFree;
		}
		
		private static class Utf16CharSequenceBitVector extends AbstractBitVector implements Serializable {
			private static final long serialVersionUID = 1L;
			private transient CharSequence s;
			private transient long length;
			private transient long actualEnd;

			public Utf16CharSequenceBitVector( final CharSequence s, final boolean prefixFree ) {
				this.s = s;
				actualEnd = s.length() * Character.SIZE;
				length = actualEnd + ( prefixFree ? Character.SIZE : 0 );
			}
			
			public boolean getBoolean( long index ) {
				if ( index > length ) throw new IndexOutOfBoundsException();
				if ( index >= actualEnd ) return false;
				final int charIndex = (int)( index / Character.SIZE );
				return ( s.charAt( charIndex ) & 0x8000 >>> index % Character.SIZE ) != 0; 
			}

			public long getLong( final long from, final long to ) {
				final int startBit = (int)( from % Long.SIZE );
				if ( startBit == 0 && to % Character.SIZE == 0 ) {
					if ( from == to ) return 0;
					long l;
					int pos = (int)( from / Character.SIZE );
					if ( to == from + Long.SIZE ) l = ( ( to > actualEnd ? 0 : (long)s.charAt( pos + 3 ) ) << 48 | (long)s.charAt( pos + 2 ) << 32 | (long)s.charAt( pos + 1 ) << 16 | s.charAt( pos ) );
					else {
						l = 0;
						final int residual = (int)( Math.min( actualEnd, to ) - from );
						for( int i = residual / Character.SIZE; i-- != 0; ) 
							l |= (long)s.charAt( pos + i ) << i * Character.SIZE; 
					}

					l = ( l & 0x5555555555555555L ) << 1 | ( l >>> 1 ) & 0x5555555555555555L;
					l = ( l & 0x3333333333333333L ) << 2 | ( l >>> 2 ) & 0x3333333333333333L;
					l = ( l & 0x0f0f0f0f0f0f0f0fL ) << 4 | ( l >>> 4 ) & 0x0f0f0f0f0f0f0f0fL;
					return ( l & 0x00ff00ff00ff00ffL ) << 8 | ( l >>> 8 ) & 0x00ff00ff00ff00ffL;
				}

				final long l = Long.SIZE - ( to - from );
				final long startPos = from - startBit;
				if ( l == Long.SIZE ) return 0;

				if ( startBit <= l ) return getLong( startPos, Math.min( length, startPos + Long.SIZE ) ) << l - startBit >>> l;
				return getLong( startPos, startPos + Long.SIZE ) >>> startBit | getLong( startPos + Long.SIZE, Math.min( length, startPos + 2 * Long.SIZE ) ) << Long.SIZE + l - startBit >>> l;
			}
			
			public long length() {
				return length;
			}
		}

		private static class Utf16MutableStringBitVector extends AbstractBitVector implements Serializable {
			private static final long serialVersionUID = 1L;
			private transient char[] a;
			private transient long length;
			private transient long actualEnd;

			public Utf16MutableStringBitVector( final MutableString s, final boolean prefixFree ) {
				this.a = s.array();
				actualEnd = s.length() * Character.SIZE;
				length = actualEnd + ( prefixFree ? Character.SIZE : 0 );
			}
			
			public boolean getBoolean( long index ) {
				if ( index > length ) throw new IndexOutOfBoundsException();
				if ( index >= actualEnd ) return false;
				final int charIndex = (int)( index / Character.SIZE );
				return ( a[ charIndex ] & 0x8000 >>> index % Character.SIZE ) != 0; 
			}

			public long getLong( final long from, final long to ) {
				final int startBit = (int)( from % Long.SIZE );
				if ( startBit == 0 && to % Character.SIZE == 0 ) {
					if ( from == to ) return 0;
					long l;
					int pos = (int)( from / Character.SIZE );
					if ( to == from + Long.SIZE ) l = ( ( to > actualEnd ? 0 : (long)a[ pos + 3 ] ) << 48 | (long)a[ pos + 2 ] << 32 | (long)a[ pos + 1 ] << 16 | a[ pos ] );
					else {
						l = 0;
						final int residual = (int)( Math.min( actualEnd, to ) - from );
						for( int i = residual / Character.SIZE; i-- != 0; ) 
							l |= (long)a[ pos + i ] << i * Character.SIZE; 
					}

					l = ( l & 0x5555555555555555L ) << 1 | ( l >>> 1 ) & 0x5555555555555555L;
					l = ( l & 0x3333333333333333L ) << 2 | ( l >>> 2 ) & 0x3333333333333333L;
					l = ( l & 0x0f0f0f0f0f0f0f0fL ) << 4 | ( l >>> 4 ) & 0x0f0f0f0f0f0f0f0fL;
					return ( l & 0x00ff00ff00ff00ffL ) << 8 | ( l >>> 8 ) & 0x00ff00ff00ff00ffL;
				}

				final long l = Long.SIZE - ( to - from );
				final long startPos = from - startBit;
				if ( l == Long.SIZE ) return 0;

				if ( startBit <= l ) return getLong( startPos, Math.min( length, startPos + Long.SIZE ) ) << l - startBit >>> l;
				return getLong( startPos, startPos + Long.SIZE ) >>> startBit | getLong( startPos + Long.SIZE, Math.min( length, startPos + 2 * Long.SIZE ) ) << Long.SIZE + l - startBit >>> l;
			}
			
			public long length() {
				return length;
			}
		}

		public BitVector toBitVector( final CharSequence s ) {
			return s instanceof MutableString ? new Utf16MutableStringBitVector( (MutableString)s, prefixFree ) : new Utf16CharSequenceBitVector( s, prefixFree );
		}

		public long numBits() { return 0; }

		public TransformationStrategy<CharSequence> copy() {
			return this;
		}
		
		private Object readResolve() {
			return prefixFree ? PREFIX_FREE_UTF16 : UTF16; 
		}
	}
	
	
	private static final TransformationStrategy<CharSequence> ISO = new ISOTransformationStrategy( false );

	/** A trivial transformation from strings to bit vectors that concatenates the lower eight bits of the UTF-16 representation.
	 * 
	 * <p>Note that this transformation is sensible only for strings that are known to be contain just characters in the ISO-8859-1 charset.
	 * 
	 * <strong>Warning</strong>: bit vectors returned by this strategy are adaptors around the original string. If the string
	 * changes while the bit vector is being accessed, the results will be unpredictable.  
	 */
	@SuppressWarnings("unchecked")
	public static <T extends CharSequence> TransformationStrategy<T> iso() {
		return (TransformationStrategy<T>)ISO;
	}

	private static final TransformationStrategy<CharSequence> PREFIX_FREE_ISO = new ISOTransformationStrategy( true );
	
	/** A trivial transformation from strings to bit vectors that concatenates the bits of the UTF-16 representation and completes
	 * the representation with an ASCII NUL to guarantee lexicographical ordering and prefix-freeness.
	 * 
	 * <p>Note that this transformation is sensible only for strings that are known to be contain just characters in the ISO-8859-1 charset, and
	 * that strings provided to this strategy must not contain ASCII NULs. 
	 * 
	 * <strong>Warning</strong>: bit vectors returned by this strategy are adaptors around the original string. If the string
	 * changes while the bit vector is being accessed, the results will be unpredictable.  
	 */
	@SuppressWarnings("unchecked")
	public static <T extends CharSequence> TransformationStrategy<T> prefixFreeIso() {
		return (TransformationStrategy<T>)PREFIX_FREE_ISO;
	}


	private static class ISOTransformationStrategy implements TransformationStrategy<CharSequence>, Serializable {
		private static final long serialVersionUID = 1L;
		/** Whether we should guarantee prefix-freeness by adding 0 to the end of each string. */
		private final boolean prefixFree;

		/** Creates an ISO transformation strategy. The strategy will map a string to the lowest eight bits of its natural UTF16 bit sequence.
		 * 
		 * @param prefixFree if true, the resulting set of binary words will be made prefix free by adding 
		 */
		protected ISOTransformationStrategy( boolean prefixFree ) {
			this.prefixFree = prefixFree;
		}
		
		private static class ISOCharSequenceBitVector extends AbstractBitVector implements Serializable {
			private static final long serialVersionUID = 1L;
			private transient CharSequence s;
			private transient long length;
			private transient long actualEnd;

			public ISOCharSequenceBitVector( final CharSequence s, final boolean prefixFree ) {
				this.s = s;
				actualEnd = s.length() * Byte.SIZE;
				length = actualEnd + ( prefixFree ? Byte.SIZE : 0 );
			}
			
			public boolean getBoolean( long index ) {
				if ( index > length ) throw new IndexOutOfBoundsException();
				if ( index >= actualEnd ) return false;
				final int byteIndex = (int)( index / Byte.SIZE );
				return ( s.charAt( byteIndex ) & 0x80 >>> index % Byte.SIZE ) != 0; 
			}

			public long getLong( final long from, final long to ) {
				//System.err.println ( from + "->" + to );
				final int startBit = (int)( from % Long.SIZE );
				if ( startBit == 0 && to % Byte.SIZE == 0 ) {
					if ( from == to ) return 0;
					long l;
					int pos = (int)( from / Byte.SIZE );
					if ( to == from + Long.SIZE ) 
						l = ( to > actualEnd ? 0 : ( s.charAt( pos + 7 ) & 0xFFL ) ) << 56 | 
								( s.charAt( pos + 6 ) & 0xFFL ) << 48 |
								( s.charAt( pos + 5 ) & 0xFFL ) << 40 |
								( s.charAt( pos + 4 ) & 0xFFL ) << 32 |
								( s.charAt( pos + 3 ) & 0xFFL ) << 24 |
								( s.charAt( pos + 2 ) & 0xFF ) << 16 |
								( s.charAt( pos + 1 ) & 0xFF ) << 8 |
								( s.charAt( pos ) & 0xFF );
					else {
						l = 0;
						final int residual = (int)( Math.min( actualEnd, to ) - from );
						for( int i = residual / Byte.SIZE; i-- != 0; ) 
							l |= ( s.charAt( pos + i ) & 0xFFL ) << i * Byte.SIZE; 
					}
					
					l = ( l & 0x5555555555555555L ) << 1 | ( l >>> 1 ) & 0x5555555555555555L;
					l = ( l & 0x3333333333333333L ) << 2 | ( l >>> 2 ) & 0x3333333333333333L;
					return ( l & 0x0f0f0f0f0f0f0f0fL ) << 4 | ( l >>> 4 ) & 0x0f0f0f0f0f0f0f0fL;
				}

				final long l = Long.SIZE - ( to - from );
				final long startPos = from - startBit;
				if ( l == Long.SIZE ) return 0;

				if ( startBit <= l ) return getLong( startPos, Math.min( length, startPos + Long.SIZE ) ) << l - startBit >>> l;
				return getLong( startPos, startPos + Long.SIZE ) >>> startBit | getLong( startPos + Long.SIZE, Math.min( length, startPos + 2 * Long.SIZE ) ) << Long.SIZE + l - startBit >>> l;
			}
			
			public long length() {
				return length;
			}
		}

		private static class ISOMutableStringBitVector extends AbstractBitVector implements Serializable {
			private static final long serialVersionUID = 1L;
			private transient char[] a;
			private transient long length;
			private transient long actualEnd;

			public ISOMutableStringBitVector( final MutableString s, final boolean prefixFree ) {
				this.a = s.array();
				actualEnd = s.length() * Byte.SIZE;
				length = actualEnd + ( prefixFree ? Byte.SIZE : 0 );
			}
			
			public boolean getBoolean( long index ) {
				if ( index > length ) throw new IndexOutOfBoundsException();
				if ( index >= actualEnd ) return false;
				final int byteIndex = (int)( index / Byte.SIZE );
				return ( a[ byteIndex ] & 0x80 >>> index % Byte.SIZE ) != 0; 
			}

			public long getLong( final long from, final long to ) {
				//System.err.println ( from + "->" + to );
				final int startBit = (int)( from % Long.SIZE );
				if ( startBit == 0 && to % Byte.SIZE == 0 ) {
					if ( from == to ) return 0;
					long l;
					int pos = (int)( from / Byte.SIZE );
					if ( to == from + Long.SIZE ) 
						l = ( to > actualEnd ? 0 : ( a[ pos + 7 ] & 0xFFL ) ) << 56 | 
								( a[ pos + 6 ] & 0xFFL ) << 48 |
								( a[ pos + 5 ] & 0xFFL ) << 40 |
								( a[ pos + 4 ] & 0xFFL ) << 32 |
								( a[ pos + 3 ] & 0xFFL ) << 24 |
								( a[ pos + 2 ] & 0xFF ) << 16 |
								( a[ pos + 1 ] & 0xFF ) << 8 |
								( a[ pos ] & 0xFF );
					else {
						l = 0;
						final int residual = (int)( Math.min( actualEnd, to ) - from );
						for( int i = residual / Byte.SIZE; i-- != 0; ) 
							l |= ( a[ pos + i ] & 0xFFL ) << i * Byte.SIZE; 
					}
					
					l = ( l & 0x5555555555555555L ) << 1 | ( l >>> 1 ) & 0x5555555555555555L;
					l = ( l & 0x3333333333333333L ) << 2 | ( l >>> 2 ) & 0x3333333333333333L;
					return ( l & 0x0f0f0f0f0f0f0f0fL ) << 4 | ( l >>> 4 ) & 0x0f0f0f0f0f0f0f0fL;
				}

				final long l = Long.SIZE - ( to - from );
				final long startPos = from - startBit;
				if ( l == Long.SIZE ) return 0;

				if ( startBit <= l ) return getLong( startPos, Math.min( length, startPos + Long.SIZE ) ) << l - startBit >>> l;
				return getLong( startPos, startPos + Long.SIZE ) >>> startBit | getLong( startPos + Long.SIZE, Math.min( length, startPos + 2 * Long.SIZE ) ) << Long.SIZE + l - startBit >>> l;
			}
			
			public long length() {
				return length;
			}
		}

		public BitVector toBitVector( final CharSequence s ) {
			return s instanceof MutableString ? new ISOMutableStringBitVector( (MutableString)s, prefixFree ) : new ISOCharSequenceBitVector( s, prefixFree );
		}

		public long numBits() { return 0; }

		public TransformationStrategy<CharSequence> copy() {
			return this;
		}
		
		private Object readResolve() {
			return prefixFree ? PREFIX_FREE_ISO : ISO; 
		}
	}
	
	
	
	private final static class IteratorWrapper<T> extends AbstractObjectIterator<BitVector> {
		final Iterator<T> iterator;
		final TransformationStrategy<? super T> transformationStrategy;
		
		public IteratorWrapper( final Iterator<T> iterator, final TransformationStrategy<? super T> transformationStrategy ) {
			this.iterator = iterator;
			this.transformationStrategy = transformationStrategy;
		}

		public boolean hasNext() {
			return iterator.hasNext();
		}

		public BitVector next() {
			return transformationStrategy.toBitVector( iterator.next() );
		}

	}
	
	
	/** Wraps a given iterator, returning an iterator that emits {@linkplain BitVector bit vectors}.
	 * 
	 * @param iterator an iterator.
	 * @param transformationStrategy a strategy to transform the object returned by <code>iterator</code>.
	 * @return an iterator that emits the content of <code>iterator</code> passed through <code>transformationStrategy</code>. 
	 */
	@SuppressWarnings("unchecked")
	public static <T> Iterator<BitVector> wrap( final Iterator<T> iterator, final TransformationStrategy<? super T> transformationStrategy ) {
		return (Iterator<BitVector>)( transformationStrategy == IDENTITY ? iterator : new IteratorWrapper<T>( iterator, transformationStrategy ) );
	}

	private final static class IterableWrapper<T> implements Iterable<BitVector> {
		private final TransformationStrategy<? super T> transformationStrategy;
		private final Iterable<T> collection;
		
		public IterableWrapper( final Iterable<T> collection, final TransformationStrategy<? super T> transformationStrategy ) {
			this.collection = collection;
			this.transformationStrategy = transformationStrategy;
		}

		public ObjectIterator<BitVector> iterator() {
			return new IteratorWrapper<T>( collection.iterator(), transformationStrategy.copy() );
		}
	}
	
	
	/** Wraps a given iterable, returning an iterable that contains {@linkplain BitVector bit vectors}.
	 * 
	 * @param iterable an iterable.
	 * @param transformationStrategy a strategy to transform the object contained in <code>iterable</code>.
	 * @return an iterable that has the content of <code>iterable</code> passed through <code>transformationStrategy</code>. 
	 */
	@SuppressWarnings("unchecked")
	public static <T> Iterable<BitVector> wrap( final Iterable<T> iterable, final TransformationStrategy<? super T> transformationStrategy ) {
		return (Iterable<BitVector>)( transformationStrategy == IDENTITY ? iterable : new IterableWrapper<T>( iterable, transformationStrategy ) );
	}

	private final static class ListWrapper<T> extends AbstractObjectList<BitVector> {
		private final TransformationStrategy<? super T> transformationStrategy;
		private final List<T> list;
		
		public ListWrapper( final List<T> list, final TransformationStrategy<? super T> transformationStrategy ) {
			this.list = list;
			this.transformationStrategy = transformationStrategy;
		}

		public BitVector get( int index ) {
			return transformationStrategy.toBitVector( list.get( index ) );
		}

		public int size() {
			return list.size();
		}
	}
	
	
	/** Wraps a given list, returning a list that contains {@linkplain BitVector bit vectors}.
	 * 
	 * @param list a list.
	 * @param transformationStrategy a strategy to transform the object contained in <code>list</code>.
	 * @return a list that has the content of <code>list</code> passed through <code>transformationStrategy</code>. 
	 */
	@SuppressWarnings("unchecked")
	public static <T> List<BitVector> wrap( final List<T> list, final TransformationStrategy<? super T> transformationStrategy ) {
		return (List<BitVector>)( transformationStrategy == IDENTITY ? list : new ListWrapper<T>( list, transformationStrategy ) );
	}
	

	
	private static class PrefixFreeTransformationStrategy implements TransformationStrategy<BitVector>, Serializable {
		private static final long serialVersionUID = 1L;
		
		private static class PrefixFreeBitVector extends AbstractBitVector implements Serializable {
			private static final long serialVersionUID = 1L;
			private transient BitVector v;
			private transient long length;

			public PrefixFreeBitVector( final BitVector v ) {
				this.v = v;
				length = v.length() * 2 + 1;
			}
			
			public boolean getBoolean( long index ) {
				if ( index >= length ) throw new IndexOutOfBoundsException();
				if ( index == length - 1 ) return false;
				if ( index % 2 == 0 ) return true;
				return v.getBoolean( index / 2 );
			}

			public long getLong( final long from, final long to ) {
				final int startBit = (int)( from % Long.SIZE );
				if ( startBit == 0 && to - from == Long.SIZE ) {
					long word = v.getLong(  from / 2, Math.min(  v.length(), from / 2 + Long.SIZE ) );
					if ( from % 2 != 0 ) word >>>= Long.SIZE / 2;
					else word &= 0xFFFFFFFFL;
					word = ( word | word << 16 ) & 0x0000FFFF0000FFFFL;
					//System.err.println( Long.toHexString( word ) );
					word = ( word | word << 8 )  & 0x00FF00FF00FF00FFL;
					//System.err.println( Long.toHexString( word ) );
					word = ( word | word << 4 )  & 0x0F0F0F0F0F0F0F0FL;
					//System.err.println( Long.toHexString( word ) );
					word = ( word | word << 2 )  & 0x3333333333333333L;
					//System.err.println( Long.toHexString( word ) );
					word = ( word << 1 | word << 2 ) | 0x5555555555555555L;
					//System.err.println( Long.toHexString( word ) );
					return word;
				}
				
				// TODO: implement in a fast way the case startBit == 0, to == length.
				
				return super.getLong(  from, to );
			}
			
			public long length() {
				return length;
			}
		}

		public BitVector toBitVector( final BitVector v ) {
			return new PrefixFreeBitVector( v );
		}

		public long numBits() { return 0; }

		public TransformationStrategy<BitVector> copy() {
			return this;
		}
		
		private Object readResolve() {
			return PREFIX_FREE; 
		}
	}
	

	/** A transformation from bit vectors to bit vectors that guarantees that its results are prefix free.
	 * 
	 * <p>More in detail, we map 0 to 10, 1 to 11, and we add a 0 at the end of all strings.
	 * 
	 * <p><strong>Warning</strong>: bit vectors returned by this strategy are adaptors around the original string. If the string
	 * changes while the bit vector is being accessed, the results will be unpredictable.  
	 */
	@SuppressWarnings("unchecked")
	public static <T extends BitVector> TransformationStrategy<T> prefixFree() {
		return (TransformationStrategy<T>)PREFIX_FREE;
	}

	private static final TransformationStrategy<? extends BitVector> PREFIX_FREE = new PrefixFreeTransformationStrategy();
}
