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

import java.io.Serializable;

/** 
 * @deprecated Use {@link TransformationStrategies#utf16()} and {@link TransformationStrategies#prefixFreeUtf16()}.
 */

@Deprecated
public class Utf16TransformationStrategy implements TransformationStrategy<CharSequence>, Serializable {
	private static final long serialVersionUID = 1L;
	
	/** Creates a prefix-free UTF16 transformation strategy. The
	 * strategy will map a string to its natural UTF16 bit sequence, and the resulting set of binary words will be made prefix free by adding
	 * 
	 */
	public Utf16TransformationStrategy() {}
	
	private static class Utf16CharSequenceBitVector extends AbstractBitVector implements Serializable {
		private static final long serialVersionUID = 1L;
		private transient CharSequence s;
		private transient long length;
		private transient long actualEnd;

		public Utf16CharSequenceBitVector( final CharSequence s ) {
			this.s = s;
			actualEnd = s.length() * Character.SIZE;
			length = actualEnd + Character.SIZE;
		}
		
		public boolean getBoolean( long index ) {
			if ( index > length ) throw new IndexOutOfBoundsException();
			if ( index >= actualEnd ) return false;
			final int charIndex = (int)( index / Character.SIZE );
			return ( s.charAt( charIndex ) & 0x8000 >>> index % Character.SIZE ) != 0; 
		}

		public long getLong( final long from, final long to ) {
			if ( from % Long.SIZE == 0 && to % Character.SIZE == 0 ) {
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

			return super.getLong( from, to );
		}
		
		public long length() {
			return length;
		}
	}

	public BitVector toBitVector( final CharSequence s ) {
		return new Utf16CharSequenceBitVector( s );
	}

	public long numBits() { return 0; }

	public TransformationStrategy<CharSequence> copy() {
		return new Utf16TransformationStrategy();
	}
}
