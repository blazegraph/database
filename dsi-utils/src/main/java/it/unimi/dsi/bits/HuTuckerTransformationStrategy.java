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

import it.unimi.dsi.fastutil.chars.Char2IntMap;
import it.unimi.dsi.fastutil.chars.Char2IntOpenHashMap;
import it.unimi.dsi.compression.HuTuckerCodec;

import java.util.Iterator;

/** A transformation strategy mapping strings to their {@linkplain HuTuckerCodec Hu-Tucker encoding}. The
 * encoding is guaranteed to preserve lexicographical ordering. 
 */

public class HuTuckerTransformationStrategy extends PrefixCoderTransformationStrategy {
	private static final long serialVersionUID = 1;
	/** Creates a Hu-Tucker transformation strategy for the character sequences returned by the given iterable. The
	 * strategy will map a string to its Hu-Tucker encoding.
	 * 
	 * @param iterable an iterable object returning character sequences.
	 * @param prefixFree if true, the resulting set of binary words will be prefix free.
	 */
	public HuTuckerTransformationStrategy( final Iterable<? extends CharSequence> iterable, final boolean prefixFree ) {
		this( getCoder( iterable, prefixFree ), prefixFree );
	}
	
	protected HuTuckerTransformationStrategy( PrefixCoderTransformationStrategy huTuckerTransformationStrategy ) {
		super( huTuckerTransformationStrategy );
	}
	
	protected HuTuckerTransformationStrategy( Object[] a, boolean prefixFree ) {
		super( (BitVector[])a[ 0 ], (Char2IntOpenHashMap)a[ 1 ], prefixFree );
	}
		
	private static Object[] getCoder( final Iterable<? extends CharSequence> iterable, boolean prefixFree ) {
		// First of all, we gather frequencies for all Unicode characters
		long[] frequency = new long[ Character.MAX_VALUE + 1 ]; 
		int maxWordLength = 0;
		CharSequence s;
		int n = 0;

		for( Iterator<? extends CharSequence> i = iterable.iterator(); i.hasNext(); ) {
			s = i.next();
			maxWordLength = Math.max( s.length(), maxWordLength );
			for( int j = s.length(); j-- != 0; ) frequency[ s.charAt( j ) ]++;
			n++;
		}
		
		// Then, we compute the number of actually used characters. We count from the start the stop character.
		int count = prefixFree ? 1 : 0;
		for( int i = frequency.length; i-- != 0; ) if ( frequency[ i ] != 0 ) count++;

		/* Now we remap used characters in f, building at the same time the map from characters to symbols (except for the stop character). */
		long[] packedFrequency = new long[ count ];
		final Char2IntMap char2symbol = new Char2IntOpenHashMap( count );
		
		for( int i = frequency.length, k = count; i-- != 0; ) {
			if ( frequency[ i ] != 0 ) {
				packedFrequency[ --k ] = frequency[ i ];
				char2symbol.put( (char)i, k );
			}
		}
		
		if ( prefixFree ) packedFrequency[ 0 ] = n; // The stop character appears once in each string.
		
		// We now build the coder used to code the strings
		return new Object[] { new HuTuckerCodec( packedFrequency ).coder().codeWords(), char2symbol };
	}
	
	public PrefixCoderTransformationStrategy copy() {
		return new HuTuckerTransformationStrategy( this );
	}
}
