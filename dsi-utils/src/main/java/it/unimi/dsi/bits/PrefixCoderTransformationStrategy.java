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

import it.unimi.dsi.compression.PrefixCodec;
import it.unimi.dsi.compression.PrefixCoder;
import it.unimi.dsi.fastutil.chars.Char2IntMap;
import it.unimi.dsi.fastutil.chars.Char2IntOpenHashMap;

/** A transformation strategy mapping strings using a {@linkplain PrefixCodec prefix-free encoder}. 
 * 
 * <p>The actual encoding must be provided via a map from characters to symbols, and a set
 * of codewords. The default return value of the map will be used for unknown characters.
 * 
 * <p>This strategy creates a new {@link LongArrayBitVector} each time {@link #toBitVector(CharSequence)} is invoked.
 */

public class PrefixCoderTransformationStrategy implements TransformationStrategy<CharSequence> {
	private static final long serialVersionUID = 1;
	
	protected final BitVector[] codeWord;
	protected final Char2IntOpenHashMap char2symbol;
	protected final boolean prefixFree;

	/** Create a new transformation strategy based on a prefix-free coder.
	 *  
	 * @param coder a prefix-free coder.
	 * @param char2symbol a map from character to symbols (the default returned value will be used for unknown symbols).
	 * @param prefixFree whether it is required that the resulting bit vectors are prefix-free: in this case, symbol 0 will
	 * be appended to each string, and will not be allowed to appear in any string.
	 */
	
	public PrefixCoderTransformationStrategy( final PrefixCoder coder, final Char2IntOpenHashMap char2symbol, final boolean prefixFree ) {
		this( coder.codeWords(), char2symbol, prefixFree );
	}

	protected PrefixCoderTransformationStrategy( final PrefixCoderTransformationStrategy transformationStrategy ) {
		this( transformationStrategy.codeWord, transformationStrategy.char2symbol, transformationStrategy.prefixFree );
	}

	protected PrefixCoderTransformationStrategy( final BitVector[] codeWord, final Char2IntOpenHashMap char2symbol, final boolean prefixFree ) {
		this.codeWord = codeWord;
		this.char2symbol = char2symbol;
		this.prefixFree = prefixFree;
	}

	public LongArrayBitVector toBitVector( final CharSequence s ) {
		final BitVector[] codeWord = this.codeWord;
		final Char2IntMap char2symbol = this.char2symbol;
		final int length = s.length();
		int numBits = prefixFree ? codeWord[ 0 ].size() : 0;
		
		for( int i = length; i-- != 0; ) numBits += codeWord[ char2symbol.get( s.charAt( i ) ) ].size();
		final LongArrayBitVector result = LongArrayBitVector.getInstance( numBits );
		for( int i = 0; i < s.length(); i++ ) result.append( codeWord[ char2symbol.get( s.charAt( i ) ) ] );
		if ( prefixFree ) result.append( codeWord[ 0 ] );
		return result;
	}
	
	public long numBits() {
		long numBits = 0;
		for( int i = codeWord.length; i-- != 0; ) numBits += codeWord[ i ].size();
		return numBits;
	}

	public PrefixCoderTransformationStrategy copy() {
		return new PrefixCoderTransformationStrategy( this );
	}
}
