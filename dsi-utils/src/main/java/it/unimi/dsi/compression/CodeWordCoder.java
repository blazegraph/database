package it.unimi.dsi.compression;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2005-2009 Sebastiano Vigna 
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

import it.unimi.dsi.bits.BitVector;
import it.unimi.dsi.fastutil.booleans.BooleanIterator;
import it.unimi.dsi.fastutil.booleans.BooleanIterators;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;
import java.io.Serializable;

/** A coder based on a set of codewords. */

public class CodeWordCoder implements PrefixCoder, Serializable {
	private static final long serialVersionUID = 1L;
	/** The array of codewords of this coder. */
	protected final BitVector[] codeWord;
	
	/** Creates a new codeword-based coder using the given vector of codewords. The
	 * coder will be able to encode symbols numbered from 0 to <code>codeWord.length-1</code>, included.
	 * 
	 * @param codeWord a vector of codewords.
	 */
	public CodeWordCoder( final BitVector[] codeWord ) {
		this.codeWord = codeWord;
	}

	public BooleanIterator encode( final int symbol ) {
		return codeWord[ symbol ].iterator();
	}
	
	public int encode( final int symbol, final OutputBitStream obs ) throws IOException {
		final BitVector w = codeWord[ symbol ];
		final int size = w.size();
		for( int i = 0; i < size; i++ ) obs.writeBit( w.getBoolean( i ) );
		return size;
	}

	public int flush( final OutputBitStream unused ) { return 0; }

	public BooleanIterator flush() { return BooleanIterators.EMPTY_ITERATOR; }
	
	public BitVector[] codeWords() { return codeWord; }
}
