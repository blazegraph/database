package it.unimi.dsi.compression;

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

import it.unimi.dsi.bits.BitVector;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;

/**
 * A fast coder based on a set of codewords of length at most 64.
 * <p>
 * Modified to report the longCodeWord[].
 */

final public class Fast64CodeWordCoder extends CodeWordCoder {
    private static final long serialVersionUID = 1L;
    /** An array parallel to {@link #codeWord} containing the codewords as longs (right aligned). */
    private final long[] longCodeWord;
    /** A cached array, parallel to {@link #longCodeWord}, of codewords length. */
    private final int[] length;

    public final long[] getLongCodeWord() {
        return longCodeWord;
    }
    
    /** Creates a new codeword-based coder using the given vector of codewords. The
     * coder will be able to encode symbols numbered from 0 to <code>codeWord.length-1</code>, included.
     * 
     * @param codeWord a vector of codewords.
     * @param longCodeWord the same codewords as those specified in <code>codeWord</code>, but
     * as right-aligned longs written in left-to-right fashion.
     */
    public Fast64CodeWordCoder( final BitVector[] codeWord, final long[] longCodeWord ) {
        super( codeWord );
        this.longCodeWord = longCodeWord;
        length = new int[ codeWord.length ];
        for( int i = length.length; i-- != 0; ) length[ i ] = codeWord[ i ].size();
    }

    @Override
    public int encode( final int symbol, final OutputBitStream obs ) throws IOException {
        return obs.writeLong( longCodeWord[ symbol ], length[ symbol ] );
    }
}
