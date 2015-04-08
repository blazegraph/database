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

import it.unimi.dsi.fastutil.booleans.BooleanIterator;
import it.unimi.dsi.io.InputBitStream;

import java.io.IOException;
import java.io.Serializable;

/** A fast table-based decoder for canonical Huffman codes supporting only codes with limited (less than 64 bits) codewords. 
 * We use the technique described by Daniel S. Hirschberg and Debra A. Lelewer, &ldquo;Efficient Decoding of Prefix Codes&rdquo;, 
 * <i>Comm. ACM</i>, 33(4): 449&minus;459, 1990. */

final public class CanonicalFast64CodeWordDecoder implements Decoder, Serializable {
    private static final long serialVersionUID = 1L;
    
    /** The last codeword in each block of same-length codewords, plus one. */
    private final long[] lastCodeWordPlusOne;
    /** An array parallel to {@link #lastCodeWordPlusOne} specifying the increment in length between codeword lengths 
     * (without repetitions). In particular, the first entry
     * is the length of the first block of same-length codewords, the second entry is the difference in length
     * between the second and the first block of same-length codewords, and so on. */
    private final int[] lengthIncrement;
    /** An array parallel to {@link #lastCodeWordPlusOne} specifying how many codewords we have up to a certain block (included). */
    private final int[] howManyUpToBlock;
    /** The symbol assigned to each code word. */
    private final int[] symbol;
        
    /** Creates a new codeword-based decoder using the given vector of codewords lengths and
     * a symbol array.
     * 
     * @param codeWordLength a vector of nondecreasing codeword lengths suitable for a canonical code.
     * @param symbol a parallel array of symbols corresponding to each codeword length.
     */
    public CanonicalFast64CodeWordDecoder( final int[] codeWordLength, final int[] symbol ) {
        final int size = codeWordLength.length;
        this.symbol = symbol;
        
        // We compute how many different codeword lengths are present. We check also for excessive or nondecreasing length.
        int howManyLengths = 1;
        if ( size > 0 )
            for( int i = size - 1; i-- != 0; ) {
                if ( codeWordLength[ i ] > Long.SIZE ) throw new IllegalArgumentException( "Codeword length must not exceed 64" );
                if ( codeWordLength[ i ] > codeWordLength[ i + 1 ] ) throw new IllegalArgumentException( "Codeword lengths must be nondecreasing" );
                if ( codeWordLength[ i ] != codeWordLength[ i + 1 ] ) howManyLengths++; 
            }
        
        lengthIncrement = new int[ howManyLengths ];
        howManyUpToBlock = new int[ howManyLengths ];
        lastCodeWordPlusOne = new long[ howManyLengths ];
        
        int p = -1, l, prevL = 0;
        long word = 0;
        
        for( int i = 0; i < size; i++ ) {
            l = codeWordLength[ i ];
            if ( l != prevL ) {
                if ( i != 0 ) {
                    lastCodeWordPlusOne[ p ] = word;
                    howManyUpToBlock[ p ] = i; 
                }
                lengthIncrement[ ++p ] = l - prevL;
                word <<= l - prevL;
                prevL = l;
            }
            
            word++;
        }
        
        if ( p != -1 ) {
            howManyUpToBlock[ p ] = size;
            lastCodeWordPlusOne[ howManyLengths - 1 ] = word;
        }
        else {
             // This covers the case size = 1
            howManyUpToBlock[ 0 ] = 1;
            lastCodeWordPlusOne[ 0 ] = 1;
        }
    }

    /** Reads a specified number of bits from a Boolean iterator and stores them into a long.
     * 
     * @param iterator a Boolean iterator.
     * @param length the number of bits to read.
     * @return the bits read, stored into a long: the first read bit will be bit <code>length</code> &minus; 1.
     */
    private static long readLong( final BooleanIterator iterator, final int length ) {
        long x = 0;
        for( int i = length; i-- != 0; ) if ( iterator.nextBoolean() ) x |= 1L << i;
        return x;
    }
    
    public int decode( final BooleanIterator iterator ) {
        final int[] lengthIncrement = this.lengthIncrement;
        final long[] lastCodeWordPlusOne = this.lastCodeWordPlusOne;
        int curr = 0, l; 
        long x;

        x = readLong( iterator, lengthIncrement[ curr ] );
        
        for(;;) {
            if ( x < lastCodeWordPlusOne[ curr ] ) return symbol[ (int)( howManyUpToBlock[ curr ] - lastCodeWordPlusOne[ curr ] + x ) ];
            l = lengthIncrement[ ++curr ];
            x = x << l | readLong( iterator, l );
        }
    }

    public int decode( final InputBitStream ibs ) throws IOException {
        final int[] lengthIncrement = this.lengthIncrement;
        final long[] lastCodeWordPlusOne = this.lastCodeWordPlusOne;
        int curr = 0, l; 
        long x;

        x = ibs.readLong( lengthIncrement[ curr ] );
        
        for(;;) {
            if ( x < lastCodeWordPlusOne[ curr ] ) return symbol[ (int)( howManyUpToBlock[ curr ] - lastCodeWordPlusOne[ curr ] + x ) ];
            l = lengthIncrement[ ++curr ];
            if ( l == 1 ) x = x << 1 | ibs.readBit();
            else x = x << l | ibs.readLong( l );
        }
    }
}
