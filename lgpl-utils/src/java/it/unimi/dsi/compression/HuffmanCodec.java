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
import it.unimi.dsi.bits.LongArrayBitVector;

import java.io.Serializable;
import java.util.Arrays;

import cern.colt.Sorting;
import cern.colt.function.IntComparator;

/** An implementation of Huffman optimal prefix-free coding.
 * 
 * <p>A Huffman coder is built starting from an array of frequencies corresponding to each
 * symbol. Frequency 0 symbols are allowed, but they will degrade the resulting code.
 * 
 * <p>Instances of this class compute a <em>canonical</em> Huffman code 
 * (Eugene S. Schwartz and Bruce Kallick, &ldquo;Generating a Canonical Prefix Encoding&rdquo;, <i>Commun. ACM</i> 7(3), pages 166&minus;169, 1964), which can 
 * by {@linkplain CanonicalFast64CodeWordDecoder quickly decoded using table lookups}. 
 * The construction uses the most efficient one-pass in-place codelength computation procedure
 * described by Alistair Moffat and Jyrki Katajainen in &ldquo;In-Place Calculation of Minimum-Redundancy Codes&rdquo;,
 * <i>Algorithms and Data Structures, 4th International Workshop</i>, 
 * number 955 in Lecture Notes in Computer Science, pages 393&minus;402, Springer-Verlag, 1995.
 * 
 * <p>We note by passing that this coded uses a {@link CanonicalFast64CodeWordDecoder}, which does not support codelengths above 64.
 * However, since the worst case for codelengths is given by Fibonacci numbers, and frequencies are to be provided as integers,
 * no codeword longer than the base-[(5<sup>1/2</sup> + 1)/2] logarithm of 5<sup>1/2</sup> &#x00B7; 2<sup>31</sup> (less than 47) will ever be generated. 
 * <p>
 * <h3>Modifications</h3>
 * This class has been modified to expose the symbol[] in correlated order with
 * the codeWord bitLength[].  This information is only available when the ctor
 * is invoked directly. It is not serialized and therefore not available if an
 * instance of this class is deserialized.
 * */

public class HuffmanCodec implements PrefixCodec, Serializable {
    
	private static final boolean DEBUG = false;
	private static final boolean ASSERTS = false;
	private static final long serialVersionUID = 2L;

	/** The number of symbols of this coder. */
	public final int size;
	/** The codewords for this coder. */
	private final BitVector[] codeWord;
	/** A cached singleton instance of the coder of this codec. */
	private final Fast64CodeWordCoder coder;
	/** A cached singleton instance of the decoder of this codec. */
	private final CanonicalFast64CodeWordDecoder decoder;
	
	// Modified BBT 8/11/2009
	private transient int symbol[];
    /**
     * Return the symbol[] in the permuted order used to construct the
     * {@link CanonicalFast64CodeWordDecoder}.
     */
	public int[] getSymbols() {
	    return symbol;
	}

	/** Creates a new Huffman codec using the given vector of frequencies.
	 * 
	 * @param frequency a vector of nonnnegative frequencies.
	 */
	public HuffmanCodec( final int[] frequency ) {
		size = frequency.length;
		
		if ( size == 0 || size == 1 ) {
			codeWord = new BitVector[ size ];
			if ( size == 1 ) codeWord[ 0 ] = LongArrayBitVector.getInstance();
			coder = new Fast64CodeWordCoder( codeWord, new long[ size ] );
			// Modified BBT 8/11/2009
//            decoder = new CanonicalFast64CodeWordDecoder( new int[ size ], new int[ size ] );
			decoder = new CanonicalFast64CodeWordDecoder( new int[ size ], (symbol=new int[ size ]) );
			return;
        }
        
        final long[] a = new long[ size ];
        for( int i = size; i-- != 0; ) a[ i ] = frequency[ i ];
        // Sort frequencies (this is the only n log n step).
        Arrays.sort( a );
        
        // The following lines are from Moffat & Katajainen sample code. Please refer to their paper.
        
        // First pass, left to right, setting parent pointers.
		a[ 0 ] += a[ 1 ];
		int root = 0;
		int leaf = 2;
		for ( int next = 1; next < size - 1; next++ ) {
			// Select first item for a pairing.
			if ( leaf >= size || a[ root ] < a[ leaf ] ) {
				a[ next ] = a[ root ];
				a[ root++ ] = next;
			}
			else a[ next ] = a[ leaf++ ];

			// Add on the second item.
			if ( leaf >= size || ( root < next && a[ root ] < a[ leaf ] ) ) {
				a[ next ] += a[ root ];
				a[ root++ ] = next;
			}
			else a[ next ] += a[ leaf++ ];
		}

		// Second pass, right to left, setting internal depths.
		a[ size - 2 ] = 0;
		for ( int next = size - 3; next >= 0; next-- ) a[ next ] = a[ (int)a[ next ] ] + 1;

		// Third pass, right to left, setting leaf depths.
		int available = 1, used = 0, depth = 0;
		root = size - 2;
		int next = size - 1;
		while ( available > 0 ) {
			while ( root >= 0 && a[ root ] == depth ) {
				used++;
				root--;
			}
			while ( available > used ) {
				a[ next-- ] = depth;
				available--;
			}
			available = 2 * used;
			depth++;
			used = 0;
		}
		
		// Reverse the order of symbol lengths, and store them into an int array.
		final int[] length = new int[ size ];
		for( int i = size; i-- != 0; ) length[ i ] = (int)a[ size - 1 - i ];

		// Sort symbols indices by decreasing frequencies (so symbols correspond to lengths).
		final int[] symbol = new int[ size ];
		for( int i = size; i-- != 0; ) symbol[ i ] = i;
		Sorting.quickSort( symbol, 0, size, new IntComparator() {
			public int compare( int x, int y ) {
				return frequency[ y ] - frequency[ x ];
			}
		});
		
		// Assign codewords (just for the coder--the decoder needs just the lengths).
		int s = symbol[ 0 ];
		int l = length[ 0 ];
		long value = 0;
		BitVector v;
		codeWord = new BitVector[ size ];
		final long[] longCodeWord = new long[ size ];
		codeWord[ s ] = LongArrayBitVector.getInstance().length( l );
		
		for( int i = 1; i < size; i++ ) {
			s = symbol[ i ];
			if ( length[ i ] == l ) value++;
			else {
				value++;
				value <<= length[ i ] - l;
				if ( ASSERTS ) assert length[ i ] > l;
				l = length[ i ];
			}
			v = LongArrayBitVector.getInstance().length( l );
			for( int j = l; j-- != 0; ) if ( ( 1L << j & value ) != 0 ) v.set( l - 1 - j );
			codeWord[ s ] = v;
			longCodeWord[ s ] = value;
		}
		
		coder = new Fast64CodeWordCoder( codeWord, longCodeWord );
		// Modified BBT 8/11/2009
//        decoder = new CanonicalFast64CodeWordDecoder( length, symbol );
        decoder = new CanonicalFast64CodeWordDecoder( length, this.symbol = symbol );
		
		if ( DEBUG ) {
			final BitVector[] codeWord = codeWords();
			System.err.println( "Codes: " );
			for( int i = 0; i < size; i++ ) 
				System.err.println( i + " (" + codeWord[ i ].size() + " bits): " + codeWord[ i ] );
	
			long totFreq = 0;
			for( int i = size; i-- != 0; ) totFreq += frequency[ i ];
			long totBits = 0;
			for( int i = size; i-- != 0; ) totBits += frequency[ i ] * codeWord[ i ].size();
			System.err.println( "Compression: " + totBits + " / " + totFreq * Character.SIZE + " = " + (double)totBits/(totFreq * Character.SIZE) );
		}
}

	public CodeWordCoder coder() {
		return coder;
	}
	
	public Decoder decoder() {
		return decoder;
	}
	
	public int size() {
		return size;
	}

	public BitVector[] codeWords() {
		return coder.codeWords();
	}
}
