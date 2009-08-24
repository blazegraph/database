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
 * <ol><li>
 * This class has been modified to define an alternative ctor which exposes the
 * symbol[] in correlated order with the codeWord bitLength[] and the shortest
 * code word in the generated canonical.</li>
 * <li>
 * A method has been added to recreate the {@link PrefixCoder} from the 
 * shortest code word, the code word length[], and the symbol[].
 * </li>
 * </ol>
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

    /**
     * Class encapsulates the data necessary to reconstruct a
     * {@link CanonicalFast64CodeWordDecoder} or recreate the code.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
	public static class DecoderInputs {
	    
	    private BitVector shortestCodeWord;
	    private int symbol[];
	    private int length[];

        /**
         * Ctor may be passed to {@link HuffmanCodec} to obtain the assigned
         * length[] and symbol[] data and the shortest code word.  
         */
        public DecoderInputs() {
            
        }

        /**
         * Ctor may be used to explicitly populate an instance with the caller's
         * data.
         * 
         * @param shortestCodeWord
         * @param length
         * @param symbol
         */
        public DecoderInputs(final BitVector shortestCodeWord,
                final int[] length, final int[] symbol) {
            assert shortestCodeWord!=null;
            assert length!=null;
            assert symbol!=null;
            assert length.length==symbol.length;
            assert shortestCodeWord.size()==length[0];
            this.shortestCodeWord = shortestCodeWord;
            this.length = length;
            this.symbol = symbol;
	    }

        /**
         * The shortest code word. Note that canonical huffman codes can be
         * recreated from just length[0] and the shortest code word.
         */
        public BitVector getShortestCodeWord() {
            return shortestCodeWord;
        }
        
	    /**
	     * Return the symbol[] in the permuted order used to construct the
	     * {@link CanonicalFast64CodeWordDecoder}. This information is
	     * <em>transient</em>.
	     */
	    public int[] getSymbols() {
	        return symbol;
	    }

	    /**
	     * Return the codeWord bit lengths in the non-decreasing order used to
	     * construct the {@link CanonicalFast64CodeWordDecoder}. This information is
	     * <em>transient</em>.
	     */
	    public int[] getLengths() {
	        return length;
	    }

	}
	
    /** Creates a new Huffman codec using the given vector of frequencies.
     * 
     * @param frequency a vector of nonnnegative frequencies.
     */
    public HuffmanCodec( final int[] frequency ) {
    
        this(frequency, new DecoderInputs());
        
    }

    /**
     * Creates a new Huffman codec using the given vector of frequencies.
     * 
     * @param frequency
     *            a vector of nonnnegative frequencies.
     * @param decoderInputs
     *            The inputs necessary to reconstruct a
     *            {@link CanonicalFast64CodeWordDecoder} will be set on this
     *            object.
     */
	public HuffmanCodec( final int[] frequency, final DecoderInputs decoderInputs ) {
	    if(decoderInputs==null)
	        throw new IllegalArgumentException();
		size = frequency.length;
		
		if ( size == 0 || size == 1 ) {
			codeWord = new BitVector[ size ];
			if ( size == 1 ) codeWord[ 0 ] = LongArrayBitVector.getInstance();
			coder = new Fast64CodeWordCoder( codeWord, new long[ size ] );
			// Modified BBT 8/11/2009
//            decoder = new CanonicalFast64CodeWordDecoder( new int[ size ], new int[ size ] );
			decoderInputs.shortestCodeWord = LongArrayBitVector.getInstance().length( 0 );
			decoderInputs.length = new int[size];
			decoderInputs.symbol = new int[size];
			decoder = new CanonicalFast64CodeWordDecoder( decoderInputs.length, decoderInputs.symbol );
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
        decoderInputs.shortestCodeWord = codeWord[symbol[0]];
		decoderInputs.length = length;
        decoderInputs.symbol = symbol;
        assert decoderInputs.shortestCodeWord.size() == length[0] : "shortestCodeWord="
                + decoderInputs.shortestCodeWord
                + ", but length[0]="
                + length[0]; 
        decoder = new CanonicalFast64CodeWordDecoder( decoderInputs.length, decoderInputs.symbol);
		
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

    /**
     * (Re-)constructs the canonical huffman code from the shortest code word,
     * the non-decreasing bit lengths of each code word, and the permutation of
     * the symbols corresponding to those bit lengths. This information is
     * necessary and sufficient to reconstruct a canonical huffman code.
     * 
     * @param decoderInputs
     *            This contains the necessary and sufficient information to
     *            recreate the {@link PrefixCoder}.
     * 
     * @return A new {@link PrefixCoder} instance for the corresponding
     *         canonical huffman code.
     */
    static public PrefixCoder newCoder(final DecoderInputs decoderInputs) {

        return newCoder(decoderInputs.getShortestCodeWord(), decoderInputs
                .getLengths(), decoderInputs.getSymbols());

	}

    /**
     * (Re-)constructs the canonical huffman code from the shortest code word,
     * the non-decreasing bit lengths of each code word, and the permutation of
     * the symbols corresponding to those bit lengths. This information is
     * necessary and sufficient to reconstruct a canonical huffman code.
     * 
     * @param shortestCodeWord
     *            The code word with the shortest bit length.
     * @param length
     *            The bit length of each code word in the non-decreasing order
     *            assigned when the code was generated. The length of this array
     *            is the #of symbols in the code.
     * @param symbol
     *            The permutation of the symbols in the assigned when the
     *            canonical huffman code was generated. The length of this array
     *            is the #of symbols in the code.
     * 
     * @return A new {@link PrefixCoder} instance for the corresponding
     *         canonical huffman code.
     * 
     * @see DecoderInputs
     */
    static public PrefixCoder newCoder(final BitVector shortestCodeWord,
            final int[] length, final int[] symbol) {

        if (shortestCodeWord == null)
            throw new IllegalArgumentException();
        if (shortestCodeWord.size() == 0)
            throw new IllegalArgumentException();
        if (length == null)
            throw new IllegalArgumentException();
        if (length.length == 0)
            throw new IllegalArgumentException();
        if (symbol == null)
            throw new IllegalArgumentException();
        if (symbol.length == 0)
            throw new IllegalArgumentException();

        final int size = length.length;
        int s = symbol[ 0 ];
        int l = length[ 0 ];
        long value = 0;
        BitVector v;
        final BitVector[] codeWord = new BitVector[ size ];
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

        return new Fast64CodeWordCoder(codeWord, longCodeWord);

    }
    
}
