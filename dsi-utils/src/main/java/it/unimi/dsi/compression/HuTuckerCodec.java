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
import it.unimi.dsi.fastutil.booleans.BooleanArrays;

import java.io.Serializable;

/** An implementation of the Hu&ndash;Tucker optimal lexicographical prefix-free code.
 * 
 * <p>The familiar Huffman coding technique can be extended so to preserve the order in which
 * symbols are given to the coder, in the sense that if <var>j</var>&lt;<var>k</var>, then the
 * <var>j</var>-th symbol will get a code lexicographically smaller than the one
 * assigned to the <var>k</var>-th symbol. This result can be obtained with a small loss in 
 * code length (for more details, see the third volume of <i>The Art of Computer Programming</i>).
 * 
 * <p>A Hu&ndash;Tucker coder is built given an array of frequencies corresponding to each
 * symbol. Frequency 0 symbols are allowed, but they will degrade the resulting code.
 * 
 * <p>The implementation of this class is rather inefficient, and the time required to build
 * a Hu&ndash;Tucker code is quadratic in the number of symbols.
 * An <i>O</i>(<var>n</var> log <var>n</var>) implementation
 * is possible, but it requires very sophisticated data structures.
 */
public class HuTuckerCodec implements PrefixCodec, Serializable {
	private static final boolean DEBUG = false;
	private static final long serialVersionUID = 2L;

	/** The number of symbols of this coder. */
	public final int size;
	/** The root of the decoding tree. */
	private final TreeDecoder.Node root;
	/** A cached singleton instance of the coder of this codec. */
	private final CodeWordCoder coder;
	/** A cached singleton instance of the decoder of this codec. */
	private final TreeDecoder decoder;


	/** A node to be used for the tree construction: it records both the level and the index. */
	private static final class LevelNode extends TreeDecoder.LeafNode {
		private static final long serialVersionUID = 1L;

		int level;

		private LevelNode( final int symbol ) {
			super( symbol );
		}

		private LevelNode() {
			super( -1 );
		}
	}	

	private static long[] intArray2LongArray( final int a[] ) {
		final long[] b = new long[ a.length ];
		for( int i = a.length; i-- != 0; ) b[ i ] = a[ i ];
		return b;
	}

	public HuTuckerCodec( final int[] frequency ) {
		this( intArray2LongArray( frequency ) );
	}
	
	public HuTuckerCodec( final long[] frequency ) {
		size = frequency.length;
		final boolean[] internal = new boolean[ size ];
		final boolean[] removed = new boolean[ size ];
		final long[] compoundFrequency = new long[ size ];
		final LevelNode[] externalNode = new LevelNode[ size ], node = new LevelNode[ size ];
		
		long currPri;
		int first, last, left, right, minLeft, minRight;
		LevelNode n;
		
		// We create a node with level information for each symbol
		for( int i = size; i-- != 0; ) {
			compoundFrequency[ i ] = frequency[ i ];
			node[ i ] = externalNode[ i ] = new LevelNode( i );
		}
		
		first = 0;
		last = size - 1;
		minLeft = 0;
		int currMinLeft; 

		// First selection phase (see Knuth)
		
		for( int i = size; --i != 0; ) {
			
			currMinLeft = minLeft = minRight = -1;
			currPri = Long.MAX_VALUE;

			while( removed[ first ] ) first++;
			while( removed[ last ] ) last--;
			
			right = first;
			
			assert right < last;
			
			while( right < last ) {
				
				left = currMinLeft = right;
				
				do {
					right++;
					
					if ( ! removed[ right ] ) {
						if ( compoundFrequency[ currMinLeft ] + compoundFrequency[ right ] < currPri ) {
							currPri = compoundFrequency[ currMinLeft ] + compoundFrequency[ right ];
							minLeft = currMinLeft;
							minRight = right;
						}
						
						if ( compoundFrequency[ right ] < compoundFrequency[ currMinLeft ] ) currMinLeft = right;
					}
				} while( ( removed[ right ] || internal[ right ] ) && right < last );
				
				assert right == last || ( ! removed[ right ] && ! internal[ right ] );
				assert left < right;				
				
			}
			
			internal[ minLeft ] = true;
			removed[ minRight ] = true;
			
			n = new LevelNode();
			n.left = node[ minLeft ];
			n.right = node[ minRight ];
			node[ minLeft ] = n;
						
			compoundFrequency[ minLeft ] += compoundFrequency[ minRight ];
		}
		
		// Recursive marking
		markRec( node[ minLeft ], 0 );
		
		// We now restart the aggregation process
		BooleanArrays.fill( removed, false );
		System.arraycopy( externalNode, 0, node, 0, size );
		int currLevel, leftLevel;
		
		first = 0;
		minLeft = 0;
		last = size - 1;
		
		for( int i = size; --i != 0; ) {
			
			while( removed[ first ] ) first++;
			while( removed[ last ] ) last--;
			
			left = first;
			currLevel = minLeft = minRight = -1;

			while( left < last ) {
				leftLevel = node[ left ].level;
				
				assert leftLevel > currLevel;
				
				for( right = left + 1; right <= last && removed[ right ]; right++ );
				
				assert right <= last;
				assert ! removed[ right ];
				
				if ( leftLevel == node[ right ].level ) {
					currLevel = leftLevel;
					minLeft = left;
					minRight = right;
				}
				
				do left++; while( left < last && ( removed[ left ] || node[ left ].level <= currLevel ) );
			}

			removed[ minRight ] = true;
			
			n = new LevelNode();
			n.left = node[ minLeft ];
			n.right = node[ minRight ];
			n.level = currLevel - 1;
			node[ minLeft ] = n;
		}

		root = rebuildTree( node[ minLeft ] );
		decoder = new TreeDecoder( root, size );
		coder = new CodeWordCoder( decoder.buildCodes() );

		if ( DEBUG ) {
			final BitVector[] codeWord = coder.codeWords();
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

	/** We scan recursively the tree, making a copy that uses lightweight nodes. */
	
	private TreeDecoder.Node rebuildTree( final LevelNode n ) {
		if ( n == null ) return null;

		if ( n.symbol != -1 ) return new TreeDecoder.LeafNode( n.symbol );
		
		TreeDecoder.Node newNode = new TreeDecoder.Node();
		newNode.left = rebuildTree( (LevelNode) n.left );
		newNode.right = rebuildTree( (LevelNode) n.right );
		
		return newNode;
	}

	/** Mark recursively the height of each node. */
	private void markRec( final LevelNode n, final int height ) {
		if ( n == null ) return;
		n.level = height;
		markRec( (LevelNode) n.left, height + 1 );
		markRec( (LevelNode) n.right, height + 1 );
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

	@Deprecated
	public PrefixCoder getCoder() { return coder(); }
	@Deprecated
	public Decoder getDecoder() { return decoder(); }
}
