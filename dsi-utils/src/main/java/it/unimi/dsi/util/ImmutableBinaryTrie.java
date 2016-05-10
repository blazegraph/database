package it.unimi.dsi.util;

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
import it.unimi.dsi.bits.TransformationStrategy;
import it.unimi.dsi.fastutil.booleans.BooleanIterator;
import it.unimi.dsi.fastutil.objects.AbstractObject2LongFunction;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.lang.MutableString;

import java.io.Serializable;
import java.util.Iterator;
import java.util.ListIterator;

import cern.colt.bitvector.QuickBitVector;


/** An immutable implementation of binary tries.
 * 
 * <P>Instance of this class are built starting from a lexicographically ordered
 * list of {@link BitVector}s representing binary words. Each word
 * is assigned its position (starting from 0) in the list. The words are then organised in a
 * binary trie with path compression.
 * 
 * <p>Once the trie has been
 * built, it is possible to ask whether a word <var>w</var> is {@linkplain #get(BooleanIterator) contained in the trie}
 * (getting back its position in the list), the {@linkplain #getInterval(BooleanIterator) interval given by the words extending <var>w</var>} and the
 * {@linkplain #getApproximatedInterval(BooleanIterator) approximated interval defined by <var>w</var>}. 
 
 * @author Sebastiano Vigna
 * @since 0.9.2
 */

public class ImmutableBinaryTrie<T> extends AbstractObject2LongFunction<T> implements Serializable {
	
	private final static boolean ASSERTS = false;
	public static final long serialVersionUID = 1L;
	
	/** A node in the trie. */
	protected static class Node implements Serializable {
		private static final long serialVersionUID = 1L;
		public Node left, right;
		/** An array containing the path compacted in this node (<code>null</code> if there is no compaction at this node). */
		final public long[] path;
		/** The length of the path compacted in this node (0 if there is no compaction at this node). */
		final public int pathLength;
		/** If nonnegative, this node represent the <code>word</code>-th word. */
		final public int word ;
		
		/** Creates a node representing a word. 
		 * 
		 * <p>Note that the long array contained in <code>path</code> will be stored inside the node.
		 * 
		 * @param path the path compacted in this node, or <code>null</code> for the empty path.
		 * @param word the index of the word represented by this node.
		 */
		
		public Node( final BitVector path, final int word ) {
			if ( path == null ) {
				this.path = null;
				this.pathLength = 0;
			}
			else {
				this.path = path.bits();
				this.pathLength = path.size();
			}
			this.word = word;
		}
			
		/** Creates a node that does not represent a word. 
		 * 
		 * @param path the path compacted in this node, or <code>null</code> for the empty path.
		 */
		public Node( final BitVector path ) {
			this( path, -1 );
		}


		/** Returns true if this node is a leaf.
		 * 
		 * @return  true if this node is a leaf.
		 */
		public boolean isLeaf() {
			return right == null && left == null;
		}
		
		public String toString() {
			return "[" + path + ", " + word + "]";
		}
		
	}
		
	/** The root of the trie. */
	protected final Node root;
	/** The number of words in this trie. */
	private int size;
	private final TransformationStrategy<? super T> transformationStrategy;
	
	/** Creates a trie from a set of elements.
	 * 
	 * @param elements a set of elements
	 * @param transformationStrategy a transformation strategy that must turn <code>elements</code> into a list of
	 * distinct, lexicographically increasing (in iteration order) binary words.
	 */
	
	public ImmutableBinaryTrie( final Iterable<? extends T> elements, final TransformationStrategy<? super T> transformationStrategy ) {
		this.transformationStrategy = transformationStrategy;
		defRetValue = -1;
		// Check order
		final Iterator<? extends T> iterator = elements.iterator();
		final ObjectList<LongArrayBitVector> words = new ObjectArrayList<LongArrayBitVector>();
		int cmp;
		if ( iterator.hasNext() ) {
			final LongArrayBitVector prev = LongArrayBitVector.copy( transformationStrategy.toBitVector( iterator.next() ) );
			words.add( prev.copy() );
			BitVector curr;

			while( iterator.hasNext() ) {
				curr = transformationStrategy.toBitVector( iterator.next() );
				cmp = prev.compareTo( curr );
				if ( cmp == 0 ) throw new IllegalArgumentException( "The trie elements are not unique" );
				if ( cmp > 0 ) throw new IllegalArgumentException( "The trie elements are not sorted" );
				prev.replace( curr );
				words.add( prev.copy() );
			}
		}
		root = buildTrie( words, 0 );
	}

	/** Builds a trie recursively. 
	 * 
	 * <p>The trie will contain the suffixes of words in <code>words</code> starting at <code>pos</code>.
	 * 
	 * @param elements a list of elements.
	 * @param pos a starting position.
	 * @return a trie containing the suffixes of words in <code>words</code> starting at <code>pos</code>.
	 */
		
	protected Node buildTrie( final ObjectList<LongArrayBitVector> elements, final int pos ) {
		// TODO: on-the-fly check for lexicographical order
		
		if ( elements.size() == 0 ) return null;

		BitVector first = elements.get( 0 ), curr;
		int prefix = first.size(), change = -1, j;

		// We rule out the case of a single word (it would work anyway, but it's faster)
		if ( elements.size() == 1 ) return new Node( pos < prefix ? LongArrayBitVector.copy( first.subVector( pos, prefix ) ) : null, size++ );
		
		// 	Find maximum common prefix. change records the point of change (for splitting the word set).
		for( ListIterator<LongArrayBitVector> i = elements.listIterator( 1 ); i.hasNext(); ) {
			curr = i.next();
			
			if ( curr.size() < prefix ) prefix = curr.size(); 
			for( j = pos; j < prefix; j++ ) if ( first.get( j ) != curr.get( j ) ) break;
			if ( j < prefix ) {
				change = i.previousIndex();
				prefix = j;
			}
		}
		
		final Node n;
		if ( prefix == first.size() ) {
			// Special case: the first word is the common prefix. We must store it in the node,
			// and explicitly search for the actual change point, which is the first
			// word with prefix-th bit true.
			change = 1;
			for( ListIterator<LongArrayBitVector> i = elements.listIterator( 1 ); i.hasNext(); ) {
				curr = i.next();
				if ( curr.getBoolean( prefix ) ) break;
				change++;
			}
				
			n = new Node( prefix > pos ? LongArrayBitVector.copy( first.subVector( pos, prefix ) ) : null, size++ );
			n.left = buildTrie( elements.subList( 1, change ), prefix + 1 );
			n.right = buildTrie( elements.subList( change, elements.size() ), prefix + 1 );
		}
		else {
			n = new Node( prefix > pos ? LongArrayBitVector.copy( first.subVector( pos, prefix ) ) : null ); // There's some common prefix
			n.left = buildTrie( elements.subList( 0, change ), prefix + 1 );
			n.right = buildTrie( elements.subList( change, elements.size() ), prefix + 1 );
		}
		return n;
	}

	/** Returns the number of binary words in this trie.
	 * 
	 * @return the number of binary words in this trie.
	 */

	public int size() {
		return size;
	}
	
	@SuppressWarnings("unchecked")
	public long getIndex( final Object element ) {
		final BitVector word = transformationStrategy.toBitVector( (T)element );
		final int length = word.size();
		Node n = root;
			
		int pos = 0; // Current position in word
		long[] path;	
		
		while( n != null ) {
			if ( pos == length ) return n.word;

			path = n.path;
			if ( path != null ) {
				int minLength = Math.min( length - pos, n.pathLength ), i;
				for( i = 0; i < minLength; i++ ) if ( word.getBoolean( pos + i ) != QuickBitVector.get( path, i ) ) break;
				// Incompatible with current path.
				if ( i < minLength ) return -1;
			
				pos += i;

				// Completely contained in the current path (note that n.word == -1 if this is not a word).
				if ( pos == length ) return n.word;
			}

			n = word.getBoolean( pos++ ) ? n.right : n.left;	
		}

		return -1;
	}
	
	public long getLong( final Object element ) {
		final long result = getIndex( element );
		return result == -1 ? defRetValue : result;
	}
	
	public boolean containsKey( final Object element ) {
		return getIndex( element ) != -1;
	}
	
	
	/** Return the index of the word returned by the given iterator, or -1 if the word is not this trie.
	 * 
	 * @param iterator a boolean iterator that will be used to find a word in this trie.
	 * @return the index of the specified word, or -1 if the word returned by the iterator is not this trie.
	 * @see #getLong(Object)
	 */
	
	public int get( final BooleanIterator iterator ) {
		Node n = root;
		int pathLength;
		long[] path;
		
		while( n != null ) {
			if ( ! iterator.hasNext() ) return n.word;

			pathLength = n.pathLength;
			
			if ( pathLength != 0 ) {
				int i;
				path = n.path;
				for( i = 0; i < pathLength && iterator.hasNext(); i++ ) if ( iterator.nextBoolean() != QuickBitVector.get( path, i ) ) break;
				// Incompatible with current path.
				if ( i < pathLength ) return -1;
			
				// Completely contained in the current path (note that n.word == -1 if this is not a word).
				if ( ! iterator.hasNext() ) return n.word;
			}

			n = iterator.nextBoolean() ? n.right : n.left;	
			
		}

		return -1;
	}

	/** Returns an interval given by the smallest and the largest word in the trie starting with the specified word.
	 * 
	 * @param word a word.
	 * @return  an interval given by the smallest and the largest word in the trie 
	 * that start with <code>word</code> (thus, the {@linkplain Intervals#EMPTY_INTERVAL empty inteval}
	 * if no such words exist).
	 * @see #getInterval(BooleanIterator)
	 */
		
	public Interval getInterval( final BitVector word ) {
		final int length = word.size();
		Node n = root;
		long[] path;
	
		int pos = 0; // Current position in word
		
		while( n != null ) {
			// We found the current path: we go searching for left and right delimiters.
			if ( pos == length ) break;

			path = n.path;
			
			if ( path != null ) {
				int maxLength = Math.min( length - pos, n.pathLength );
				int i;
				for( i = 0; i < maxLength; i++ ) if ( word.getBoolean( pos + i ) != QuickBitVector.get( path, i ) ) break;
				// Incompatible with current path--we return the empty interval.
				if ( i < maxLength ) return Intervals.EMPTY_INTERVAL;
			
				pos += i;

				// Completely contained in the current path: we go searching for left and right delimiters.
				if ( pos == length ) break;
			}

			n = word.getBoolean( pos++ ) ? n.right : n.left;	
		}
		
		// If n == null, we did not found the path. Otherwise, it's the current node,
		// and we must search for left and right delimiters.
		if ( n == null ) return Intervals.EMPTY_INTERVAL;

		Node l = n;
		// Searching for the left extreme...
		while( l.word < 0 ) l = l.left != null ? l.left : l.right;
		// Searching for the right extreme, unless we're on a leaf.
		while( ! n.isLeaf() ) n = n.right != null ? n.right : n.left;
		
		return Interval.valueOf( l.word, n.word );
		
	}


	/** Returns an interval given by the smallest and the largest word in the trie starting with 
	 * the word returned by the given iterator.
	 * 
	 * @param iterator an iterator.
	 * @return  an interval given by the smallest and the largest word in the trie 
	 * that start with the word returned by <code>iterator</code> (thus, the {@linkplain Intervals#EMPTY_INTERVAL empty inteval}
	 * if no such words exist).
	 * @see #getInterval(BitVector)
	 */
		
	public Interval getInterval( final BooleanIterator iterator ) {
		Node n = root;
		boolean mismatch = false;
		long[] path;
		int pathLength;
		
		while( n != null ) {
			// We found the current path: we go searching for left and right delimiters.
			if ( ! iterator.hasNext() ) break;

			pathLength = n.pathLength;
			if ( pathLength != 0 ) {
				int i;
				path = n.path;
				for( i = 0; i < pathLength && iterator.hasNext(); i++ ) if ( ( mismatch = ( iterator.nextBoolean() != QuickBitVector.get( path, i ) ) ) ) break;
				// Incompatible with current path--we return the empty interval.
				if ( mismatch ) return Intervals.EMPTY_INTERVAL;
			
				// Completely contained in the current path: we go searching for left and right delimiters.
				if ( ! iterator.hasNext() ) break;
				
			}

			n = iterator.nextBoolean() ? n.right : n.left;
		}
		
		// If n == null, we did not found the path. Otherwise, it's the current node,
		// and we must search for left and right delimiters.
		if ( n == null ) return Intervals.EMPTY_INTERVAL;

		Node l = n;
		// Searching for the left extreme...
		while( l.word < 0 ) l = l.left != null ? l.left : l.right;
		// Searching for the right extreme, unless we're on a leaf.
		while ( ! n.isLeaf() ) n = n.right != null ? n.right : n.left;
		
		return Interval.valueOf( l.word, n.word );
		
	}

	
	/** Returns an approximated interval around the specified word.
	 * 
	 * <P>Given a word <var>w</var>, the corresponding approximated  interval is
	 * defined as follows: if the words in the approximator are thought of as left interval extremes in a
	 * larger lexicographically ordered set of words, and we number these word intervals using the
	 * indices of their left extremes, then the first word extending <var>w</var> would be in the
	 * word interval given by the left extreme of the interval returned by this method, whereas
	 * the last word extending <var>w</var> would be in the word interval given by the right
	 * extreme of the interval returned by this method. If no word in the larger set could possibly extend 
	 * <var>w</var> (because <var>w</var> is smaller than the lexicographically smallest word in the approximator) 
	 * the result is just an {@linkplain it.unimi.dsi.util.Intervals#EMPTY_INTERVAL empty interval}.
	 * 
	 * @param element an element.
	 * @return an approximated interval around the specified word.
	 * @see #getApproximatedInterval(BooleanIterator)  
	 */
		
	public Interval getApproximatedInterval( final T element ) {
		final BitVector word = transformationStrategy.toBitVector( element );
		final int length = word.size();
		Node n = root;
		long[] path;
		boolean exactMatch = false, mismatch = false, nextBit;
		
		int pos = 0; // Current position in word

		while( n != null ) {
			// We found the current path: we go searching for left and right delimiters.
			
			path = n.path;
			
			if ( pos == length ) {
				if ( n.word >= 0 && path == null ) exactMatch = true;
				break;
			}

			if ( path != null ) {
				int maxLength = Math.min( length - pos, n.pathLength );
				int i;
				for( i = 0; i < maxLength; i++ ) if ( mismatch = ( word.getBoolean( pos + i ) != QuickBitVector.get( path, i ) ) ) break;

				if ( mismatch ) {
					// System.err.println( "Exit 1" );
					// A mismatch. In this case, it is guaranteed that all
					// strings starting with the prefix examined so far lie
					// in a single block. The block index depends, however
					// on the bit that went wrong.
					if ( QuickBitVector.get( path, i ) ) {
						while( n.word < 0 ) n = n.left != null ? n.left : n.right;
						return n.word > 0 ? Interval.valueOf( n.word - 1 ) : Intervals.EMPTY_INTERVAL;
					}
					else {
						while( ! n.isLeaf() ) n = n.right != null ? n.right : n.left;
						return Interval.valueOf( n.word );
					}
				}

				pos += i;
				
				// Completely contained in the current path
				if ( pos == length ) {
					if ( ASSERTS ) assert n.pathLength == maxLength;
					if ( i == n.pathLength && n.word >= 0 ) exactMatch = true;
					break;
				}
				
			}

			if ( n.isLeaf() ) break;
			
			nextBit = word.getBoolean( pos++ );

			// We would like to take an impossible turn. This case is similar to
			// prefix mismatches, with subtly different off-by-ones.
			if ( nextBit && n.right == null ) {
				while( ! n.isLeaf() ) n = n.right != null ? n.right : n.left;
				return Interval.valueOf( n.word );
			}
			else if ( ! nextBit && n.left == null ) {
				while( n.word < 0 ) n = n.left != null ? n.left : n.right;
				return Interval.valueOf( n.word );
			}

			n = nextBit ? n.right : n.left;
		}
		
		Node l = n;
		// Searching for the left extreme...
		
		//System.err.println("Going for exit 2: l:" + l + " n:" + n);
		
		while( l.word < 0 ) l = l.left != null ? l.left : l.right;

		// Searching for the right extreme, unless we're on a leaf.
		while ( ! n.isLeaf() ) n = n.right != null ? n.right : n.left;
		
		// System.err.println("Following exit 2: l:" + l + " n:" + n);
		
		// If did not find an exact match and l.word is 0 we are lexicographically before every word.
		if ( pos == length && ! exactMatch ) {
			if ( l.word == 0 ) return mismatch ? Intervals.EMPTY_INTERVAL : Interval.valueOf( l.word, n.word );
			else return Interval.valueOf( l.word - 1, n.word );
		}
		
		// System.err.println( "Exit 2 (exactMatch: " + exactMatch +")" );
		return Interval.valueOf( l.word, n.word );
	}



	/** Returns an approximated prefix interval around the word returned by the specified iterator.
	 * 
	 * @param iterator an iterator.
	 * @return an approximated interval around the specified word: if the words in this trie
	 * are thought of as left interval extremes in a larger lexicographically ordered set of words,
	 * and we number these word intervals using the indices of their left extremes,
	 * then the first word extending <code>word</code> would be in the word interval given by
	 * the left extreme of the {@link Interval} returned by this method, whereas
	 * the last word extending <code>word</code> would be in the word
	 * interval given by the right extreme of the {@link Interval} returned by this method.
	 * @see #getApproximatedInterval(Object)
	 */
		
	public Interval getApproximatedInterval( final BooleanIterator iterator ) {
		Node n = root;
		long[] path;
		boolean exactMatch = false, mismatch = false, nextBit;
		
		for(;;) {
			// We found the current path: we go searching for left and right delimiters.
			
			path = n.path;
			
			if ( ! iterator.hasNext() ) {
				if ( n.word >= 0 && path == null ) exactMatch = true;
				break;
			}

			if ( path != null ) {
				int i;
				final int pathSize = n.pathLength;
				for( i = 0; i < pathSize && iterator.hasNext(); i++ ) if ( ( mismatch = ( iterator.nextBoolean() != QuickBitVector.get( path, i ) ) ) ) break;

				if ( mismatch ) {
					// System.err.println( "Exit 1" );
					// A mismatch. In this case, it is guaranteed that all
					// strings starting with the prefix examined so far lie
					// in a single block. The block index depends, however
					// on the bit that went wrong.
					if ( QuickBitVector.get( path, i ) ) {
						while( n.word < 0 ) n = n.left != null ? n.left : n.right;
						return n.word > 0 ? Interval.valueOf( n.word - 1 ) : Intervals.EMPTY_INTERVAL;
					}
					else {
						while( ! n.isLeaf() ) n = n.right != null ? n.right : n.left;
						return Interval.valueOf( n.word );
					}
				}

				// Completely contained in the current path
				if ( ! iterator.hasNext() ) {
					if ( i == pathSize && n.word >= 0 ) exactMatch = true;
					break;
				}
				
			}

			if ( n.isLeaf() ) break;
			
			nextBit = iterator.nextBoolean();
			
			// We would like to take an impossible turn. This case is similar to
			// prefix mismatches, with subtly different off-by-ones.
			if ( nextBit && n.right == null ) {
				while( ! n.isLeaf() ) n = n.right != null ? n.right : n.left;
				return Interval.valueOf( n.word );
			}
			else if ( ! nextBit && n.left == null ) {
				while( n.word < 0 ) n = n.left != null ? n.left : n.right;
				return Interval.valueOf( n.word );
			}
			
			n = nextBit ? n.right : n.left;
		}
		
		Node l = n;
		// Searching for the left extreme...
		
		//System.err.println("Going for exit 2: l:" + l + " n:" + n);
		
		while( l.word < 0 ) l = l.left != null ? l.left : l.right;
	
		// Searching for the right extreme, unless we're on a leaf.
		while ( ! n.isLeaf() ) n = n.right != null ? n.right : n.left;
		
		// If did not find an exact match and l.word is 0 we are lexicographically before every word.
		if ( ! iterator.hasNext() && ! exactMatch ) {
			if ( l.word == 0 ) return mismatch ? Intervals.EMPTY_INTERVAL : Interval.valueOf( 0 );
			else return Interval.valueOf( l.word - 1, n.word );
		}

		// System.err.println( "Exit 2 (hasNext: " +iterator.hasNext() + " exactMatch: " + exactMatch +")" );
		return Interval.valueOf( l.word, n.word );
	}

	
	
	private void recToString( final Node n, final MutableString printPrefix, final MutableString result, final MutableString path, final int level ) {
		if ( n == null ) return;
		
		//System.err.println( "Called with prefix " + printPrefix );
		
		result.append( printPrefix ).append( '(' ).append( level ).append( ')' );
		
		if ( n.path != null ) {
			path.append( LongArrayBitVector.wrap( n.path, n.pathLength ) );
			result.append( " path:" ).append( LongArrayBitVector.wrap( n.path, n.pathLength ) );
		}
		if ( n.word >= 0 ) result.append( " word: " ).append( n.word ).append( " (" ).append( path ).append( ')' );

		result.append( '\n' );
		
		path.append( '0' );
		recToString( n.left, printPrefix.append( '\t' ).append( "0 => " ), result, path, level + 1 );
		path.charAt( path.length() - 1, '1' ); 
		recToString( n.right, printPrefix.replace( printPrefix.length() - 5, printPrefix.length(), "1 => "), result, path, level + 1 );
		path.delete( path.length() - 1, path.length() ); 
		printPrefix.delete( printPrefix.length() - 6, printPrefix.length() );
		
		//System.err.println( "Path now: " + path + " Going to delete from " + ( path.length() - n.pathLength));
		
		path.delete( path.length() - n.pathLength, path.length() );
	}
	
	public String toString() {
		MutableString s = new MutableString();
		recToString( root, new MutableString(), s, new MutableString(), 0 );
		return s.toString();
	}
}
