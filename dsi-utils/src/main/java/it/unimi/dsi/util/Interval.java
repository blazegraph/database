package it.unimi.dsi.util;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2003-2009 Paolo Boldi and Sebastiano Vigna 
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
                                                                                                                                                            
import static it.unimi.dsi.util.Intervals.EMPTY_INTERVAL;
import it.unimi.dsi.fastutil.ints.AbstractIntSortedSet;
import it.unimi.dsi.fastutil.ints.IntBidirectionalIterator;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

import java.io.Serializable;
import java.util.NoSuchElementException;

                                                                                                                                                             
/** An interval of integers. An interval is defined
 *  by two integers, called its {@link #left} and {@link #right} 
 *  extremes, and contains all integers <var>x</var> such that
 *  {@link #left} &le; x &le; {@link #right}.
 *  
 * <P>This class has no constructor: use the static factory methods
 * {@link #valueOf(int, int)} and {@link #valueOf(int)}, instead.
 * 
 * <P>Instances of this class are immutable, and moreover implement
 * the {@link it.unimi.dsi.fastutil.ints.IntSortedSet} interface. The
 * {@linkplain #equals(Object) equality method} allows to check equality with
 * both sorted and non-sorted sets of integers. 
 * 
 * <P>To reduce garbage collection,
 * intervals made of one non-negative small points are precomputed and returned upon request. 
 */

public final class Interval extends AbstractIntSortedSet implements IntSortedSet, Serializable {
	private static final long serialVersionUID = 1L;
	/** One-point intervals between 0 (inclusive) and this number (exclusive) are generated
     * from a pre-computed array of instances. */
	private final static int MAX_SINGLE_POINT = 1024;
	/** The precomputed array of one-point intervals. */
	private final static Interval[] POINT_INTERVAL = new Interval[ MAX_SINGLE_POINT ];
	
	static {
		int i = MAX_SINGLE_POINT;
		while( i-- != 0 ) POINT_INTERVAL[ i ] = new Interval( i, i );
	}

	/** The left extreme of the interval. */
	public final int left;
	/** The right extreme of the interval. */
	public final int right;
                                                                                                                                                             
	/** Builds an interval with given extremes.
	 * 
	 * <P>You cannot generate an empty interval with this constructor. Use {@link Intervals#EMPTY_INTERVAL} instead.
	 * 
	 *  @param left the left extreme.
	 *  @param right the right extreme (which must be greater than 
	 *  or equal to the left extreme).
	 */
	protected Interval( final int left, final int right ) {
		this.left = left;
		this.right = right;
	}
                                                                                                                                                             
	/** Returns an interval with given extremes.
	 * 
	 * <P>You cannot obtain an empty interval with this factory method. Use {@link Intervals#EMPTY_INTERVAL} instead.
	 *
	 *  @param left the left extreme.
	 *  @param right the right extreme (which must be greater than 
	 *  or equal to the left extreme).
	 *  @return an interval with the given extremes.
	 */
	public static Interval valueOf( final int left, final int right ) {
		if ( left > right ) throw new IllegalArgumentException( "The left extreme (" + left + ") is greater than the right extreme (" + right + ")" );
		if ( left == right ) return valueOf( left );
		return new Interval( left, right );		
	}
                                                                                                                                                             
	/** Returns a one-point interval.
	 * 
	 * <P>You cannot obtain an empty interval with this factory method. Use {@link Intervals#EMPTY_INTERVAL} instead.
	 *
	 *  @param point a point.
	 *  @return a one-point interval 
	 */
	public static Interval valueOf( final int point ) {
		if ( point >= 0 && point < MAX_SINGLE_POINT ) return POINT_INTERVAL[ point ]; 
		return new Interval( point, point );
	}
                                                                                                                                                             
	/** Returns the interval length, that is, the number of integers 
	 *  contained in the interval.
	 * 
	 *  @return the interval length.
	 */
	public int length() {
		return right - left + 1;
	}
    
	/** An alias for {@link #length()}.
	 * @return the interval length.
	 */
	public int size() {
		return length();
	}
	
	/** Returns an iterator over the integers in this interval. 
	 * 
	 * @return an integer iterator over the elements in this interval.
	 */
	public IntBidirectionalIterator iterator() {
		if ( this == EMPTY_INTERVAL ) return IntIterators.EMPTY_ITERATOR;
		// Note that fromTo() does NOT include the last integer.
		return IntIterators.fromTo( left, right + 1 );
	}
	
	/** Returns an iterator over the integers in this interval larger than or equal to a given integer. 
	 *
	 * @param from the starting integer.
	 * @return an integer iterator over the elements in this interval.
	 */
	public IntBidirectionalIterator iterator( final int from ) {
		if ( this == EMPTY_INTERVAL ) return IntIterators.EMPTY_ITERATOR;
		// Note that fromTo() does NOT include the last integer.
		final IntBidirectionalIterator i = IntIterators.fromTo( left, right + 1 );
		if ( from > left ) i.skip( Math.min( length(), from - left ) );
		return i;
	}
	
	/** Checks whether this interval contains the specified integer.
	 * 
	 * @param x an integer.
	 * @return whether this interval contains <code>x</code>, that is,
	 * whether {@link #left} &le; <code>x</code> &le; {@link #right}.
	 */
	
	public boolean contains( final int x ) {
		return x >= left && x <= right;
	}
	
	/** Checks whether this interval contains the specified interval.
	 * 
	 * @param interval an interval.
	 * @return whether this interval contains (as a set) <code>interval</code>.
	 */
	
	public boolean contains( final Interval interval ) {
		if ( interval == EMPTY_INTERVAL ) return true;
		if ( this == EMPTY_INTERVAL ) return false;
		return left <= interval.left && interval.right <= right;
	}
	
	/** Checks whether this interval would contain the specified integer if enlarged in both
	 * directions by the specified radius.
	 * 
	 * @param x an integer.
	 * @param radius the radius.
	 * @return whether this interval enlarged by <code>radius</code> would contain <code>x</code>,
	 * e.g., whether {@link #left}&minus;<code>radius</code> &le; <code>x</code> &le; {@link #right}+<code>radius</code>.
	 */

	public boolean contains( final int x, final int radius ) {
		if ( this == EMPTY_INTERVAL ) throw new IllegalArgumentException();
		return x >= left - radius && x <= right + radius;
	}

	/** Checks whether this interval would contain the specified integer if enlarged in each
	 * direction with the respective radius.
	 * directions by the specified radius.
	 * 
	 * @param x an integer.
	 * @param leftRadius the left radius.
	 * @param rightRadius the right radius.
	 * @return whether this interval enlarged to the left by <code>leftRadius</code> 
	 * and to the right by <code>rightRadius</code> would contain <code>x</code>,
	 * e.g., whether {@link #left}&minus;<code>leftRadius</code> &le; <code>x</code> &le; {@link #right}+<code>rightRadius</code>.
	 */

	public boolean contains( final int x, final int leftRadius, final int rightRadius ) {
		if ( this == EMPTY_INTERVAL ) throw new IllegalArgumentException();
		return x >= left - leftRadius && x <= right + rightRadius;
	}

	
    
	/** Compares this interval to an integer.
	 * 
	 * @param x an integer.
	 * @return  a negative integer, zero, or a positive integer as <code>x</code> is positioned
	 * at the left, belongs, or is positioned to the right of this interval, e.g., 
	 * as <code>x</code> &lt; {@link #left},
	 * {@link #left} &le; <code>x</code> &le; {@link #right} or
	 * {@link #right} &lt; <code>x</code>.
	 */
	
	public int compareTo( final int x ) {
		if ( this == EMPTY_INTERVAL ) throw new IllegalArgumentException();
		if ( x < left ) return -1;
		if ( x > right ) return 1;
		return 0;
	}
	
	/** Compares this interval to an integer with a specified radius.
	 * 
	 * @param x an integer.
	 * @param radius the radius.
	 * @return  a negative integer, zero, or a positive integer as <code>x</code> is positioned
	 * at the left, belongs, or is positioned to the right of this interval enlarged by <code>radius</code>, that is, 
	 * as <code>x</code> &lt; {@link #left}&minus;<code>radius</code>,
	 * {@link #left}&minus;<code>radius</code> &le; <code>x</code> &le; {@link #right}+<code>radius</code> or
	 * {@link #right}+<code>radius</code> &lt; <code>x</code>.
	 */

	public int compareTo( final int x, final int radius ) {
		if ( this == EMPTY_INTERVAL ) throw new IllegalArgumentException();
		if ( x < left - radius ) return -1;
		if ( x > right + radius ) return 1;
		return 0;
	}
	
	/** Compares this interval to an integer with specified left and right radii.
	 * 
	 * @param x an integer.
	 * @param leftRadius the left radius.
	 * @param rightRadius the right radius.
	 * @return  a negative integer, zero, or a positive integer as <code>x</code> is positioned
	 * at the left, belongs, or is positioned to the right of this interval enlarged by <code>leftRadius</code>
	 * on the left and <code>rightRadius</code> in the right, that is, 
	 * as <code>x</code> &lt; {@link #left}&minus;<code>leftRadius</code>,
	 * {@link #left}&minus;<code>leftRadius</code> &le; <code>x</code> &le; {@link #right}+<code>rightRadius</code> or
	 * {@link #right}+<code>rightRadius</code> &lt; <code>x</code>.
	 */

	public int compareTo( final int x, final int leftRadius, final int rightRadius ) {
		if ( this == EMPTY_INTERVAL ) throw new IllegalArgumentException();
		if ( x < left - leftRadius ) return -1;
		if ( x > right + rightRadius ) return 1;
		return 0;
	}
	
	public IntComparator comparator() {
		return null;
	}
	
	public IntSortedSet headSet( final int to ) {
		if ( this == EMPTY_INTERVAL ) return this;
		if ( to > left ) return to > right ? this : valueOf( left, to - 1 );
		else return EMPTY_INTERVAL;
	}

	public IntSortedSet tailSet( final int from ) {
		if ( this == EMPTY_INTERVAL ) return this;
		if ( from <= right ) return from <= left ? this : valueOf( from, right );
		else return EMPTY_INTERVAL;
	}

	public IntSortedSet subSet( final int from, final int to ) {
		if ( this == EMPTY_INTERVAL ) return this;
		if ( from > to ) throw new IllegalArgumentException( "Start element (" + from  + ") is larger than end element (" + to + ")" );
		if ( to <= left || from > right || from == to ) return EMPTY_INTERVAL;
		if ( from <= left && to > right ) return this;
		return valueOf( Math.max( left, from ), Math.min( right, to - 1 ) );
	}

	public int firstInt() {
		if ( this == EMPTY_INTERVAL ) throw new NoSuchElementException();
		return left;
	}
	
	public int lastInt() {
		if ( this == EMPTY_INTERVAL ) throw new NoSuchElementException();
		return right;
	}

	public String toString() {
		if ( this == EMPTY_INTERVAL ) return "\u2205";
		if ( left == right ) return "[" + left + "]"; 
		return "[" + left + ".." + right + "]"; // Hoare's notation.
	}
                                            
	public int hashCode() {
		return left * 23 + right;
	}

	/** Checks whether this interval is equal to another set of integers.
	 *  
	 * @param o an object.
	 * @return true if <code>o</code> is an ordered set of integer containing
	 * the same element of this interval in the same order, or if <code>o</code>
	 * is a set of integers containing the same elements of this interval.
	 */
	
	public boolean equals( final Object o ) {
		if ( o instanceof Interval )
			return ((Interval)o).left == left && ((Interval)o).right == right;
		else if ( o instanceof IntSortedSet ) { // For sorted sets, we require the same order
			IntSortedSet s = (IntSortedSet) o;
			if ( s.size() != length() ) return false;
			int n = length();
			IntIterator i = iterator(), j = s.iterator();
			while( n-- != 0 ) if ( i.nextInt() != j.nextInt() ) return false;
			return true;
		}
		else if ( o instanceof IntSet ) { // For sets, we just require the same elements
			IntSet s = (IntSet) o;
			if ( s.size() != length() ) return false;
			int n = length();
			IntIterator i = iterator();
			while( n-- != 0 ) if ( ! s.contains( i.nextInt() ) ) return false;
			return true;
		}
		else return false;
	}
}
