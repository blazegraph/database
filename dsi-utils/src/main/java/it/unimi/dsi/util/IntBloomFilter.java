package it.unimi.dsi.util;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2006-2009 Sebastiano Vigna 
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

import it.unimi.dsi.bits.LongArrayBitVector;

import java.io.Serializable;
import java.util.Random;

import cern.jet.random.engine.MersenneTwister;

/** A Bloom filter for integers.
 * 
 * <P>Instances of this class represent a set of integers (with false positives)
 * using a Bloom filter. Because of the way Bloom filters work,
 * you cannot remove elements.
 *
 * <P>Bloom filters have an expected error rate, depending on the number
 * of hash functions used, on the filter size and on the number of elements in the filter. This implementation
 * uses a variable optimal number of hash functions, depending on the expected
 * number of elements. More precisely, a Bloom
 * filter for <var>n</var> integers with <var>d</var> hash functions will use 
 * ln 2 <var>d</var><var>n</var> &#8776; 1.44 <var>d</var><var>n</var> bits;
 * false positives will happen with probability 2<sup>-<var>d</var></sup>.  
 *
 * <P>Hash functions are generated at creation time using universal hashing. Each hash function
 * uses two integers <var>A</var> and <var>B</var>, and the integer <var>x</var> is mapped
 * to (<var>Ax</var>)&#8853;<var>B</var> before taking the remainder modulo the number of bits 
 * in the filter.
 * 
 * <P>This class exports access methods that are very similar to those of {@link java.util.Set},
 * but it does not implement that interface, as too many non-optional methods
 * would be unimplementable (e.g., iterators).
 *
 * @author Sebastiano Vigna
 */

public class IntBloomFilter implements Serializable {
	private static final long serialVersionUID = 1L;
	/** The number of bits in this filter. */
	final public long m;
	/** The number of hash functions used by this filter. */
	final public int d;
	/** The underlying bit vector. */
	final private LongArrayBitVector bits;
	/** The random integers used multiplicatively. */
	final private int[] a;
	/** The random integers used in exclusive-or. */
	final private int[] b;

	/** The natural logarithm of 2, used in the computation of the number of bits. */
	private final static double NATURAL_LOG_OF_2 = Math.log( 2 );

	private final static boolean DEBUG = false;

	/** Creates a new Bloom filter with given number of hash functions and expected number of elements.
	 * 
	 * @param n the expected number of elements.
	 * @param d the number of hash functions; if the filter add not more than <code>n</code> elements,
	 * false positives will happen with probability 2<sup>-<var>d</var></sup>.
	 */
	public IntBloomFilter( final int n, final int d ) {
		this.d = d;
		bits = LongArrayBitVector.getInstance().length( (long)Math.ceil( ( n * d / NATURAL_LOG_OF_2 ) ) );
		m = bits.length() * Long.SIZE;

		if ( DEBUG ) System.err.println( "Number of bits: " + m );
		
		// The purpose of Random().nextInt() is to generate a different seed at each invocation.
		final MersenneTwister mersenneTwister = new MersenneTwister( new Random().nextInt() );
		a = new int[ d ];
		b = new int[ d ];
		for( int i = 0; i < d; i++ ) {
			a[ i ] = mersenneTwister.nextInt();
			b[ i ] = mersenneTwister.nextInt();
		}
	}

	/** Hashes the given integer with the given hash function.
	 * 
	 * @param x an integer.
	 * @param k a hash function index (smaller than {@link #d}).
	 * @return the position in the filter corresponding to <code>x</code> for the hash function <code>k</code>.
	 */

	private long hash( final int x, final int k ) {
		return ( ( ( a[ k ] * x ) ^ b[ k ] ) & 0x7FFFFFFFFFFFFFFFL ) % m; 
	}

	/** Checks whether the given integer is in this filter. 
	 * 
	 * <P>Note that this method may return true on an integer that has
	 * not been added to the filter. This will happen with probability 2<sup>-<var>d</var></sup>,
	 * where <var>d</var> is the number of hash functions specified at creation time, if
	 * the number of the elements in the filter is less than <var>n</var>, the number
	 * of expected elements specified at creation time.
	 * 
	 * @param x an integer.
	 * @return true if the integer is in the filter (or if an integer with the
	 * same hash sequence is in the filter).
	 */

	public boolean contains( final int x ) {
		int i = d;
		while( i-- != 0 ) if ( ! bits.getBoolean( hash( x, i ) ) ) return false;
		return true;
	}

	/** Adds an integer to the filter.
	 * 
	 * @param x an integer.
	 */

	public void add( final int x ) {
		int i = d;
		while( i-- != 0 ) bits.set( hash( x, i ) );
	}
}
