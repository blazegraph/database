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

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.chars.CharList;
import it.unimi.dsi.lang.MutableString;

/** QuickSearch matching against a constant string.
 *
 * <P>The {@linkplain java.util.regex regular expression facilities} of the Java API
 * are a powerful tool; however, when searching for a <em>constant</em> pattern
 * many algorithms can increase of orders magnitude the speed of a search.
 *
 * <P>This class provides constant-pattern text search facilities by
 * implementing Sunday's QuickSearch (a simplified but very effective variant
 * of the Boyer&mdash;Moore search algorithm) using <a href="http://vigna.dsi.unimi.it/papers.php#BoVMSJ"><em>compact
 * approximators</em></A>, a randomised data structure that can accomodate in a
 * small space (but in an approximated way) the bad-character shift table of a
 * large alphabet such as Unicode.
 *
 * <P>Since a large subset of US-ASCII is used in all languages (e.g.,
 * whitespace, punctuation, etc.), this class caches separately the shifts for
 * the first 128 Unicode characters, resulting in  very good performance even on 
 * text in pure US-ASCII.
 *
 * <P>Note that the {@link MutableString#indexOf(MutableString) indexOf}
 * methods of {@link MutableString} use a even more simplified variant of
 * QuickSearch which is less efficient, but has a smaller setup time and does
 * not generate any object. The search facilities provided by this class are
 * targeted at searches on very large texts, repeated searches with the same
 * pattern, and case-insensitive searches.
 *
 * <P>Instances of this class are immutable and thread-safe.
 *
 * <P>This class is experimental: APIs could change with the next release.
 *
 * @author Sebastiano Vigna
 * @author Paolo Boldi
 * @since 0.6
 */

public class TextPattern implements java.io.Serializable, CharSequence {
	private static final long serialVersionUID = 1L;

	/** Enables case-insensitive matching.
	 *
	 * <P>By default, case-insensitive matching assumes that only characters in
	 * the ASCII charset are being matched. Unicode-aware case-insensitive
	 * matching can be enabled by specifying the UNICODE_CASE flag in
	 * conjunction with this flag.
	 *
	 * <P>Case-insensitivity involves a performance drop.
	 */
	public final static int CASE_INSENSITIVE = 1;

	/** Enables Unicode-aware case folding.
	 *
	 * <P>When this flag is specified then case-insensitive matching, when enabled
	 * by the CASE_INSENSITIVE flag, is done in a manner consistent with the
	 * Unicode Standard. By default, case-insensitive matching assumes that
	 * only characters in the ASCII charset are being matched.
	 *
	 * <P>Unicode-aware case folding is very expensive (two method calls per
	 * examined non-ASCII character).
	 */
	public final static int UNICODE_CASE = 2;

	/** The square of the golden ratio multiplied by 2<sup>32</sup>. */
	private final static int PHI2 = 1640531525;

	/** The pattern backing array. */
	protected char[] pattern;

	/** The compact approximator containing the bad-character shifts; its lenght is a power of 2. */
	private transient int[] badCharShift;

	/** The bad-character shift for US-ASCII. */
	private transient int[] asciiBadCharShift = new int[ 128 ];

	/** A bit mask equal to the length of {@link #badCharShift} minus 1. */
	private transient int mask;

	/** A cached shift value for computing one of the hashes. It is equal to 16 minus the base 2 logarithm of the length of {@link #badCharShift}. */
	private transient int hashShift;

	/** Whether this pattern is case sensitive. */
	private boolean caseSensitive;
	/** Whether this pattern uses optimised ASCII downcasing (as opposed to the correct Unicode downcasing procedure). */
	private boolean asciiCase;

	/** Creates a new case-sensitive {@link TextPattern} object that can be used to search for the given pattern.
	 *
	 * @param pattern the constant pattern to search for.
	 */
	public TextPattern( final CharSequence pattern ) {
		  this( pattern, 0 );
	}

	/** Creates a new {@link TextPattern} object that can be used to search for the given pattern.
	 *
	 * @param pattern the constant pattern to search for.
	 * @param flags a bit mask that may include {@link #CASE_INSENSITIVE} and {@link #UNICODE_CASE}.
	 */
	public TextPattern( final CharSequence pattern, final int flags ) {
		this.pattern = new char[ pattern.length() ];
		MutableString.getChars( pattern, 0, this.pattern.length, this.pattern, 0 );
		caseSensitive = ( flags & CASE_INSENSITIVE ) == 0;
		asciiCase = ( flags & UNICODE_CASE ) == 0;
		if ( ! caseSensitive ) {
			int i = this.pattern.length;
			if ( asciiCase ) while( i-- != 0 ) this.pattern[ i ] = asciiToLowerCase( this.pattern[ i ] );
			else while( i-- != 0 ) this.pattern[ i ] = unicodeToLowerCase( this.pattern[ i ] );
		}
		compile();
	}

	/** Returns whether this pattern is case insensitive.
	 *
	 */
	public boolean caseInsensitive() {
		return ! caseSensitive;
	}

	/** Returns whether this pattern uses Unicode case folding.
	 *
	 */
	public boolean unicodeCase() {
		return ! asciiCase;
	}


	/** A fast, optimised method to lower case just ASCII letters.
	 *
	 * @param c a character.
	 * @return the character <code>c</code>, downcased if it lies between <samp>A</samp> and <samp>Z</samp>.
	 */
	private static char asciiToLowerCase( final char c ) {
		return c >= 'A' && c <= 'Z' ? (char)( c + 32 ) : c;
	}

	/** A method to downcase correctly Unicode characters optimised for ASCII letters.
	 *
	 * @param c a character.
	 * @return the character <code>c</code>, downcased.
	 */
	private static char unicodeToLowerCase( final char c ) {
		if ( c < 128 ) return c >= 'A' && c <= 'Z' ? (char)( c + 32 ) : c;
		return Character.toLowerCase( Character.toUpperCase( c ) );
	}

	private static final int MIN_COUNT = 8;

	/** Fills the search data structures using the pattern contained in {@link #pattern}. */
	private void compile() {
		final char[] p = pattern;
		final int n = p.length;
		int[] s = asciiBadCharShift;

		char c;

		// We make two passes on the pattern, so to approximate first the number of non-US-ASCII characters.

		int h, i, j = 0, k = 0, l;
		int[] max = new int[ MIN_COUNT ];

		i = s.length;
		while( i-- != 0 ) s[ i ] = n;

		i = MIN_COUNT;
		while( i-- != 0 ) max[ i ] = Integer.MAX_VALUE;

		i = n;
		while( i-- != 0 ) {
			c = p[ j++ ];
			if ( c < 128 ) s[ c ] = i;
			else {
				k++;

				/* Bar-Yossef-Jayram-Kumar-Sivakumar-Trevisan's improvement on
				Flajolet-Martin's algorithm for approximating the number of
				distinct elements in a stream. We map each character with a
				hash function on the unit interval, keep track of the smallest
				MIN_COUNT elements, and then approximate the number of distinct
				characters with MIN_COUNT divided by the largest element we
				have (of course, everything is done in fixed point arithmetic
				on the interval 0-2^32). If the approximation is less than the
				number of characters, we use it. */

				h = c * PHI2;
				l = MIN_COUNT;
				while( l-- != 0 ) if ( h > max[ l ] ) break;

				if ( ++l < MIN_COUNT && h != max[ l ] ) {
					System.arraycopy( max, l , max, l + 1, MIN_COUNT - l - 1 );
					max[ l ] = h;
				}
			}
		}


		//System.err.println( k );
		//System.err.println( (int)( MIN_COUNT * 0x100000000L / ( 0x80000000L + M[ MIN_COUNT - 1 ] ) ) );

		k = Math.min( k, (int)( MIN_COUNT * 0x100000000L / ( 0x80000000L + max[ MIN_COUNT - 1 ] ) ) );

		/* We do not use less than 2^7 entries, so to compensate the setup
		overhead with a more precise approximator for very small patterns. If,
		however, there are no non-US-ASCII characters, we use a single-bucket
		approximator (so to avoid special cases in the algorithm). In any case,
		we do not use approximators with more than 2^16 entries. */
		final int log2m = k == 0 ? 0 : Math.min( Math.max( Fast.mostSignificantBit( k * 3 ) + 1, 7 ), 16 );
		final int m = ( 1 << log2m ) - 1;
		final int hs = 16 - log2m;

		/* For the characters outside Unicode, we build a compact approximator
		with two hash functions h_0 and h_1. The approximator stores a function
		f from the character set to integers in a table s. The value of the
		function on c can be obtained by maximising the values s[h_0(c)] and
		s[h_1(c)]. When storing a value, instead, we minimise s[h_0(c)] and
		s[h_1(c)] with f(c). As a result, the value stored is smaller than or
		equal to the actual value f(c). By tuning the size of s and the number
		of hash functions one can get a desired error precision. */

		s =  new int[ m + 1 ];

		i = m + 1;
		while( i-- != 0 ) s[ i ] = n;

		i = n;
		j = 0;
		while( i-- != 0 ) {
			c = p[ j++ ];
			if ( c >= 128 ) {
				s[ c * c & m ] = i;
				s[ ( c * PHI2 ) >> hs & m ] = i;
			}
		}
		
		this.badCharShift = s;
		this.mask = m;
		this.hashShift = hs;

		/*
		for( i = 0; i <= m; i++ ) System.err.print( "" + s[ i ] + ", " );
		System.err.println();

		for( c = 'a'; c <= 'z'; c++ ) System.err.print( c + ":" + Math.max( s[ c * c & m ], s[ ( c * PHI2 ) >> hs & m ] ) + ", " );
		System.err.println();

		for( c = 'A'; c <= 'Z'; c++ ) System.err.print( c + ":" + Math.max( s[ c * c & m ], s[ ( c * PHI2 ) >> hs & m ] ) + ", " );
		System.err.println();

		MutableString msp = new MutableString( pattern );

		for( c = 'a'; c <= 'z'; c++ ) if ( msp.indexOf( c ) == -1 && Math.max( s[ c * c & m ], s[ ( c * PHI2 ) >> hs & m ] ) != n ) 
			System.err.println( c + ":" + Math.max( s[ c * c & m ], s[ ( c * PHI2 ) >> hs & m ] ) + ", " );

		for( c = 'A'; c <= 'Z'; c++ ) if ( msp.indexOf( c ) == -1 && Math.max( s[ c * c & m ], s[ ( c * PHI2 ) >> hs & m ] ) != n ) 
			System.err.println( c + ":" + Math.max( s[ c * c & m ], s[ ( c * PHI2 ) >> hs & m ] ) + ", " );
		*/

	}

	public int length() {
		return pattern.length;
	}

	public char charAt( final int i ) {
		return pattern[ i ];
	}

	public CharSequence subSequence( final int from, final int to ) {
		return new MutableString( pattern, from, to - from + 1 );
	}


    /** Returns the index of the first occurrence of this one-character pattern 
     * in the specified character array, starting at the specified index. 
	 *
	 * <P>This method is a fallback for searches on one-character patterns.
     *
     * @param array the character array to look in.
     * @param from the index from which the search must start.
     * @param to the index at which the search must end.
     * @return the index of the first occurrence of this pattern or
     * <code>-1</code>, if this pattern never appears with index greater than
     * or equal to <code>from</code>.
     */
    private int indexOf( final char[] array, final int from, final int to ) {
		final char[] a = array;
		final int c = pattern[ 0 ];

		int i = from < 0 ? -1 : from - 1;

		if ( caseSensitive ) {
			while( ++i < to ) if ( a[ i ] == c ) return i;
			return -1;
		}
		else if ( asciiCase ) {
			while( ++i < to ) if ( asciiToLowerCase( a[ i ] ) == c ) return i;
			return -1;
		}
		else {
			while( ++i < to ) if ( unicodeToLowerCase( a[ i ] ) == c ) return i;
			return -1;
		}
    }

    /** Returns the index of the first occurrence of this one-character pattern
     * in the specified character sequence, starting at the specified index. 
	 *
	 * <P>This method is a fallback for searches on one-character patterns.
     *
     * @param s the character sequence to look in.
     * @param from the index from which the search must start.
     * @return the index of the first occurrence of this pattern or
     * <code>-1</code>, if the this pattern never appears with index greater than
     * or equal to <code>from</code>.
     */
    private int indexOf( final CharSequence s, final int from, final int to ) {
		final int c = pattern[ 0 ];

		int i = from < 0 ? -1 : from - 1;

		if ( caseSensitive ) {
			while( ++i < to ) if ( s.charAt( i ) == c ) return i;
			return -1;
		}
		else if ( asciiCase ) {
			while( ++i < to ) if ( asciiToLowerCase( s.charAt( i ) ) == c ) return i;
			return -1;
		}
		else {
			while( ++i < to ) if ( unicodeToLowerCase( s.charAt( i ) ) == c ) return i;
			return -1;
		}
    }

    /** Returns the index of the first occurrence of this one-character pattern 
     * in the specified byte array, seen as an ISO-8859-1 string, 
     * starting at the specified index. 
	 *
	 * <P>This method is a fallback for searches on one-character patterns.
     *
     * @param array the byte array to look in.
     * @param from the index from which the search must start.
     * @return the index of the first occurrence of this pattern or
     * <code>-1</code>, if this pattern never appears with index greater than
     * or equal to <code>from</code>.
     */
    private int indexOf( final byte[] array, final int from, final int to ) {
		final byte[] a = array;
		final int c = pattern[ 0 ];

		int i = from < 0 ? -1 : from - 1;

		if ( caseSensitive ) {
			while( ++i < to ) if ( ( a[ i ] & 0xFF ) == c ) return i;
			return -1;
		}
		else {
			while( ++i < to ) if ( asciiToLowerCase( (char)( a[ i ] & 0xFF ) ) == c ) return i;
			return -1;
		}
    }

    /** Returns the index of the first occurrence of this pattern in the given character array.
	 *
     * @param array the character array to look in.
     * @return the index of the first occurrence of this pattern contained in the
	 * given array, or <code>-1</code>, if the pattern cannot be found.
	 */

	public int search( final char[] array ) {
		return search( array, 0, array.length );
	}


    /** Returns the index of the first occurrence of this pattern in the given character array starting from a given index. 
	 *
     * @param array the character array to look in.
     * @param from the index from which the search must start.
     * @return the index of the first occurrence of this pattern contained in the
	 * subarray starting from <code>from</code> (inclusive), or
	 * <code>-1</code>, if the pattern cannot be found.
	 */
	public int search( char[] array, int from ) {
		return search( array, from, array.length );
	}

    /** Returns the index of the first occurrence of this pattern in the given character array between given indices. 
	 *
     * @param a the character array to look in.
     * @param from the index from which the search must start.
     * @param to the index at which the search must end.
     * @return the index of the first occurrence of this pattern contained in the
	 * subarray starting from <code>from</code> (inclusive) up to <code>to</code>
	 * (exclusive) characters, or <code>-1</code>, if the pattern cannot be found.
	 */

	public int search( final char[] a, final int from, final int to ) {

		final int n = pattern.length;

		if ( n == 0 ) return from > to ? to : ( from < 0 ? 0 : from );
		if ( n == 1 ) return indexOf( a, from, to );

		final char[] p = pattern;
		final char last = p[ n - 1 ];
		final int m1 = to - 1;
		final int[] shift = badCharShift;
		final int[] asciiShift = asciiBadCharShift;
		final int m = mask;
		final int hs = hashShift;

		int i = ( from < 0 ? 0 : from ) + n - 1, j, k;
		char c;

		if ( caseSensitive ) {
			while ( i < m1 ) {
				if ( a[ i ] == last ) {
					j = n - 1;
					k = i;
					while( j-- != 0 && a[ --k ] == p[ j ] );
					if ( j < 0 ) return k;
				}
				if ( ( c = a[ ++i ]  ) < 128 ) i += asciiShift[ c ];
				else {
					j = shift[ c * c & m ];
					k = shift[ ( c * PHI2 ) >> hs & m ];
					i += j > k ? j : k;
				}
			}
			
			if ( i == m1 ) {
				j = n;
				while( j-- != 0 && a[ i-- ] == p[ j ] );
				if ( j < 0 ) return i + 1;
			}
			return -1;
		}
		else if ( asciiCase ) {
			while ( i < m1 ) {
				if ( asciiToLowerCase( a[ i ] ) == last ) {
					j = n - 1;
					k = i;
					while( j-- != 0 && asciiToLowerCase( a[ --k ] ) == p[ j ] );
					if ( j < 0 ) return k;
				}
				if ( ( c = asciiToLowerCase( a[ ++i ] ) ) < 128 ) i += asciiShift[ c ];
				else {
					j = shift[ c * c & m ];
					k = shift[ ( c * PHI2 ) >> hs & m ];
					i += j > k ? j : k;
				}
			}
			
			if ( i == m1 ) {
				j = n;
				while( j-- != 0 && asciiToLowerCase( a[ i-- ] ) == p[ j ] );
				if ( j < 0 ) return i + 1;
			}
			return -1;
		}
		else {
			while ( i < m1 ) {
				if ( unicodeToLowerCase( a[ i ] ) == last ) {
					j = n - 1;
					k = i;
					while( j-- != 0 && unicodeToLowerCase( a[ --k ] ) == p[ j ] );
					if ( j < 0 ) return k;
				}
				if ( ( c = unicodeToLowerCase( a[ ++i ] ) ) < 128 ) i += asciiShift[ c ];
				else {
					j = shift[ c * c & m ];
					k = shift[ ( c * PHI2 ) >> hs & m ];
					i += j > k ? j : k;
				}
			}
			
			if ( i == m1 ) {
				j = n;
				while( j-- != 0 && unicodeToLowerCase( a[ i-- ] ) == p[ j ] );
				if ( j < 0 ) return i + 1;
			}
			return -1;
		}
	}




    /** Returns the index of the first occurrence of this pattern in the given character sequence.
	 *
     * @param s the character sequence to look in.
     * @return the index of the first occurrence of this pattern contained in the
	 * given character sequence, or <code>-1</code>, if the pattern cannot be found.
	 */

	public int search( final CharSequence s ) {
		return search( s, 0, s.length() );
	}


    /** Returns the index of the first occurrence of this pattern in the given character sequence starting from a given index. 
	 *
     * @param s the character array to look in.
     * @param from the index from which the search must start.
     * @return the index of the first occurrence of this pattern contained in the
	 * subsequence starting from <code>from</code> (inclusive), or
	 * <code>-1</code>, if the pattern cannot be found.
	 */
	public int search( final CharSequence s, final int from ) {
		return search( s, from, s.length() );
	}

    /** Returns the index of the first occurrence of this pattern in the given character sequence between given indices. 
	 *
     * @param s the character array to look in.
     * @param from the index from which the search must start.
     * @param to the index at which the search must end.
     * @return the index of the first occurrence of this pattern contained in the
	 * subsequence starting from <code>from</code> (inclusive) up to <code>to</code>
	 * (exclusive) characters, or <code>-1</code>, if the pattern cannot be found.
	 */

	public int search( final CharSequence s, final int from, final int to ) {

		final int n = pattern.length;

		if ( n == 0 ) return from > to ? to : ( from < 0 ? 0 : from );
		if ( n == 1 ) return indexOf( s, from, to );

		final char[] p = pattern;
		final char last = p[ n - 1 ];
		final int m1 = to - 1;
		final int[] shift = badCharShift;
		final int[] asciiShift = asciiBadCharShift;
		final int m = mask;
		final int hs = hashShift;

		int i = ( from < 0 ? 0 : from ) + n - 1, j, k;
		char c;

		if ( caseSensitive ) {
			while ( i < m1 ) {
				if ( s.charAt( i ) == last ) {
					j = n - 1;
					k = i;
					while( j-- != 0 && s.charAt( --k ) == p[ j ] );
					if ( j < 0 ) return k;
				}
				if ( ( c = s.charAt( ++i ) ) < 128 ) i += asciiShift[ c ];
				else {
					j = shift[ c * c & m ];
					k = shift[ ( c * PHI2 ) >> hs & m ];
					i += j > k ? j : k;
				}
			}
			
			if ( i == m1 ) {
				j = n;
				while( j-- != 0 && s.charAt( i-- ) == p[ j ] );
				if ( j < 0 ) return i + 1;
			}
			return -1;
		}
		else if ( asciiCase ) {
			while ( i < m1 ) {
				if ( asciiToLowerCase( s.charAt( i ) ) == last ) {
					j = n - 1;
					k = i;
					while( j-- != 0 && asciiToLowerCase( s.charAt( --k ) ) == p[ j ] );
					if ( j < 0 ) return k;
				}
				if ( ( c = asciiToLowerCase( s.charAt( ++i ) ) ) < 128 ) i += asciiShift[ c ];
				else {
					j = shift[ c * c & m ];
					k = shift[ ( c * PHI2 ) >> hs & m ];
					i += j > k ? j : k;
				}
			}
			
			if ( i == m1 ) {
				j = n;
				while( j-- != 0 && asciiToLowerCase( s.charAt( i-- ) ) == p[ j ] );
				if ( j < 0 ) return i + 1;
			}
			return -1;
		}
		else {
			while ( i < m1 ) {
				if ( unicodeToLowerCase( s.charAt( i ) ) == last ) {
					j = n - 1;
					k = i;
					while( j-- != 0 && unicodeToLowerCase( s.charAt( --k ) ) == p[ j ] );
					if ( j < 0 ) return k;
				}
				if ( ( c = unicodeToLowerCase( s.charAt( ++i ) ) ) < 128 ) i += asciiShift[ c ];
				else {
					j = shift[ c * c & m ];
					k = shift[ ( c * PHI2 ) >> hs & m ];
					i += j > k ? j : k;
				}
			}
			
			if ( i == m1 ) {
				j = n;
				while( j-- != 0 && unicodeToLowerCase( s.charAt( i-- ) ) == p[ j ] );
				if ( j < 0 ) return i + 1;
			}
			return -1;
		}
	}

    /** Returns the index of the first occurrence of this pattern in the given byte array.
	 *
    * @param a the byte array to look in.
    * @return the index of the first occurrence of this pattern contained in the
	 * given byte array, or <code>-1</code>, if the pattern cannot be found.
	 */

	public int search( final byte[] a ) {
		return search( a, 0, a.length );
	}


   /** Returns the index of the first occurrence of this pattern in the given byte array starting from a given index. 
	 *
    * @param a the byte array to look in.
    * @param from the index from which the search must start.
    * @return the index of the first occurrence of this pattern contained in the
	 * array fragment starting from <code>from</code> (inclusive), or
	 * <code>-1</code>, if the pattern cannot be found.
	 */
	public int search( final byte[] a, final int from ) {
		return search( a, from, a.length );
	}

   /** Returns the index of the first occurrence of this pattern in the given byte array between given indices. 
	 *
    * @param a the byte array to look in.
    * @param from the index from which the search must start.
    * @param to the index at which the search must end.
    * @return the index of the first occurrence of this pattern contained in the
	 * array fragment starting from <code>from</code> (inclusive) up to <code>to</code>
	 * (exclusive) characters, or <code>-1</code>, if the pattern cannot be found.
	 * TODO: this must be tested
	 */
	public int search( final byte[] a, final int from, final int to ) {

		final int n = pattern.length;

		if ( n == 0 ) return from > to ? to : ( from < 0 ? 0 : from );
		if ( n == 1 ) return indexOf( a, from, to );

		final char[] p = pattern;
		final char last = p[ n - 1 ];
		final int m1 = to - 1;
		final int[] shift = badCharShift;
		final int[] asciiShift = asciiBadCharShift;
		final int m = mask;
		final int hs = hashShift;

		int i = ( from < 0 ? 0 : from ) + n - 1, j, k;
		char c;

		if ( caseSensitive ) {
			while ( i < m1 ) {
				if ( ( a[ i ] & 0xFF ) == last ) {
					j = n - 1;
					k = i;
					while( j-- != 0 && ( a[ --k ] & 0xFF )== p[ j ] );
					if ( j < 0 ) return k;
				}
				if ( ( c = (char)( a[ ++i ] & 0xFF ) ) < 128 ) i += asciiShift[ c ];
				else {
					j = shift[ c * c & m ];
					k = shift[ ( c * PHI2 ) >> hs & m ];
					i += j > k ? j : k;
				}
			}
			
			if ( i == m1 ) {
				j = n;
				while( j-- != 0 && ( a[ i-- ] & 0xFF ) == p[ j ] );
				if ( j < 0 ) return i + 1;
			}
			return -1;
		}
		else if ( asciiCase ) {
			while ( i < m1 ) {
				if ( asciiToLowerCase( (char)( a[ i ] & 0xFF ) ) == last ) {
					j = n - 1;
					k = i;
					while( j-- != 0 && asciiToLowerCase( (char)( a[ --k ] & 0xFF ) ) == p[ j ] );
					if ( j < 0 ) return k;
				}
				if ( ( c = asciiToLowerCase( (char)( a[ ++i ] & 0xFF ) ) ) < 128 ) i += asciiShift[ c ];
				else {
					j = shift[ c * c & m ];
					k = shift[ ( c * PHI2 ) >> hs & m ];
					i += j > k ? j : k;
				}
			}
			
			if ( i == m1 ) {
				j = n;
				while( j-- != 0 && asciiToLowerCase( (char)( a[ i-- ] & 0xFF ) ) == p[ j ] );
				if ( j < 0 ) return i + 1;
			}
			return -1;
		}
		else {
			while ( i < m1 ) {
				if ( unicodeToLowerCase( (char)( a[ i ] & 0xFF ) ) == last ) {
					j = n - 1;
					k = i;
					while( j-- != 0 && unicodeToLowerCase( (char)( a[ --k ] & 0xFF ) ) == p[ j ] );
					if ( j < 0 ) return k;
				}
				if ( ( c = unicodeToLowerCase( (char)( a[ ++i ] & 0xFF ) ) ) < 128 ) i += asciiShift[ c ];
				else {
					j = shift[ c * c & m ];
					k = shift[ ( c * PHI2 ) >> hs & m ];
					i += j > k ? j : k;
				}
			}
			
			if ( i == m1 ) {
				j = n;
				while( j-- != 0 && unicodeToLowerCase( (char)( a[ i-- ] & 0xFF ) ) == p[ j ] );
				if ( j < 0 ) return i + 1;
			}
			return -1;
		}
	}


	

    /** Returns the index of the first occurrence of this pattern in the given character list.
	 *
     * @param list the character list to look in.
     * @return the index of the first occurrence of this pattern contained in the
	 * given list, or <code>-1</code>, if the pattern cannot be found.
	 */

	public int search( final CharList list ) {
		return search( list, 0, list.size() );
	}


    /** Returns the index of the first occurrence of this pattern in the given character list starting from a given index. 
	 *
     * @param list the character list to look in.
     * @param from the index from which the search must start.
     * @return the index of the first occurrence of this pattern contained in the
	 * sublist starting from <code>from</code> (inclusive), or
	 * <code>-1</code>, if the pattern cannot be found.
	 */
	public int search( final CharList list, int from ) {
		return search( list, from, list.size() );
	}

    /** Returns the index of the first occurrence of this pattern in the given character list between given indices. 
	 *
     * @param list the character list to look in.
     * @param from the index from which the search must start.
     * @param to the index at which the search must end.
     * @return the index of the first occurrence of this pattern contained in the
	 * sublist starting from <code>from</code> (inclusive) up to <code>to</code>
	 * (exclusive) characters, or <code>-1</code>, if the pattern cannot be found.
	 */

	public int search( final CharList list, final int from, final int to ) {

		final int n = pattern.length;

		if ( n == 0 ) return from > to ? to : ( from < 0 ? 0 : from );
		if ( n == 1 ) return list.subList( from, to ).indexOf( pattern[ 0 ] );

		final char[] p = pattern;
		final char last = p[ n - 1 ];
		final int m1 = to - 1;
		final int[] shift = badCharShift;
		final int[] asciiShift = asciiBadCharShift;
		final int m = mask;
		final int hs = hashShift;

		int i = ( from < 0 ? 0 : from ) + n - 1, j, k;
		char c;

		if ( caseSensitive ) {
			while ( i < m1 ) {
				if ( list.getChar( i ) == last ) {
					j = n - 1;
					k = i;
					while( j-- != 0 && list.getChar( --k ) == p[ j ] );
					if ( j < 0 ) return k;
				}
				if ( ( c = list.getChar( ++i ) ) < 128 ) i += asciiShift[ c ];
				else {
					j = shift[ c * c & m ];
					k = shift[ ( c * PHI2 ) >> hs & m ];
					i += j > k ? j : k;
				}
			}
			
			if ( i == m1 ) {
				j = n;
				while( j-- != 0 && list.getChar( i-- ) == p[ j ] );
				if ( j < 0 ) return i + 1;
			}
			return -1;
		}
		else if ( asciiCase ) {
			while ( i < m1 ) {
				if ( asciiToLowerCase( list.getChar( i ) ) == last ) {
					j = n - 1;
					k = i;
					while( j-- != 0 && asciiToLowerCase( list.getChar( --k ) ) == p[ j ] );
					if ( j < 0 ) return k;
				}
				if ( ( c = asciiToLowerCase( list.getChar( ++i ) ) ) < 128 ) i += asciiShift[ c ];
				else {
					j = shift[ c * c & m ];
					k = shift[ ( c * PHI2 ) >> hs & m ];
					i += j > k ? j : k;
				}
			}
			
			if ( i == m1 ) {
				j = n;
				while( j-- != 0 && asciiToLowerCase( list.getChar( i-- ) ) == p[ j ] );
				if ( j < 0 ) return i + 1;
			}
			return -1;
		}
		else {
			while ( i < m1 ) {
				if ( unicodeToLowerCase( list.getChar( i ) ) == last ) {
					j = n - 1;
					k = i;
					while( j-- != 0 && unicodeToLowerCase( list.getChar( --k ) ) == p[ j ] );
					if ( j < 0 ) return k;
				}
				if ( ( c = unicodeToLowerCase( list.getChar( ++i ) ) ) < 128 ) i += asciiShift[ c ];
				else {
					j = shift[ c * c & m ];
					k = shift[ ( c * PHI2 ) >> hs & m ];
					i += j > k ? j : k;
				}
			}
			
			if ( i == m1 ) {
				j = n;
				while( j-- != 0 && unicodeToLowerCase( list.getChar( i-- ) ) == p[ j ] );
				if ( j < 0 ) return i + 1;
			}
			return -1;
		}
	}




	/** Compares this text pattern to another object.
	 *
	 * <P>This method will return <code>true</code> iff its argument
	 * is a <code>TextPattern</code> containing the same constant pattern with the same flags set.
	 *
	 * @param o an object.
	 * @return true if the argument is a <code>TextPattern</code>s that contains the same constant pattern of this text pattern
	 * and has the same flags set.
	 */
	final public boolean equals( final Object o ) {
		if ( o instanceof TextPattern ) {
			TextPattern p = (TextPattern)o;
			return caseSensitive == p.caseSensitive && asciiCase == p.asciiCase && java.util.Arrays.equals( p.pattern, pattern );
		}
		return false;
	}

	/** Returns a hash code for this text pattern. 
	 *
	 * <P>The hash code of a text pattern is the same as that of a
	 * <code>String</code> with the same content (suitably lower cased, if the pattern is case insensitive).
	 *
	 * @return  a hash code array for this object.
	 * @see java.lang.String#hashCode()
	 */
	final public int hashCode() {
		final char[] a = pattern;
		final int l = a.length;
		int h;
		for ( int i = h = 0; i < l; i++ ) h = 31 * h + a[ i ];
		return h;
	}

	final public String toString() { 
		return new String( pattern ); 
	}
}
