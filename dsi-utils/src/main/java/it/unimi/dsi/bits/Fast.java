package it.unimi.dsi.bits;

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


/** All-purpose optimised bit-fiddling static-method container class.
 *
 * <P>This class contains static optimised utility methods that are used by all
 * classes manipulating bits. They include:
 * <ul>
 * <li>methods to map bijectively {@linkplain #int2nat(int) integer to natural numbers}
 * and {@linkplain #nat2int(int) back};
 * <li>an implementation of Gerth Brodal's fast algorithm for the {@linkplain #mostSignificantBit(long) most significant bit},
 * and derived methods such as {@link #log2(double)}, {@link #ceilLog2(int)}, etc.;
 * <li>an implementation of the classical <em>{@linkplain #count(long) sideways addition}</em> algorithm (a case of <em>broadword programming</em>), 
 * which can be used to do <em>ranking</em>, and of a new, dual algorithm for <em>{@linkplain #select(long, int) selection}</em>; the latter has been presented
 * by Sebastiano Vigna in &ldquo;<a href="http://vigna.dsi.unimi.it/papers.php#VigBIRSQ">Broadword Implementation of Rank/Select Queries</a>&rdquo;,
 * <i>Proc. of the 7th International Workshop on Experimental Algorithms, WEA 2008</i>,
 * number 5038 in Lecture Notes in Computer Science, pages 154&minus;168. Springer&ndash;Verlag, 2008.
 * </ul>
 * @author Sebastiano Vigna
 * @since 0.1
 */

public final class Fast {
	private Fast() {}

	private static final boolean ASSERTS = false;
	
	public static final long ONES_STEP_4 = 0x1111111111111111L;
	public static final long ONES_STEP_8 = 0x0101010101010101L;
	public static final long MSBS_STEP_8 = 0x80L * ONES_STEP_8;
	private static final long INCR_STEP_8 = 0x80L << 56 | 0x40L << 48 | 0x20L << 40 | 0x10L << 32 | 0x8L << 24 | 0x4L << 16 | 0x2L << 8 | 0x1;

	/** Maps integers bijectively into natural numbers.
	 * 
	 * <P>This method will map a negative integer <var>x</var> to -2<var>x</var>-1 and
	 * a nonnegative integer <var>x</var> to 2<var>x</var>. It can be used to save 
	 * integers in the range [{@link Integer#MIN_VALUE}/2..{@link Integer#MAX_VALUE}/2] 
	 * (i.e., [-2<sup>30</sup>..2<sup>30</sup>-1])
	 * using the standard coding methods (which all work on natural numbers). Note
	 * that no range checks are performed.
	 * 
	 * <P>The inverse of the above map is computed by {@link #nat2int(int)}.
	 *
	 * @param x an integer.
	 * @return the argument mapped into a natural number.
	 * @see #nat2int(int)
	 */

	public static int int2nat( final int x ) {
		return x >= 0 ? x << 1 : -( ( x << 1 ) + 1 );
	}

	/** Maps natural numbers bijectively into integers.
	 * 
	 * <P>This method computes the inverse of {@link #int2nat(int)}.
	 *
	 * @param x a natural  number.
	 * @return the argument mapped into an integer.
	 * @see #int2nat(int)
	 */

	public static int nat2int( final int x ) {
		return x % 2 == 0 ? x >> 1 : -( x >> 1 ) - 1;
	}

	/** Maps longs bijectively into long natural numbers.
	 * 
	 * <P>This method will map a negative long <var>x</var> to -2<var>x</var>-1 and
	 * a nonnegative long <var>x</var> to 2<var>x</var>. It can be used to save 
	 * longs in the range [{@link Long#MIN_VALUE}/2..{@link Long#MAX_VALUE}/2] 
	 * (i.e., [-2<sup>62</sup>..2<sup>62</sup>-1])
	 * using the standard coding methods (which all work on natural numbers). Note
	 * that no range checks are performed.
	 * 
	 * <P>The inverse of the above map is computed by {@link #nat2int(long)}.
	 *
	 * @param x a long.
	 * @return the argument mapped into a long natural number.
	 * @see #int2nat(int)
	 */

	public static long int2nat( final long x ) {
		return x >= 0 ? x << 1 : -( ( x << 1 ) + 1 );
	}

	/** Maps long natural numbers bijectively into longs.
	 * 
	 * <P>This method computes the inverse of {@link #int2nat(long)}.
	 *
	 * @param x a long natural  number.
	 * @return the argument mapped into a long.
	 * @see #nat2int(int)
	 */

	public static long nat2int( final long x ) {
		return x % 2 == 0 ? x >> 1 : -( x >> 1 ) - 1;
	}

	/** Returns the base-two logarithm of the argument.
	 * 
	 * @param x a double.
	 * @return the base-2 logarithm of <code>x</code>.
	 */
	public static double log2( final double x ) {
		return Math.log( x ) / 0.6931471805599453;
	}

	// TODO: implement ceilLog2 using only and/or/xor/etc.
	
	/** Computes the ceiling of the base-two logarithm of the argument.
	 *
	 * <p>This method relies of {@link #mostSignificantBit(int)}, and thus is pretty fast.
	 * 
	 * @param x an integer.
	 * @return the ceiling of the base-two logarithm of the argument, or -1 if <code>x</code> is zero.
	 */
	public static int ceilLog2( final int x ) {
		if ( x == 0 ) return -1;
		return mostSignificantBit( x - 1 ) + 1;
	}

	/** Computes the ceiling of the base-two logarithm of the argument.
	 * 
	 * <p>This method relies of {@link #mostSignificantBit(long)}, and thus is pretty fast.
	 * 
	 * @param x an integer.
	 * @return the ceiling of the base-two logarithm of the argument, or -1 if <code>x</code> is zero.
	 */
	public static int ceilLog2( final long x ) {
		if ( x == 0 ) return -1;
		return mostSignificantBit( x - 1 ) + 1;
	}

	/** Returns the number of bits that are necessary to encode the argument.
	 * 
	 * @param x an integer.
	 * @return the number of bits that are necessary to encode <code>x</code>.
	 */
	public static int length( final int x ) {
		return x == 0 ? 1 : mostSignificantBit( x ) + 1;
	}

	/** Returns the number of bits that are necessary to encode the argument.
	 * 
	 * @param x a long.
	 * @return the number of bits that are necessary to encode <code>x</code>.
	 */
	public static int length( final long x ) {
		return x == 0 ? 1 : mostSignificantBit( x ) + 1;
	}

	/** Returns the number of bits set to one in a long.
	 * 
	 * <p>This method implements a classical broadword algorithm. 
	 * 
	 * @param x a long.
	 * @return the number of bits set to one in <code>x</code>.
	 */

	public static int count( final long x ) {
		long byteSums = x - ( ( x & 0xa * ONES_STEP_4 ) >>> 1 );
        byteSums = ( byteSums & 3 * ONES_STEP_4 ) + ( ( byteSums >>> 2 ) & 3 * ONES_STEP_4 );
        byteSums = ( byteSums + ( byteSums >>> 4 ) ) & 0x0f * ONES_STEP_8;
        return (int)( byteSums * ONES_STEP_8 >>> 56 );
	}
	
	/** Returns the position of a bit of given rank.
	 * 
	 * <p>This method implements a new broadword algorithm. 
	 * 
	 * @param x a long.
	 * @param rank a rank (smaller than 128).
	 * @return the position in <code>x</code> of the bit of rank <code>k</code> ones; if no such
	 * bit exists, returns 72.
	 */
	public static int select( final long x, final int rank ) {
        if ( ASSERTS ) assert rank < count( x ) : rank + " >= " + count( x );

        // Phase 1: sums by byte
        long byteSums = x - ( ( x & 0xa * ONES_STEP_4 ) >>> 1 );
        byteSums = ( byteSums & 3 * ONES_STEP_4 ) + ( ( byteSums >>> 2 ) & 3 * ONES_STEP_4 );
        byteSums = ( byteSums + ( byteSums >>> 4 ) ) & 0x0f * ONES_STEP_8;
        byteSums *= ONES_STEP_8;
        
        // Phase 2: compare each byte sum with rank to obtain the relevant byte
        final long rankStep8 = rank * ONES_STEP_8;
        final long byteOffset = ( ( ( ( ( rankStep8 | MSBS_STEP_8 ) - byteSums ) & MSBS_STEP_8 ) >>> 7 ) * ONES_STEP_8 >>> 53 ) & ~0x7;

        // Phase 3: Locate the relevant byte and make 8 copies with incremental masks
        final int byteRank = (int)( rank - ( ( ( byteSums << 8 ) >>> byteOffset ) & 0xFF ) );

        final long spreadBits = ( x >>> byteOffset & 0xFF ) * ONES_STEP_8 & INCR_STEP_8;
        final long bitSums = ( ( ( spreadBits | ( ( spreadBits | MSBS_STEP_8 ) - ONES_STEP_8 ) ) & MSBS_STEP_8 ) >>> 7 ) * ONES_STEP_8;

        // Compute the inside-byte location and return the sum
        final long byteRankStep8 = byteRank * ONES_STEP_8;

        return (int)( byteOffset + ( ( ( ( ( byteRankStep8 | MSBS_STEP_8 ) - bitSums ) & MSBS_STEP_8 ) >>> 7 ) * ONES_STEP_8 >>> 56 ) );
	}


	/** Returns the most significant bit of a long.
	 * 
	 * <p>This method implements Gerth Brodal's broadword algorithm. On 64-bit architectures
	 * it is an order of magnitude faster than standard bit-fiddling techniques.
	 * 
	 * @param x a long.
	 * @return the most significant bit of <code>x</code>, of <code>x</code> is nonzero; &minus;1, otherwise.
	 */
	public static int mostSignificantBit( long x ) {
		if ( x == 0 ) return -1;
		
		int msb = 0;
		
		if ( ( x & 0xFFFFFFFF00000000L ) != 0 ) {
			x >>>= ( 1 << 5 );
			msb += ( 1 << 5 );
		}
		
		if ( ( x & 0xFFFF0000 ) != 0 ) {
			x >>>= ( 1 << 4 );
			msb += ( 1 << 4 );
		}
		
		// We have now reduced the problem to finding the msb in a 16-bit word.
		
		x |= x << 16;
		x |= x << 32;
		
		final long y = x & 0xFF00F0F0CCCCAAAAL;
		
		long t = 0x8000800080008000L & ( y | (( y | 0x8000800080008000L ) - ( x ^ y )));
		
		t |= t << 15;
		t |= t << 30;
		t |= t << 60;
		
		return (int)( msb + ( t >>> 60 ) );
	}
	
	/** Returns the most significant bit of an integer.
	 * 
	 * @param x an integer.
	 * @return the most significant bit of <code>x</code>, of <code>x</code> is nonzero; &minus;1, otherwise.
	 * @see #mostSignificantBit(long)
	 */
	public static int mostSignificantBit( int x ) {
		if ( x == 0 ) return -1;
		
		int msb = 0;
		
		if ( ( x & 0xFFFF0000 ) != 0 ) {
			x >>>= ( 1 << 4 );
			msb += ( 1 << 4 );
		}

		long z = x;
		
		// We have now reduced the problem to finding the msb in a 16-bit word.
		
		z |= z << 16;
		z |= z << 32;
		
		final long y = z & 0xFF00F0F0CCCCAAAAL;
		
		long t = 0x8000800080008000L & ( y | (( y | 0x8000800080008000L ) - ( z ^ y )));
		
		t |= t << 15;
		t |= t << 30;
		t |= t << 60;
		
		return (int)( msb + ( t >>> 60 ) );
	}
	
	/** Precomputed least significant bits for bytes (-1 for 0 ). */
	public static final int[] BYTELSB = {
		-1, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0
	};
	    

	/** Precomputed most significant bits for bytes (-1 for 0 ). 
	 */
	public static final int[] BYTEMSB = {
		-1, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 
		4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 
		5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 
		5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 
		6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 
		6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 
		6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 
		6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7
	};
	

	/** Computes the least significant bit of a long integer.
	 *
	 * @param x a long integer.
	 * @return the least significant bit of the argument (-1 for 0).
	 */
	public static int leastSignificantBit( final long x ) {
		if ( x == 0 ) return -1;
		if ( ( x & 0xFF ) != 0 ) return BYTELSB[ (int)( x & 0xFF ) ];
		if ( ( x & 0xFFFF ) != 0 ) return BYTELSB[ (int)( x >>> 8 & 0xFF ) ] + 8;
		if ( ( x & 0xFFFFFF ) != 0 ) return BYTELSB[ (int)( x >>> 16 & 0xFF ) ] + 16;
		if ( ( x & 0xFFFFFFFFL ) != 0 ) return BYTELSB[ (int)( x >>> 24 & 0xFF ) ] + 24;
		if ( ( x & 0xFFFFFFFFFFL ) != 0 ) return BYTELSB[ (int)( x >>> 32 & 0xFF ) ] + 32;
		if ( ( x & 0xFFFFFFFFFFFFL ) != 0 ) return BYTELSB[ (int)( x >>> 40 & 0xFF ) ] + 40;
		if ( ( x & 0xFFFFFFFFFFFFFFL ) != 0 ) return BYTELSB[ (int)( x >>> 48 & 0xFF ) ] + 48;
		return BYTELSB[ (int)( x >>> 56 & 0xFF ) ] + 56;
	}

	
	
	final private static byte[] LSB_TABLE = { 
		0, 1, 56, 2, 57, 49, 28, 3, 61, 58, 42, 50, 38, 29, 17, 4, 62, 47, 59, 36, 45, 43, 51, 22, 53, 39, 33, 30, 24, 18, 12, 5, 63, 55, 48, 27, 60, 41, 37, 16, 46, 35, 44, 21, 52, 32, 23, 11, 54, 26, 40, 15, 34, 20, 31, 10, 25, 14, 19, 9, 13, 8, 7, 6
	};
	
	/** Returns the least significant bit of a long.
	 * 
	 * 
	 * @param x a long.
	 * @return the least significant bit of <code>x</code>, of <code>x</code> is nonzero; &minus;1, otherwise.
	 */
	private static int multLookupLeastSignificantBit( long x ) {
		return LSB_TABLE[ (int)( ( ( x & -x ) * 0x03f79d71b4ca8b09L ) >>> 58 ) ]; 
	}
	
	/** Returns the least significant bit of a long.
	 * 
	 * <p>This method computes the LSB of <var>x</var> by computing the MSB
	 * of <var>x</var> &amp; &minus;<var>x</var>. 
	 * 
	 * @param x a long.
	 * @return the least significant bit of <code>x</code>, of <code>x</code> is nonzero; &minus;1, otherwise.
	 */
	private static int msbBasedLeastSignificantBit( long x ) {
		return mostSignificantBit( x & -x ); 
	}
	
	public static void main( final String a[] ) {
		final long n = Long.parseLong( a[ 0 ] );
		final long incr = Long.MAX_VALUE / ( n / 2 );
				
		long start, elapsed;
		
		for( int k = 10; k-- !=0;  ) {
			System.out.print( "Broadword msb: " );
			
			start = System.currentTimeMillis();
			for( long i = n, v = 0; i-- != 0; ) mostSignificantBit( v += incr );
			elapsed = System.currentTimeMillis() - start;

			System.out.println( "elapsed " + elapsed + ", " + ( 1000000.0 * elapsed / n ) + " ns/call" );

			System.out.print( "java.lang msb: " );
			
			start = System.currentTimeMillis();
			for( long i = n, v = 0; i-- != 0; ) Long.numberOfLeadingZeros( v += incr );
			elapsed = System.currentTimeMillis() - start;

			System.out.println( "elapsed " + elapsed + ", " + ( 1000000.0 * elapsed / n ) + " ns/call" );

			System.out.print( "MSB-based lsb: " );
			
			start = System.currentTimeMillis();
			for( long i = n, v = 0; i-- != 0; ) msbBasedLeastSignificantBit( v += incr );
			elapsed = System.currentTimeMillis() - start;

			System.out.println( "elapsed " + elapsed + ", " + ( 1000000.0 * elapsed / n ) + " ns/call" );

			System.out.print( "Multiplication/lookup lsb: " );

			start = System.currentTimeMillis();
			for( long i = n, v = 0; i-- != 0; ) multLookupLeastSignificantBit( v += incr );
			elapsed = System.currentTimeMillis() - start;

			System.out.println( "elapsed " + elapsed + ", " + ( 1000000.0 * elapsed / n ) + " ns/call" );

			System.out.print( "Byte-by-byte lsb: " );
			
			start = System.currentTimeMillis();
			for( long i = n, v = 0; i-- != 0; ) leastSignificantBit( v += incr );
			elapsed = System.currentTimeMillis() - start;

			System.out.println( "elapsed " + elapsed + ", " + ( 1000000.0 * elapsed / n ) + " ns/call" );

			System.out.print( "java.lang lsb: " );
			
			start = System.currentTimeMillis();
			for( long i = n, v = 0; i-- != 0; ) Long.numberOfTrailingZeros( v += incr );
			elapsed = System.currentTimeMillis() - start;

			System.out.println( "elapsed " + elapsed + ", " + ( 1000000.0 * elapsed / n ) + " ns/call" );

		}

	}
	
}
