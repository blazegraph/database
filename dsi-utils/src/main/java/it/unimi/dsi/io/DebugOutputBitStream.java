package it.unimi.dsi.io;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2006-2009 Paolo Boldi and Sebastiano Vigna 
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

import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.io.PrintStream;

/** A debugging wrapper for output bit streams.
 *
 * <P>This class can be used to wrap an output bit stream. The semantics of the
 * resulting write operations is unchanged, but each operation will be logged.
 *
 * <P>To simplify the output, some operations have a simplified representation. In particular:
 *
 * <dl>
 * <dt><samp>|</samp>
 * <dd>{@link OutputBitStream#flush() flush()};
 * <dt><samp>-></samp>
 * <dd>{@link OutputBitStream#position(long) position()};
 * <dt><samp>[</samp>
 * <dd>creation;
 * <dt><samp>]</samp>
 * <dd>{@link OutputBitStream#close() close()};
 * <dt><samp>{<var>x</var>}</samp>
 * <dd>explicit bits;
 * <dt><samp>{<var>x</var>:<var>b</var>}</samp>
 * <dd>minimal binary coding of <var>x</var> with bound <var>b</var>;
 * <dt><samp>{<var>M</var>:<var>x</var>}</samp>
 * <dd>write <var>x</var> with coding <var>M</var>; the latter can be U (unary), g (&gamma;), z (&zeta;), d (&delta;), G (Golomb), GS (skewed Golomb);
 * when appropriate, <var>x</var> is followed by an extra integer (modulus, etc.).
 * </dl>
 *
 * @author Paolo Boldi
 * @author Sebastiano Vigna
 * @since 0.7.1
 */

public class DebugOutputBitStream extends OutputBitStream {

	private final PrintStream pw;
	private final OutputBitStream obs;
	 
	/** Creates a new debug output bit stream wrapping a given output bit stream and logging on a given writer.
	 *
	 * @param obs the output bit stream to wrap.
	 * @param pw a print stream that will receive the logging data.
	 */
	public DebugOutputBitStream( final OutputBitStream obs, final PrintStream pw ) {
		this.obs = obs;
		this.pw = pw;
		pw.print( "[" );
	}

	/** Creates a new debug output bit stream wrapping a given output bit stream and logging on standard error.
	 *
	 * @param obs the output bit stream to wrap.
	 */
	public DebugOutputBitStream( final OutputBitStream obs ) {
		this( obs, System.err );
	}

	public void flush() throws IOException {
		pw.print( " |" );
		obs.flush();
	}

	public void close() throws IOException {
		pw.print( " |]" );
		obs.close();
	}

	public long writtenBits() {
		return obs.writtenBits();
	}

	public void writtenBits( final long writtenBits ) {
		obs.writtenBits( writtenBits );
	}

	public int align() throws IOException {
		pw.print( " |" );
		return obs.align();
	}

    public void position( final long position ) throws IOException {
		pw.print( " ->" + position );
		obs.position( position );
	}

	static MutableString byte2Binary( int x ) {
		MutableString s = new MutableString();
		for( int i = 0 ; i < 8; i++ ) {
			s.append( (char)( '0' + ( x % 2 ) ) );
			x >>= 1;
		}
		return s.reverse();
	}

	static MutableString int2Binary( long x, final int len ) {
		MutableString s = new MutableString();
		for( int i = 0 ; i < 64; i++ ) {
			s.append( (char)( '0' + ( x % 2 ) ) );
			x >>= 1;
		}
		return s.length( len ).reverse();
	}

	public long write( final byte bits[], final long len ) throws IOException {
		if ( len > Integer.MAX_VALUE ) throw new IllegalArgumentException();
		MutableString s = new MutableString( " {" );
		for( int i = 0; i < bits.length; i++ ) s.append( byte2Binary( bits[ i ] ) );
		pw.print( s.length( (int)len ).append( "}" ) );
		return obs.write( bits, len );
	}

	public int writeBit( final boolean bit ) throws IOException {
		pw.print( " {" + ( bit ? '1' : '0' ) + "}" );
		return obs.writeBit( bit );
	}

	public int writeBit( final int bit ) throws IOException {
		pw.print( " {" + bit + "}" );
		return obs.writeBit( bit );
	}

	public int writeInt( final int x, final int len ) throws IOException {
		pw.print( " {" + int2Binary( x, len ) + "}" );
		return obs.writeInt( x, len );
	}

	public int writeLong( final long x, final int len ) throws IOException {
		pw.print( " {" + int2Binary( x, len ) + "}" );
		return obs.writeLong( x, len );
	}

	public int writeUnary( final int x ) throws IOException {
		pw.print( " {U:" + x + "}" );
		return obs.writeUnary( x );
	}

	public long writeLongUnary( final long x ) throws IOException {
		pw.print( " {U:" + x + "}" );
		return obs.writeLongUnary( x );
	}

	public int writeGamma( final int x ) throws IOException {
		pw.print( " {g:" + x + "}" );
		return obs.writeGamma( x );
	}

	public int writeLongGamma( final long x ) throws IOException {
		pw.print( " {g:" + x + "}" );
		return obs.writeLongGamma( x );
	}

	public int writeDelta( final int x ) throws IOException {
		pw.print( " {d:" + x + "}" );
		return obs.writeDelta( x );
	}

	public int writeLongDelta( final long x ) throws IOException {
		pw.print( " {d:" + x + "}" );
		return obs.writeLongDelta( x );
	}

	public int writeMinimalBinary( final int x, final int b ) throws IOException {
		pw.print( " {m:" + x + "<" + b + "}" );
		return obs.writeMinimalBinary( x, b );
	}

	public int writeMinimalBinary( final int x, final int b, final int log2b ) throws IOException {
		pw.print( " {m:" + x + "<" + b + "}" );
		return obs.writeMinimalBinary( x, b, log2b );
	}

	public int writeLongMinimalBinary( final long x, final long b ) throws IOException {
		pw.print( " {m:" + x + "<" + b + "}" );
		return obs.writeLongMinimalBinary( x, b );
	}

	public int writeLongMinimalBinary( final long x, final long b, final int log2b ) throws IOException {
		pw.print( " {m:" + x + "<" + b + "}" );
		return obs.writeLongMinimalBinary( x, b, log2b );
	}

	public int writeGolomb( final int x, final int b ) throws IOException {
		pw.print( " {G:" + x + ":" + b + "}" );
		return obs.writeGolomb( x, b );
	}

	public int writeGolomb( final int x, final int b, final int log2b ) throws IOException {
		pw.print( " {G:" + x + ":" + b + "}" );
		return obs.writeGolomb( x, b, log2b );
	}

	public long writeLongGolomb( final long x, final long b ) throws IOException {
		pw.print( " {G:" + x + ":" + b + "}" );
		return obs.writeLongGolomb( x, b );
	}

	public long writeLongGolomb( final long x, final long b, final int log2b ) throws IOException {
		pw.print( " {G:" + x + ":" + b + "}" );
		return obs.writeLongGolomb( x, b, log2b );
	}

	public int writeSkewedGolomb( final int x, final int b ) throws IOException {
		pw.print( " {SG:" + x + ":" + b + "}" );
		return obs.writeSkewedGolomb( x, b );
	}

	public long writeLongSkewedGolomb( final long x, final long b ) throws IOException {
		pw.print( " {SG:" + x + ":" + b + "}" );
		return obs.writeLongSkewedGolomb( x, b );
	}

	public int writeZeta( final int x, final int k ) throws IOException {
		pw.print( " {z" + k + ":" + x + "}" );
		return obs.writeZeta( x, k );
	}

	public int writeLongZeta( final long x, final int k ) throws IOException {
		pw.print( " {z" + k + ":" + x + "}" );
		return obs.writeLongZeta( x, k );
	}
	
	public int writeNibble( final int x ) throws IOException {
		pw.print( " {N:" + x + "}" );
		return obs.writeNibble( x );
	}
	
	public int writeLongNibble( final long x ) throws IOException {
		pw.print( " {N:" + x + "}" );
		return obs.writeLongNibble( x );
	}

}
