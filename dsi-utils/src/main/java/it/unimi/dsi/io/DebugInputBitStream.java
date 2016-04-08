package it.unimi.dsi.io;

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

import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.io.PrintStream;


/** A debugging wrapper for input bit streams.
 *
 * <P>This class can be used to wrap an input bit stream. The semantics of the
 * resulting read operations is unchanged, but each operation will be logged. The
 * conventions are the same as those of {@link it.unimi.dsi.io.DebugOutputBitStream},
 * with the following additions:
 * 
 * <dl>
 * <dt><samp>!</samp>
 * <dd>{@link InputBitStream#reset() reset()};
 * <dt><samp>+></samp>
 * <dd>{@link InputBitStream#skip(long) skip()}.
 * </dl>
 *
 * @author Sebastiano Vigna
 * @since 1.1
 */

public class DebugInputBitStream extends InputBitStream {

	private final PrintStream pw;
	private final InputBitStream ibs;
	 
	/** Creates a new debug input bit stream wrapping a given input bit stream and logging on a given writer.
	 *
	 * @param ibs the input bit stream to wrap.
	 * @param pw a print stream that will receive the logging data.
	 */
	public DebugInputBitStream( final InputBitStream ibs, final PrintStream pw ) {
		this.ibs = ibs;
		this.pw = pw;
		pw.print( "[" );
	}

	/** Creates a new debug input bit stream wrapping a given input bit stream and logging on standard error.
	 *
	 * @param ibs the input bit stream to wrap.
	 */
	public DebugInputBitStream( final InputBitStream ibs ) {
		this( ibs, System.err );
	}

	
	@Override
	public void align() {
		pw.print( " |" );
		ibs.align();
	}

	@Override
	public long available() throws IOException {
		return ibs.available();
	}

	@Override
	public void close() throws IOException {
		pw.print( " |]" );
		ibs.close();
	}

	@Override
	public void flush() {
		pw.print( " |" );
		ibs.flush();
	}

	@Override
	public void position( long position ) throws IOException {
		pw.print( " ->" + position );
		ibs.position( position );
	}

	@Override
	public void read( byte[] bits, int len ) throws IOException {
		ibs.read( bits, len );
		MutableString s = new MutableString( " {" );
		for( int i = 0; i < bits.length; i++ ) s.append( DebugOutputBitStream.byte2Binary( bits[ i ] ) );
		pw.print( s.length( len ).append( "}" ) );
	}

	@Override
	public int readBit() throws IOException {
		final int bit = ibs.readBit();
		pw.print( " {" + bit + "}" );
		return bit;
	}

	@Override
	public long readBits() {
		return ibs.readBits();
	}

	@Override
	public void readBits( long readBits ) {
		ibs.readBits( readBits );
	}

	@Override
	public int readDelta() throws IOException {
		final int x = ibs.readDelta();
		pw.print( " {d:" + x + "}" );
		return x;
	}

	@Override
	public int readGamma() throws IOException {
		final int x = ibs.readGamma();
		pw.print( " {g:" + x + "}" );
		return x;
	}

	@Override
	public int readGolomb( int b, int log2b ) throws IOException {
		final int x = ibs.readGolomb( b, log2b );
		pw.print( " {G:" + x + ":" + b + "}" );
		return x;
	}

	@Override
	public int readGolomb( int b ) throws IOException {
		final int x = ibs.readGolomb( b );
		pw.print( " {G:" + x + ":" + b + "}" );
		return x;
	}

	@Override
	public int readInt( int len ) throws IOException {
		final int x = ibs.readInt( len );
		pw.print( " {" + DebugOutputBitStream.int2Binary( x, len ) + "}" );
		return x;
	}

	@Override
	public long readLong( int len ) throws IOException {
		final long x = ibs.readLong( len );
		pw.print( " {" + DebugOutputBitStream.int2Binary( x, len ) + "}" );
		return x;
	}

	@Override
	public long readLongDelta() throws IOException {
		final long x = ibs.readLongDelta();
		pw.print( " {d:" + x + "}" );
		return x;
	}

	@Override
	public long readLongGamma() throws IOException {
		final long x = ibs.readLongGamma();
		pw.print( " {g:" + x + "}" );
		return x;
	}

	@Override
	public long readLongGolomb( long b, int log2b ) throws IOException {
		final long x = ibs.readLongGolomb( b, log2b );
		pw.print( " {G:" + x + ":" + b + "}" );
		return x;
	}

	@Override
	public long readLongGolomb( long b ) throws IOException {
		final long x = ibs.readLongGolomb( b );
		pw.print( " {G:" + x + ":" + b + "}" );
		return x;
	}

	@Override
	public long readLongMinimalBinary( long b, int log2b ) throws IOException {
		final long x = ibs.readLongMinimalBinary( b, log2b );
		pw.print( " {m:" + x + "<" + b + "}" );
		return x;
	}

	@Override
	public long readLongMinimalBinary( long b ) throws IOException {
		final long x = ibs.readLongMinimalBinary( b );
		pw.print( " {m:" + x + "<" + b + "}" );
		return x;
	}

	@Override
	public long readLongNibble() throws IOException {
		final long x = ibs.readLongNibble();
		pw.print( " {N:" + x + "}" );
		return x;
	}

	@Override
	public long readLongSkewedGolomb( long b ) throws IOException {
		final long x = ibs.readLongSkewedGolomb( b );
		pw.print( " {SG:" + x + ":" + b + "}" );
		return x;
	}

	@Override
	public long readLongUnary() throws IOException {
		final long x = ibs.readLongUnary();
		pw.print( " {U:" + x + "}" );
		return x;
	}

	@Override
	public long readLongZeta( int k ) throws IOException {
		final long x = ibs.readLongZeta( k );
		pw.print( " {z" + k + ":" + x + "}" );
		return x;
	}

	@Override
	public int readMinimalBinary( int b, int log2b ) throws IOException {
		final int x = ibs.readMinimalBinary( b, log2b );
		pw.print( " {m:" + x + "<" + b + "}" );
		return x;
		
	}

	@Override
	public int readMinimalBinary( int b ) throws IOException {
		final int x = ibs.readMinimalBinary( b );
		pw.print( " {m:" + x + "<" + b + "}" );
		return x;
	}

	@Override
	public int readNibble() throws IOException {
		final int x = ibs.readNibble();
		pw.print( " {N:" + x + "}" );
		return x;
	}

	@Override
	public int readSkewedGolomb( int b ) throws IOException {
		final int x = ibs.readSkewedGolomb( b );
		pw.print( " {SG:" + x + ":" + b + "}" );
		return x;
	}

	@Override
	public int readUnary() throws IOException {
		final int x = ibs.readUnary();
		pw.print( " {U:" + x + "}" );
		return x;
	}

	@Override
	public int readZeta( int k ) throws IOException {
		final int x = ibs.readZeta( k );
		pw.print( " {z" + k + ":" + x + "}" );
		return x;
	}

	@Override
	public void reset() throws IOException {
		pw.print( " {!}" );
		ibs.reset();
	}

	@Override
	@Deprecated
	public int skip( int n ) {
		pw.print( " {+>" + n + "}" );
		return ibs.skip( n );
	}

	@Override
	public long skip( long n ) throws IOException {
		pw.print( " {+>" + n + "}" );
		return ibs.skip( n );
	}

}
