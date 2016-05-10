package it.unimi.dsi.io;

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

import it.unimi.dsi.fastutil.io.MeasurableInputStream;
import it.unimi.dsi.fastutil.io.RepositionableStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;


/** A bridge between byte {@linkplain ByteBuffer buffers} and {@linkplain InputStream input streams}.
 * 
 * <p>The main usefulness of this class is that of making it possible creating input streams
 * that are really based on a {@link MappedByteBuffer}.
 * 
 * <p>In particular, the factory method {@link #map(FileChannel, FileChannel.MapMode)}
 * will memory-map an entire file into an array of {@link ByteBuffer} and expose the array as
 * a {@link ByteBufferInputStream}. This makes it possible to access easily mapped files larger
 * than 2GiB. 
 * 
 * @author Sebastiano Vigna
 * @since 1.2
 */

public class ByteBufferInputStream extends MeasurableInputStream implements RepositionableStream {
	private static int FIRST_SHIFT = 30;
	private static int SECOND_SHIFT = FIRST_SHIFT - 1;
	
	/** The size of a chunk created by {@link #map(FileChannel, FileChannel.MapMode)}. */
	public static final long CHUNK_SIZE = ( 1 << FIRST_SHIFT ) + ( 1 << SECOND_SHIFT );

	/** The underlying byte buffers. */
	private final ByteBuffer[] byteBuffer;

	/** The number of byte buffers. */
	private final int n;
	
	/** The current buffer. */
	private int curr = 0;

	/** The current mark as a position, or -1 if there is no mark. */
	private long mark;

	/** The overall size of this input steam. */
	private long size;
	
	/** Creates a new byte-buffer input stream from a single {@link ByteBuffer}.
	 * 
	 * @param byteBuffer the underlying byte buffer.
	 */
	
	public ByteBufferInputStream( final ByteBuffer byteBuffer ) {
		this( new ByteBuffer[] { byteBuffer } );
	}

	/** Creates a new byte-buffer input stream.
	 * 
	 * @param byteBuffer the underlying byte buffers.
	 */
	
	protected ByteBufferInputStream( final ByteBuffer[] byteBuffer ) {
		this.byteBuffer = byteBuffer;
		this.n = byteBuffer.length;
		long size = 0;
		for( int i = 0; i < n; i++ ) {
			if ( i < n - 1 && byteBuffer[ i ].capacity() != CHUNK_SIZE ) throw new IllegalArgumentException();
			size += byteBuffer[ i ].capacity();
		}
		this.size = size;
		position( 0 );
	}

	public static ByteBufferInputStream map( final FileChannel fileChannel, MapMode mapMode ) throws IOException {
		final long size = fileChannel.size();
		final int chunks = (int)( ( size + ( CHUNK_SIZE - 1 ) ) / CHUNK_SIZE );
		final ByteBuffer[] byteBuffer = new ByteBuffer[ chunks ];
		for( int i = 0; i < chunks; i++ ) byteBuffer[ i ] = fileChannel.map( mapMode, CHUNK_SIZE * i, Math.min( CHUNK_SIZE, size - CHUNK_SIZE * i ) );

		return new ByteBufferInputStream( byteBuffer );
	}
	
	
	private long remaining() {
		return curr == n - 1 ? byteBuffer[ curr ].remaining() :

			byteBuffer[ curr ].remaining() + ( (long)( n - 2 - curr ) << FIRST_SHIFT ) + ( (long)( n - 2 - curr ) << SECOND_SHIFT ) + byteBuffer[ n - 1 ].capacity();
	}
	
	public int available() {
		final long available = remaining(); 
		return available < Integer.MAX_VALUE ? (int)available : Integer.MAX_VALUE;
	}

	@Override
	public boolean markSupported() {
		return true;
	}

	@Override
	public synchronized void mark( final int unused ) {
		mark = position();
	}

	@Override
	public synchronized void reset() throws IOException {
		if ( mark == -1 ) throw new IOException();
		position( mark );
	}

	@Override
	public long skip( final long n ) throws IOException {
		final long toSkip = Math.min( remaining(), n );
		position( position() + toSkip );
		return toSkip;
	}

	@Override
	public int read() {
		if ( ! byteBuffer[ curr ].hasRemaining() ) {
			if ( curr < n - 1 ) byteBuffer[ ++curr ].position( 0 );
			else return -1;
		}

		return byteBuffer[ curr ].get() & 0xFF;
	}

	public int read( final byte[] b, final int offset, final int length ) {
		if ( length == 0 ) return 0;
		final long remaining = remaining(); 
		if ( remaining == 0 ) return -1;
		final int realLength = (int)Math.min( remaining, length );
		int read = 0;
		while( read < realLength ) {
			int rem = byteBuffer[ curr ].remaining();
			if ( rem == 0 ) byteBuffer[ ++curr ].position( 0 );
			byteBuffer[ curr ].get( b, offset + read, Math.min( realLength - read, rem ) );
			read += Math.min( realLength, rem );
		}
		return realLength;
	}

	@Override
	public long length() {
		return size;
	}

	@Override
	public long position() {
		return ( (long)curr << FIRST_SHIFT ) + ( (long)curr << SECOND_SHIFT ) + byteBuffer[ curr ].position();
	}
	
	public void position( long newPosition ) {
		newPosition = Math.min( newPosition, length() );
		if ( newPosition == length() ) {
			curr = n - 1;
			byteBuffer[ curr ].position( byteBuffer[ curr ].capacity() );
			return;
		}
		
		curr = (int)( newPosition / CHUNK_SIZE );
		byteBuffer[ curr ].position( (int)( newPosition - ( (long)curr << FIRST_SHIFT ) - ( (long)curr << SECOND_SHIFT ) ) );
	}
	
	public ByteBufferInputStream copy() {
		final ByteBuffer[] byteBuffer = new ByteBuffer[ this.byteBuffer.length ];
		for( int i = this.byteBuffer.length; i-- != 0; ) byteBuffer[ i ] = this.byteBuffer[ i ].duplicate();
		return new ByteBufferInputStream( byteBuffer );
	}
}
