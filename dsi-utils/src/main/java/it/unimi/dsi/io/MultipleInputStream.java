package it.unimi.dsi.io;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2003-2009 Sebastiano Vigna 
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

import it.unimi.dsi.fastutil.objects.ObjectArrays;

import java.io.IOException;
import java.io.InputStream;

/** A multiple input stream.
 * 
 * <p>Instances of this class encapsulate a sequence of input streams.
 * When one of the streams is exhausted, the multiple stream behaves as if on an end of file.
 * However, after calling {@link #reset()} the stream is again readable, and positioned
 * at the start of the following stream.
 */

public class MultipleInputStream extends InputStream {

	/** The sequence of input streams that will be returned. */
	private final InputStream[] inputStream;
	/** The first output stream in {@link #inputStream} to be used. */
	private final int from;
	/** The last element of {@link #inputStream} to be used plus one. */
	private final int to;
	/** The index of the current input stream in {@link #inputStream}. */
	private int curr;
	/** The current input stream. */
	private InputStream currStream;
	
	/** Creates a new multiple input stream by encapsulating a nonempty fragment of an array of input streams.
	 * 
	 * @param inputStream an array of input streams, that will be encapsulated.
	 * @param offset the first input stream that will be encapsulated.
	 * @param length the number of input streams to be encapsulated; it <strong>must</strong> be positive.
	 */
	private MultipleInputStream( final InputStream[] inputStream, final int offset, final int length ) {
		ObjectArrays.ensureOffsetLength( inputStream, offset, length );
		this.inputStream = inputStream;
		this.from = offset;
		this.to = offset + length;
		
		curr = offset;
		currStream = inputStream[ curr ];
	}
	
	/** Returns an input stream encapsulating a nonempty fragment of an array of input streams.
	 * 
	 * @param inputStream an array of input streams, that will be encapsulated.
	 * @param offset the first input stream that will be encapsulated.
	 * @param length the number of input streams to be encapsulated.
	 * @return an input stream encapsulating the argument streams (the only argument, if length is 1).
	 */
	public static InputStream getStream( final InputStream[] inputStream, final int offset, final int length ) {
		if ( length == 0 ) return NullInputStream.getInstance();
		if ( length == 1 ) return inputStream[ offset ];
		return new MultipleInputStream( inputStream, offset ,length );
	}
	
	
	/** Returns an input stream encapsulating a nonempty array of input streams.
	 * 
	 * <p>Note that if <code>inputStream.length</code> is 1 this method will return the only stream
	 * that should be encapsulated. 
	 * 
	 * @param inputStream an array of input streams, that will be encapsulated.
	 * @return an input stream encapsulating the argument streams (the only argument, if the length is 1).
	 */
	public static InputStream getStream( final InputStream[] inputStream ) {
		return getStream( inputStream, 0, inputStream.length );
	}
	
	public int available() throws IOException {
		return currStream.available();
	}

	public void close() throws IOException {
		for( int i = from; i < to; i++ ) inputStream[ i ].close();
	}
	
	public boolean markSupported() {
		return false;
	}
	
	public int read() throws IOException {
		return currStream.read();
	}
	
	public int read( final byte[] b, final int off, final int len ) throws IOException {
		return currStream.read( b, off, len );
	}
	
	public int read( final byte[] b ) throws IOException {
		return currStream.read( b );
	}
	
	public synchronized void reset() throws IOException {
		if ( curr == to - 1 ) throw new IOException( "The streams in this multiple input stream have been exhausted" );
		currStream = inputStream[ ++curr ];
	}
	
	public long skip( final long n ) throws IOException {
		return currStream.skip( n );
	}
}
