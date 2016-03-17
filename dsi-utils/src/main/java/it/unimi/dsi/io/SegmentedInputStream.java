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

import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.io.MeasurableInputStream;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

/** Exhibits a single {@link InputStream} as a number of streams divided into {@link java.io.InputStream#reset() reset()}-separated
 * segments.
 * 
 * <p>An instance of this class wraps a given input stream (usually a replicable one, such as
 * a {@link java.io.FileInputStream}) and exposes its contents as a number of separated input
 * streams. Each such stream, called a <em>block</em>, is defined by a start and a stop position (gaps
 * between blocks are possible). Inside each block we can have one or more <em>segments</em>: each
 * segment is again a separate input stream, but calling {@link SegmentedInputStream#reset()} moves
 * from one segment to the following one, whereas calling {@link SegmentedInputStream#close()}
 * moves from a block to the following one.
 * 
 * <p>An instance of this class is enriched with blocks by calling {@link #addBlock(long[])}. This
 * can also be done on-the-fly, while the underlying input stream is being scanned.
 * 
 * @author Alessio Orlandi
 * @author Luca Natali
 * @author Sebastiano Vigna
 */
public class SegmentedInputStream extends MeasurableInputStream {
	private static final boolean DEBUG = false;
	private static final Logger LOGGER = Util.getLogger( SegmentedInputStream.class );
	
	/** Underlying input stream. */
	private InputStream in;
	/** Relative position within the current segment. */
	private int relativePos;
	/** Byte length of the current segment. */
	private int segmentLen; 
	/** List of known blocks. */
	private ObjectArrayList<SegmentBlock> blocks;
	/** The start marker of the current segment. */
	private long currentStartMarker;
	/** The stop marker of the current segment. */	
	private long currentStopMarker;
	/** Index in {@link #blocks}, -1 when no blocks are in. */
	private int currentBlock; 
	/** Whether we actually closed the whole thing. */
	private boolean closed;
	
	/** A block. */
	private static class SegmentBlock {
		/** Segments delimiters, strictly increasing. */
		final long[] delimiter;
		/** The segment we're currently reading. */
		int currSegment;

		/** Creates a new block with given delimiters.
		 * 
		 * @param delimiter a list of segment delimiters.
		 * @throws IllegalArgumentException if the elements of <code>delimiter</code> are negative or not increasing.
		 */
		public SegmentBlock( long... delimiter ) throws IllegalArgumentException {
			if ( delimiter.length == 0 ) throw new IllegalArgumentException();

			for ( int i = 0; i < delimiter.length - 1; i++ )
				if ( delimiter[ i ] > delimiter[ i + 1 ] ) throw new IllegalArgumentException( "Segment " + ( i + 1 ) + " is inconsistent as it starts after the next one: " + Arrays.toString( delimiter ) );

			this.delimiter = delimiter;
			this.currSegment = -1;
		}

		public String toString() {
			return "[segments=" + Arrays.toString( delimiter ) + ", curr= " + currSegment  + "]";
		}

		/**
		 * Skips to the next segment. Now {@link #currentStartMarker()} and {@link #currentStopMarker()} can be used.
		 */
		public void nextSegment() {
			if ( ! hasMoreSegments() ) throw new NoSuchElementException();
			currSegment++;
		}

		public boolean hasMoreSegments() {
			return currSegment < delimiter.length - 2;
		}

		/** Start marker of the current segment (block start if the first segment is selected). */
		public long currentStartMarker() {
			return delimiter[ currSegment ];
		}

		/** Stop marker of the current segment (block stop if the last segment is selected) */
		public long currentStopMarker() {
			return delimiter[ currSegment + 1 ];
		}

	}

	private void ensureBlocksNotEmpty() {
		if ( blocks.isEmpty() ) throw new IllegalStateException( "You must add at least one block before reading or closing a segmented stream" );
	}

	private void ensureNotClosed() {
		if ( closed ) throw new IllegalStateException( "This segmented input stream has been closed" );
	}

	
	/** Creates a segmented input stream with no markers.
	 * 
	 * @param in the underlying input stream.  
	 */
	public SegmentedInputStream( final InputStream in ) {
		if ( in == null ) throw new NullPointerException();
		this.in = in;
		this.blocks = new ObjectArrayList<SegmentBlock>();
		this.currentBlock = -1;
	}

	/** Creats a stream with one marker in. 
	 * 
	 * @param in the underlying input stream.  
	 * @param delimiter an array of segment delimiters.  
	 */
	public SegmentedInputStream( final InputStream in, final long... delimiter ) throws NullPointerException, IOException, IllegalStateException {
		this( in );
		addBlock( delimiter );
	}


	/** Checks if the current position is a stop marker.
	 * 
	 * @return false if a skip has to be done or eof has been reached, true otherwise.
	 */
	private boolean eofInBlock() {
		ensureBlocksNotEmpty();
		ensureNotClosed();
		return relativePos >= segmentLen;
	}

	/** Skips the underlying input stream to the next segment. */
	private void nextSegment() throws IOException {
		ensureNotClosed();
		final SegmentBlock block = blocks.get( currentBlock );
		if ( ! block.hasMoreSegments() ) return;
		block.nextSegment();

		long absPos = currentStartMarker + relativePos;

		currentStartMarker = block.currentStartMarker();
		currentStopMarker = block.currentStopMarker();

		if ( currentStartMarker - absPos > 0 ) {
			long diff = in.skip( currentStartMarker - absPos );
			if ( diff != currentStartMarker - absPos ) throw new IllegalStateException( "Should have skipped " + ( currentStartMarker - absPos ) + " bytes, got " + diff );
		}

		relativePos = 0;
		segmentLen = (int)( currentStopMarker - currentStartMarker );

		if ( DEBUG ) LOGGER.debug( "New segment for block # " + currentBlock );
	}

	/** Skips to the first segment of the next block, if any. In such case, it returns true, or false
	 * otherwise.
	 */
	public void nextBlock() throws IOException {
		if ( ! hasMoreBlocks() ) throw new NoSuchElementException();
		currentBlock++;
		if ( DEBUG ) LOGGER.debug( "Moving to block # " + currentBlock );
		nextSegment();
	}

	/** Checks whether there are more blocks.
	 * 
	 * @return true if we there are more blocks.
	 */
	
	public boolean hasMoreBlocks() {
		return currentBlock < blocks.size() - 1;
	}

	/** Adds a new block defined by its array of segment delimiters.
	 * 
	 * <p>The block has length defined by the difference between the last and first
	 * delimiter.
	 *  
	 * <p>This method performs the initial call to {@link #nextBlock()} when the first marker
	 * is put in.
	 * 
	 * @param delimiter a list of segment delimiters.
	 * @throws IllegalArgumentException if the elements of <code>delimiter</code> are negative or not increasing.
	 */
	public void addBlock( final long... delimiter ) throws IllegalArgumentException, IOException {
		ensureNotClosed();
		blocks.add( new SegmentBlock( delimiter ) );
		if ( DEBUG ) LOGGER.debug( "Adding a new block with delimiters " + Arrays.toString( delimiter ) );
		if ( currentBlock == -1 ) nextBlock();
	}

	public int read() throws IOException {
		ensureNotClosed();
		if ( eofInBlock() ) return -1;
		final int r = in.read();
		relativePos++;
		return r;
	}

	public int read( final byte b[], final int off, final int len ) throws IOException {
		ensureNotClosed();
		ByteArrays.ensureOffsetLength( b, off, len );
		if ( len == 0 ) return 0; // Requested by InputStream.
		if ( eofInBlock() ) return -1;
		int effectivelen = Math.min( segmentLen - relativePos, len );
		effectivelen = in.read( b, off, effectivelen );
		relativePos += effectivelen;
		return effectivelen;
	}

	public long skip( final long n ) throws IOException {
		ensureNotClosed();
		if ( eofInBlock() ) return 0;
		long effectiveskip = Math.max( Math.min( segmentLen - relativePos, n ), 0 );
		effectiveskip = in.skip( effectiveskip );
		relativePos += effectiveskip;
		return effectiveskip;
	}

	public int available() throws IOException {
		ensureNotClosed();
		if ( eofInBlock() ) return 0;
		return Math.min( in.available(), segmentLen - relativePos );
	}

	@Override
	public long length() throws IOException {
		ensureNotClosed();
		return segmentLen;
	}

	@Override
	public long position() throws IOException {
		ensureNotClosed();
		return relativePos;
	}
	
	/** Skips to the next block, closing this segmented input stream if there are no more blocks. */
	public void close() throws IOException {
		ensureBlocksNotEmpty();
		if ( closed ) return;
		
		if ( hasMoreBlocks() ) {
			nextBlock();
			return;
		}
		
		if ( DEBUG ) LOGGER.debug( "Closing the underlying input stream of this segmented input stream" );
		closed = true;
		in.close();
	}

	/** Moves into the next segment of the current block. */
	public void reset() throws IOException {
		ensureNotClosed();
		nextSegment();
	}
}
