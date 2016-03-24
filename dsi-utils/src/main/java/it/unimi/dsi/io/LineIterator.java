package it.unimi.dsi.io;

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

import it.unimi.dsi.fastutil.objects.AbstractObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/** An adapter that exposes a fast buffered reader as an iterator
 * over the returned lines. Since we just actually read a line to know
 * whether {@link #hasNext()} should return true, the last line read
 * from the underlying fast buffered reader has to be cached.
 * Mixing calls to this adapter and to the underlying fast buffered
 * reader will not usually give the expected results.
 * 
 * <p>This class reuses the same mutable strings. As a result,
 * the comments for {@link FileLinesCollection}
 * apply here. If you want just get all the lines, use {@link #allLines()}.
 *
 */

public class LineIterator extends AbstractObjectIterator<MutableString> {
	/** The underlying fast buffered reader. */
	private final FastBufferedReader fastBufferedReader;
	/** The mutable strings returned by this iterator. */
	private final MutableString[] s = { new MutableString(), new MutableString() };
	/** An optional progress meter. */
	private final ProgressLogger pl;
	/** Whether we must still advance to the next line. */
	private boolean toAdvance = true;
	/** In case {@link #toAdvance} is false, whether there is a next line. */
	private boolean hasNext;
	/** Which string in {@link #s} should be returned next. */
	private int k;

	/** Creates a new line iterator over a specified fast buffered reader. 
	 * 
	 * @param fastBufferedReader the underlying buffered reader.
	 * @param pl an optional progress logger, or <code>null</code>.
	 * */
	public LineIterator( final FastBufferedReader fastBufferedReader, final ProgressLogger pl ) {
		this.fastBufferedReader = fastBufferedReader;
		this.pl = pl;
	}		

	/** Creates a new line iterator over a specified fast buffered reader.
	 *  
	 * @param fastBufferedReader the underlying buffered reader.
	 */
	public LineIterator( final FastBufferedReader fastBufferedReader ) {
		this( fastBufferedReader, null );
	}
				

	public boolean hasNext() {
		if ( toAdvance ) {
			try {
				k = 1 - k;
				hasNext = fastBufferedReader.readLine( s[ k ] ) != null;
			} catch (IOException e) {
				throw new RuntimeException( e );
			}
			toAdvance = false;
		}
		
		return hasNext;
	}
					
	public MutableString next() {
		if ( ! hasNext() ) throw new NoSuchElementException();
		toAdvance = true;
		if ( pl != null ) pl.update();
		return s[ k ];
	}

	/** Returns all lines remaining in this iterator as a list.
	 * 
	 * @return all lines remaining in this iterator as a list.
	 */
	
	public List<MutableString> allLines() {
		final ObjectArrayList<MutableString> result = new ObjectArrayList<MutableString>();
		while( hasNext() ) result.add( next().copy() );
		return result;
	}
}
