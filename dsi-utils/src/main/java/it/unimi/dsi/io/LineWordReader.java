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
import java.io.Reader;
import java.io.Serializable;

/** A trivial {@link it.unimi.dsi.io.WordReader} that considers each line
 * of a document a single word.
 * 
 * <p>The intended usage of this class is that of indexing stuff like lists of document
 * identifiers: if the identifiers contain nonalphabetical characters, the default
 * {@link it.unimi.dsi.io.FastBufferedReader} might do a poor job. 
 * 
 * <p>Note that the non-word returned by {@link #next(MutableString, MutableString)} is
 * always empty.
 */

public class LineWordReader implements WordReader, Serializable {
	private static final long serialVersionUID = 1L;
	/** An fast buffered reader wrapping the underlying reader. */
	private FastBufferedReader fastBufferedReader = new FastBufferedReader();

	public boolean next( final MutableString word, final MutableString nonWord ) throws IOException {
		nonWord.length( 0 );
		return fastBufferedReader.readLine( word ) != null;
	}
	
	public LineWordReader setReader( final Reader reader ) {
		fastBufferedReader.setReader( reader );
		return this;
	}
	
	public LineWordReader copy() {
		return new LineWordReader();
	}
}
