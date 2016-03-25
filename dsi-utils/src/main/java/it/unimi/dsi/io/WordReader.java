package it.unimi.dsi.io;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2005-2009 Paolo Boldi and Sebastiano Vigna 
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

/** An interface providing methods to break the input from a reader into words.
 * 
 * <p>The intended implementations of this interface should decorate
 * a given reader (see, for instance, {@link it.unimi.dsi.io.FastBufferedReader}).
 * The reader can be changed at any time using {@link #setReader(Reader)}.
 * 
 * <p>This interface is heavily oriented towards reusability and
 * streaming. It is conceived so that at most one method call has
 * to be performed per <em>word</em>, rather than per <em>character</em>,
 * and that implementations may completely avoid object creation by
 * {@linkplain #setReader(Reader) setting explicitly the underlying reader}.
 * 
 * <p>The standard implementation ({@link it.unimi.dsi.io.FastBufferedReader}) breaks
 * words in the trivial way. More complex implementations (e.g., for languages requiring
 * segmentation) can subclass {@link it.unimi.dsi.io.FastBufferedReader} or provide their
 * own implementation.
 */

public interface WordReader extends Serializable {
	/** Extracts the next word and non-word.
	 * 
	 * <p>If this method returns true, a new non-empty word, and possibly
	 * a new non-word, have been extracted. It is acceptable
	 * that the <em>first</em> call to this method after creation
	 * or after a call to {@link #setReader(Reader)} returns an empty
	 * word. In other words <em>both <code>word</code> and <code>nonWord</code> are maximal</em>.
	 * 
	 * @param word the next word returned by the underlying reader.
	 * @param nonWord the nonword following the next word returned by the underlying reader.
	 * @return true if a new word was processed, false otherwise (in which
	 * case both <code>word</code> and <code>nonWord</code> are unchanged).
	 */
	
	public abstract boolean next( MutableString word, MutableString nonWord ) throws IOException;
	
	/** Resets the internal state of this word reader, which will start again reading from the given reader.
	 * 
	 * @param reader the new reader providing characters.
	 * @return this word reader.
	 */
	
	public abstract WordReader setReader( Reader reader );
	
	/** Returns a copy of this word reader. 
	 * 
	 * <p>This method must return a word reader with a behaviour that
	 * matches exactly that of this word reader.
	 * 
	 * @return a copy of this word reader.
	 */
	
	public abstract WordReader copy();
}
