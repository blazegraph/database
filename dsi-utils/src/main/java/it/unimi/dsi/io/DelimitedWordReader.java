package it.unimi.dsi.io;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2009 Sebastiano Vigna 
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


import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.chars.CharOpenHashSet;
import it.unimi.dsi.fastutil.chars.CharSet;
import it.unimi.dsi.fastutil.chars.CharSets;
import it.unimi.dsi.lang.MutableString;

import java.io.Reader;

/** A word reader that breaks words on a given set of characters.
 * 
 * <p>This class is a simple subclass of {@link FastBufferedReader}. It
 * overwrites {@link #isWordConstituent(char)} so that word constituents
 * are defined negatively by a set of <em>delimiters</em> defined at construction time.
 * There is a {@link DelimitedWordReader#DelimitedWordReader(String) constructor
 * accepting the delimiter set as a string}. Note that LF and CR are <em>always</em> considered to be delimiters.
 *
 */
public class DelimitedWordReader extends FastBufferedReader {
	private static final long serialVersionUID = 1L;

	/** The set of delimiters used to break the character stream into words. */
	private final CharOpenHashSet delimiters;
	
	@Override
	protected boolean isWordConstituent( char c ) {
		return ! delimiters.contains( c );
	}

	private void addCrLf() {
		delimiters.add( '\n' );
		delimiters.add( '\r' );
	}
	
	/** Creates a new delimited word reader with a given buffer size and set of delimiters.
	 * The wrapped reader will have to be set later using {@link #setReader(Reader)}. 
	 * 
	 * @param bufferSize the size in bytes of the internal buffer.
	 * @param delimiters a set of characters that will be considered word delimiters.
	 */
	public DelimitedWordReader( int bufferSize, CharSet delimiters ) {
		super( bufferSize );
		this.delimiters = new CharOpenHashSet( delimiters, Hash.VERY_FAST_LOAD_FACTOR );
		addCrLf();
	}

	/** Creates a new delimited word reader with a buffer of {@link #DEFAULT_BUFFER_SIZE} characters. 
	 * The wrapped reader will have to be set later using {@link #setReader(Reader)}. 
	 * @param delimiters a set of characters that will be considered word delimiters.
	 */
	public DelimitedWordReader( CharSet delimiters ) {
		this.delimiters = new CharOpenHashSet( delimiters, Hash.VERY_FAST_LOAD_FACTOR );
		addCrLf();
	}

	/** Creates a new delimited word reader with a buffer of {@link #DEFAULT_BUFFER_SIZE} characters. 
	 * The wrapped reader will have to be set later using {@link #setReader(Reader)}. 
	 * @param delimiters a set of characters that will be considered word delimiters, specified as a string.
	 */
	public DelimitedWordReader( String delimiters ) {
		this( new CharOpenHashSet( delimiters.toCharArray() ) );
	}

	/** Creates a new delimited word reader with a given buffer size and set of delimiters.
	 * The wrapped reader will have to be set later using {@link #setReader(Reader)}. 
	 * 
	 * @param bufferSize the size in bytes of the internal buffer, specified as a string.
	 * @param delimiters a set of characters that will be considered word delimiters, specified as a string.
	 */
	public DelimitedWordReader( String bufferSize, String delimiters ) {
		this( Integer.parseInt( bufferSize ), new CharOpenHashSet( delimiters.toCharArray() ) );
	}

	/** Creates a new delimited word reader by wrapping a given reader with a given buffer size and using a set of delimiters. 
	 *
	 * @param r a reader to wrap.
	 * @param bufferSize the size in bytes of the internal buffer.
	 * @param delimiters a set of characters that will be considered word delimiters.
	 */
	public DelimitedWordReader( Reader r, int bufferSize, CharSet delimiters ) {
		super( r, bufferSize );
		this.delimiters = new CharOpenHashSet( delimiters, Hash.VERY_FAST_LOAD_FACTOR );
		addCrLf();
	}

	/** Creates a new delimited word reader by wrapping a given reader with a buffer of {@link #DEFAULT_BUFFER_SIZE} characters using a given set of delimiters. 
	 *
	 * @param r a reader to wrap.
	 * @param delimiters a set of characters that will be considered word delimiters.
	 */
	public DelimitedWordReader( Reader r, CharSet delimiters ) {
		super( r );
		this.delimiters = new CharOpenHashSet( delimiters, Hash.VERY_FAST_LOAD_FACTOR );
		addCrLf();
	}

	/** Creates a new delimited word reader by wrapping a given fragment of a character array and using a set delimiters.
	 * 
	 * <p>The effect of {@link #setReader(Reader)} on a buffer created with
	 * this constructor is undefined.
	 *
	 * @param array the array that will be wrapped by the reader.
	 * @param offset the first character to be used.
	 * @param length the number of character to be used.
	 * @param delimiters a set of characters that will be considered word delimiters.
	 */
	public DelimitedWordReader( char[] array, int offset, int length, CharSet delimiters ) {
		super( array, offset, length );
		this.delimiters = new CharOpenHashSet( delimiters, Hash.VERY_FAST_LOAD_FACTOR );
		addCrLf();
	}

	/** Creates a new delimited word reader by wrapping a given character array and using a set delimiters.
	 * 
	 * <p>The effect of {@link #setReader(Reader)} on a buffer created with
	 * this constructor is undefined.
	 *
	 * @param array the array that will be wrapped by the reader.
	 * @param delimiters a set of characters that will be considered word delimiters.
	 */
	public DelimitedWordReader( char[] array, CharSet delimiters ) {
		super( array );
		this.delimiters = new CharOpenHashSet( delimiters, Hash.VERY_FAST_LOAD_FACTOR );
		addCrLf();
	}

	/** Creates a new delimited word reader by wrapping a given mutable string and using a set of delimiters.
	 * 
	 * <p>The effect of {@link #setReader(Reader)} on a buffer created with
	 * this constructor is undefined.
	 *
	 * @param s the mutable string that will be wrapped by the reader.
	 * @param delimiters a set of characters that will be considered word delimiters.
	 */
	public DelimitedWordReader( MutableString s, CharSet delimiters ) {
		super( s, CharSets.EMPTY_SET );
		this.delimiters = new CharOpenHashSet( delimiters, Hash.VERY_FAST_LOAD_FACTOR );
		addCrLf();
	}


	public String toSpec() {
		return toString();
	}
	
	public String toString() {
		final String className = getClass().getName();
		CharOpenHashSet additionalDelimiters = (CharOpenHashSet)delimiters.clone();
		additionalDelimiters.remove( '\n' );
		additionalDelimiters.remove( '\r' );
		String delimiters = new String( additionalDelimiters.toCharArray() );
		if ( bufferSize == DEFAULT_BUFFER_SIZE ) return className + "(\"" + delimiters + "\")";
		return className + "(" + bufferSize + ",\"" + delimiters + "\")";
	}
}
