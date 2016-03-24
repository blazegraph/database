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

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.chars.CharArrays;
import it.unimi.dsi.fastutil.chars.CharOpenHashSet;
import it.unimi.dsi.fastutil.chars.CharSet;
import it.unimi.dsi.fastutil.chars.CharSets;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Reader;

/** A lightweight, unsynchronised buffered reader based on 
 * {@linkplain it.unimi.dsi.lang.MutableString mutable strings}.
 *
 * <P>This class provides buffering for readers, but it does so with 
 * purposes and an internal logic that are radically different from the ones
 * adopted in {@link java.io.BufferedReader}.
 * 
 * <P>There is no support for marking. All methods are unsychronised. All
 * methods returning strings do so by writing in a given {@link it.unimi.dsi.lang.MutableString}.
 * 
 * <p>Note that instances of this class can wrap {@linkplain #FastBufferedReader(char[]) an array}
 * or a {@linkplain #FastBufferedReader(MutableString) mutable string}. In this case,
 * instances of this class may be used as a lightweight, unsynchronised 
 * alternative to {@link java.io.CharArrayReader}
 * providing additional services such as word and line breaking.
 * 
 * <p>As any {@link it.unimi.dsi.io.WordReader}, this class is serialisable.
 * The only field kept is the current buffer size, which will be used to rebuild
 * a fast buffered reader with the same buffer size. All other fields will be reset.
 * 
 * <h2>Reading words</h2>
 * 
 * <p>This class implements {@link WordReader} in the simplest way: words are defined as
 * maximal subsequences of characters satisfying {@link Character#isLetterOrDigit(char)}.
 * To alter this behaviour, you have two choices:
 * <ul>
 * <li>you can provide at construction time a {@link CharSet} of characters that will be considered <em>word constituents</em>
 * besides those accepted by {@link Character#isLetterOrDigit(char)};
 * <li>you can override the method {@link #isWordConstituent(char)}.
 * </ul>
 * 
 * <p>The second approach is of course more flexible, but the first one is particularly useful from
 * the command line as there is a {@link FastBufferedReader#FastBufferedReader(String) constructor
 * accepting the additional word constituents as a string}.
 */

public class FastBufferedReader extends Reader implements WordReader {

	public static final long serialVersionUID = 1L;

	/** The default size of the internal buffer in bytes (16Ki). */
	public final static int DEFAULT_BUFFER_SIZE = 16 * 1024;

	/** The buffer size (must be equal to {@link #buffer buffer.length}). */
	protected final int bufferSize;
	
	/** A set of additional characters that will be considered as word constituents, beside those accepted by {@link Character#isLetterOrDigit(int)}. */
	protected final CharSet wordConstituents;
	
	/** The internal buffer. */
	protected transient char[] buffer;

	/** The current position in the buffer. */
	protected transient int pos;

	/** The number of buffer bytes available starting from {@link #pos}. */
	protected transient int avail;

	/** The underlying reader. */
	protected transient Reader reader;

	/** Creates a new fast buffered reader with a given buffer size.
	 * The wrapped reader will have to be set later using {@link #setReader(Reader)}. 
	 *
	 * @param bufferSize the size in bytes of the internal buffer.
	 */

	public FastBufferedReader( final int bufferSize ) {
		this ( bufferSize, CharSets.EMPTY_SET );
	}

	/** Creates a new fast buffered reader with a given buffer size and set of additional word constituents.
	 * The wrapped reader will have to be set later using {@link #setReader(Reader)}. 
	 *
	 * @param bufferSize the size in bytes of the internal buffer.
	 * @param wordConstituents a set of characters that will be considered word constituents.
	 */

	public FastBufferedReader( final int bufferSize, final CharSet wordConstituents ) {
		buffer = new char[ this.bufferSize = bufferSize ];
		this.wordConstituents = wordConstituents;
	}

	/** Creates a new fast buffered reader with a buffer of {@link #DEFAULT_BUFFER_SIZE} characters. 
	 * The wrapped reader will have to be set later using {@link #setReader(Reader)}. 
	 */
	public FastBufferedReader() {
		this( DEFAULT_BUFFER_SIZE );
	}


	/** Creates a new fast buffered reader with a buffer of {@link #DEFAULT_BUFFER_SIZE} characters and given set of additional word constituents. 
	 * The wrapped reader will have to be set later using {@link #setReader(Reader)}. 
	 * @param wordConstituents a set of characters that will be considered word constituents.
	 */
	public FastBufferedReader( final CharSet wordConstituents ) {
		this( DEFAULT_BUFFER_SIZE, wordConstituents );
	}

	/** Creates a new fast buffered reader with a buffer of {@link #DEFAULT_BUFFER_SIZE} characters and a set of additional word constituents specified by a string.
	 * @param wordConstituents a string of characters that will be considered word constituents.
	 */
	public FastBufferedReader( final String wordConstituents ) {
		this( new CharOpenHashSet( wordConstituents.toCharArray(), Hash.VERY_FAST_LOAD_FACTOR ) );
	}

	/** Creates a new fast buffered reader with a given buffer size and a set of additional word constituents, both specified by strings.
	 * @param bufferSize the size in bytes of the internal buffer.
	 * @param wordConstituents a string of characters that will be considered word constituents.
	 */
	public FastBufferedReader( final String bufferSize, final String wordConstituents ) {
		this( Integer.parseInt( bufferSize ), new CharOpenHashSet( wordConstituents.toCharArray(), Hash.VERY_FAST_LOAD_FACTOR ) );
	}

	/** Creates a new fast buffered reader by wrapping a given reader with a given buffer size. 
	 *
	 * @param r a reader to wrap.
	 * @param bufferSize the size in bytes of the internal buffer.
	 */

	public FastBufferedReader( final Reader r, final int bufferSize ) {
		this( bufferSize );
		this.reader = r;
	}

	/** Creates a new fast buffered reader by wrapping a given reader with a given buffer size and using a set of additional word constituents. 
	 *
	 * @param r a reader to wrap.
	 * @param bufferSize the size in bytes of the internal buffer.
	 * @param wordConstituents a set of characters that will be considered word constituents.
	 */

	public FastBufferedReader( final Reader r, final int bufferSize, final CharSet wordConstituents ) {
		this( bufferSize, wordConstituents );
		this.reader = r;
	}

	/** Creates a new fast buffered reader by wrapping a given reader with a buffer of {@link #DEFAULT_BUFFER_SIZE} characters. 
	 *
	 * @param r a reader to wrap.
	 */
	public FastBufferedReader( final Reader r ) {
		this( r, DEFAULT_BUFFER_SIZE );
	}

	/** Creates a new fast buffered reader by wrapping a given reader with a buffer of {@link #DEFAULT_BUFFER_SIZE} characters and using a set of additional word constituents.
	 *
	 * @param r a reader to wrap.
	 * @param wordConstituents a set of characters that will be considered word constituents.
	 */
	public FastBufferedReader( final Reader r, final CharSet wordConstituents ) {
		this( r, DEFAULT_BUFFER_SIZE, wordConstituents );
	}

	/** Creates a new fast buffered reader by wrapping a given fragment of a character array and using a set of additional word constituents.
	 * <p>The effect of {@link #setReader(Reader)} on a buffer created with
	 * this constructor is undefined.
	 *
	 * @param array the array that will be wrapped by the reader.
	 * @param offset the first character to be used.
	 * @param length the number of character to be used.
	 * @param wordConstituents a set of characters that will be considered word constituents.
	 */
	public FastBufferedReader( final char[] array, final int offset, final int length, final CharSet wordConstituents ) {
		CharArrays.ensureOffsetLength( array, offset, length );
		buffer = array;
		pos = offset;
		avail = length;
		bufferSize = array.length;
		reader = NullReader.getInstance();
		this.wordConstituents = wordConstituents;
	}

	/** Creates a new fast buffered reader by wrapping a given fragment of a character array.
	 * <p>The effect of {@link #setReader(Reader)} on a buffer created with
	 * this constructor is undefined.
	 *
	 * @param array the array that will be wrapped by the reader.
	 * @param offset the first character to be used.
	 * @param length the number of character to be used.
	 */
	public FastBufferedReader( final char[] array, final int offset, final int length ) {
		this( array, offset, length, CharSets.EMPTY_SET );
	}


	/** Creates a new fast buffered reader by wrapping a given character array and using a set of additional word constituents.
	 * 
	 * <p>The effect of {@link #setReader(Reader)} on a buffer created with
	 * this constructor is undefined.
	 * @param array the array that will be wrapped by the reader.
	 * @param wordConstituents a set of characters that will be considered word constituents.
	 */
	public FastBufferedReader( final char[] array, final CharSet wordConstituents ) {
		this( array, 0, array.length, wordConstituents );
	}
		
	/** Creates a new fast buffered reader by wrapping a given character array.
	 * 
	 * <p>The effect of {@link #setReader(Reader)} on a buffer created with
	 * this constructor is undefined.
	 * @param array the array that will be wrapped by the reader.
	 */
	public FastBufferedReader( final char[] array ) {
		this( array, 0, array.length );
	}
		
	/** Creates a new fast buffered reader by wrapping a given mutable string and using a set of additional word constituents.
	 * 
	 * <p>The effect of {@link #setReader(Reader)} on a buffer created with
	 * this constructor is undefined.
	 *
	 * @param s the mutable string that will be wrapped by the reader.
	 * @param wordConstituents a set of characters that will be considered word constituents.
	 */
	public FastBufferedReader( final MutableString s, final CharSet wordConstituents ) {
		this( s.array(), 0, s.length(), wordConstituents );
	}

	/** Creates a new fast buffered reader by wrapping a given mutable string.
	 * <p>The effect of {@link #setReader(Reader)} on a buffer created with
	 * this constructor is undefined.
	 *
	 * @param s the mutable string that will be wrapped by the reader.
	 */
	public FastBufferedReader( final MutableString s ) {
		this( s.array(), 0, s.length() );
	}

	public FastBufferedReader copy() {
		return new FastBufferedReader( bufferSize, wordConstituents );
	}
	
	/** Checks whether no more characters will be returned.
	 * 
	 * @return true if there are no characters in the internal buffer and
	 * the underlying reader is exhausted.
	 */
	
	protected boolean noMoreCharacters() throws IOException {
		if ( avail == 0 ) {
			avail = reader.read( buffer );
			if ( avail <= 0 ) {
				avail = 0;
				return true;
			}
			pos = 0;
		}
		return false;
	}
	
	public int read() throws IOException {
		if ( noMoreCharacters() ) return -1;
		avail--;
		return buffer[ pos++ ];
	}


	public int read( final char[] b, int offset, int length ) throws IOException {
		CharArrays.ensureOffsetLength( b, offset, length );
		
		if ( length <= avail ) {
			System.arraycopy( buffer, pos, b, offset, length );
			pos += length;
			avail -= length;
			return length;
		}
	
		final int head = avail;
		System.arraycopy( buffer, pos, b, offset, head );
		offset += head;
		length -= head;
		avail = 0;

		int result;

		result = reader.read( b, offset, length ); 
		return result < 0 
			? ( head != 0 ? head : -1 ) 
			: result + head;
	}

	/** Reads a line into the given mutable string.
	 *
	 * <P>The next line of input (defined as in {@link java.io.BufferedReader#readLine()})
	 * will be stored into <code>s</code>. Note that if <code>s</code> is 
	 * not {@linkplain it.unimi.dsi.lang.MutableString loose}
	 * this method will be quite inefficient.
	 *
	 * @param s a mutable string that will be used to store the next line (which could be empty).
	 * @return <code>s</code>, or <code>null</code> if the end of file was found, in which
	 * case <code>s</code> is unchanged.
	 */

	public MutableString readLine( final MutableString s ) throws IOException {
		char c = 0;
		int i;

		if ( noMoreCharacters() ) return null;

		s.length( 0 );

		for(;;) {
			for( i = 0; i < avail && ( c = buffer[ pos + i ] ) != '\n' && c != '\r' ; i++ );

			s.append( buffer, pos, i  );
			pos += i; 
			avail -= i;

			if ( avail > 0 ) {
				if ( c == '\n' ) { // LF only.
					pos++;
					avail--;
				}
				else { // c == '\r'
					pos++;
					avail--;
					if ( avail > 0 ) {
						if ( buffer[ pos ] == '\n' ) { // CR/LF with LF already in the buffer.
							pos ++;
							avail--;
						}
					}
					else { // We must search for the LF.
						if ( noMoreCharacters() ) return s;
						if ( buffer[ 0 ] == '\n' ) {
							pos++;
							avail--;
						}
					}
				}
				return s;
			}
			else if ( noMoreCharacters() ) return s;
		}
	}
	
	/** Returns whether the given character is a word constituent.
	 * 
	 * <p>The behaviour of this {@link FastBufferedReader} as a {@link WordReader} can
	 * be radically changed by overwriting this method.
	 * 
	 * @param c a character.
	 * @return whether <code>c</code> should be considered a word constituent.
	 */
	
	protected boolean isWordConstituent( final char c ) {
		return Character.isLetterOrDigit( c ) || wordConstituents.contains( c );
	}

	public boolean next( final MutableString word, final MutableString nonWord ) throws IOException {
		int i;
		final char buffer[] = this.buffer;

		if ( noMoreCharacters() ) return false;

		word.length( 0 );
		nonWord.length( 0 );

		for(;;) {
			for( i = 0; i < avail && isWordConstituent( buffer[ pos + i ] ); i++ );

			word.append( buffer, pos, i  );
			pos += i; 
			avail -= i;
			
			if ( avail > 0 || noMoreCharacters() ) break;
		}
		
		if ( noMoreCharacters() ) return true;

		for(;;) {
			for( i = 0; i < avail && ! isWordConstituent( buffer[ pos + i ] ); i++ );

			nonWord.append( buffer, pos, i  );
			pos += i; 
			avail -= i;

			if ( avail > 0 || noMoreCharacters() ) return true;
		}
	}

	public FastBufferedReader setReader( final Reader reader ) {
		this.reader = reader;
		avail = 0;
		return this;
	}
 
	public long skip( long n ) throws IOException {
		if ( n <= avail ) {
			pos += ((int)n);
			avail -= ((int)n);
			return n;
		}

		final int head = avail;
		n -= head;
		avail = 0;

		return reader.skip( n ) + head;
	}


	public void close() throws IOException {
		if ( reader == null ) return;
		reader.close();
		reader = null;
		buffer = null;
	}

	private void readObject( final ObjectInputStream s ) throws IOException, ClassNotFoundException {
		s.defaultReadObject();
		buffer = new char[ bufferSize ];
	}
	
	public String toSpec() {
		return toString();
	}
	
	public String toString() {
		final String className = getClass().getName();
		if ( bufferSize == DEFAULT_BUFFER_SIZE && wordConstituents.isEmpty() ) return className;
		if ( wordConstituents.isEmpty() ) return className + "(" + bufferSize + ")";
		String wordConstituents = new String( this.wordConstituents.toCharArray() );
		if ( bufferSize == DEFAULT_BUFFER_SIZE ) return className + "(\"" + wordConstituents + "\")";
		return className + "(" + bufferSize + ",\"" + wordConstituents + "\")";
	}
}
