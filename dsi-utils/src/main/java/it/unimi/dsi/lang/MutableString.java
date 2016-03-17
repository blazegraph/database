package it.unimi.dsi.lang;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2002-2009 Paolo Boldi and Sebastiano Vigna 
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

import it.unimi.dsi.fastutil.chars.Char2CharMap;
import it.unimi.dsi.fastutil.chars.CharArrays;
import it.unimi.dsi.fastutil.chars.CharList;
import it.unimi.dsi.fastutil.chars.CharSet;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.util.TextPattern;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Serializable;
import java.io.UTFDataFormatException;
import java.io.Writer;

/** Fast, compact, optimised &amp; versatile mutable strings.
 *
 * <h3>Motivation</h3>
 *
 * <P>The classical Java string classes, {@link java.lang.String} and {@link
 * java.lang.StringBuffer}, lie at the extreme of a spectrum (immutable and
 * mutable).
 *
 * <P>However, large-scale text indexing requires some features that are not
 * provided by these classes: in particular, the possibility of using a mutable
 * string, once frozen, in the same optimised way of an immutable
 * string. Moreover, usually we do not need synchronisation (which makes
 * {@link java.lang.StringBuffer} slow).
 * 
 * <P>In a typical scenario you are dividing text into words (so you use a
 * <em>mutable</em> string to accumulate characters). Once you've got your
 * word, you would like to check whether this word is in a dictionary
 * <em>without creating a new object</em>. However, equality of
 * <code>StringBuffer</code>s is not defined on their content, and storing
 * words after a conversion to <code>String</code> will not help either, as
 * then you would need to convert the current mutable string into an immutable
 * one (thus creating a new object) <em>before deciding whether you need to
 * store it</em>.
 * 
 * <P>This class tries to make the best of both worlds, and thus aims at being
 * a Better Mousetrap&trade;. 
 *
 * <P>You can read more details about the design of <code>MutableString</code>
 * in Paolo Boldi and Sebastiano Vigna, 
 * &ldquo;<a href="http://vigna.dsi.unimi.it/papers.php#BoVMSJ">Mutable strings
 * in Java: Design, implementation and lightweight text-search
 * algorithms</a>&rdquo;, <i>Sci. Comput. Programming</i>, 54(1):3-23, 2005.
 *
 * <h3>Features</h3>
 *
 * Mutable strings come in two flavours: <em>compact</em> and
 * <em>loose</em>. A mutable string created by the empty constructor or
 * the constructor specifying a capacity is loose. All other
 * constructors create compact mutable strings. In most cases, you can completely
 * forget whether your mutable strings are loose or compact and get
 * good performance.
 *
 * <P><ul> 
 *
 * <li>Mutable strings occupy little space&mdash; their only attributes are a
 * backing character array and an integer (they are smaller of both
 * <code>String</code>s and <code>StringBuffer</code>s);
 *
 * <li>they are not synchronised; 
 *
 * <li>their methods try to be as efficient as possible:for instance, if some
 * limitation on a parameter is implied by limitation on array access, we do
 * not check it explicitly, and Bloom filters are used to speed up {@link
 * #replace(char[],String[]) multi-character substitutions};
 *
 * <li>they let you access directly the backing array
 * (at your own risk);
 *
 * <li>they implement {@link CharSequence}, so, for instance, you can match or split a
 * mutable string against a regular expression using the {@linkplain java.util.regex.Pattern standard Java API};
 *
 * <li>they implement {@link Appendable}, so they can be used with {@link java.util.Formatter}
 * and similar classes;
 *
 * <li><code>null</code>is not accepted as a string argument; 
 *
 * <li>compact mutable strings have a slow growth; loose mutable strings have a fast growth;
 *
 * <li>hash codes of compact mutable strings are cached (for faster
 * equality checks);
 *
 * <li>typical conversions such as trimming, upper/lower casing and
 * replacements are made in place, with minimal reallocations;
 *
 * <li>all methods try, whenever it is possible, to return <code>this</code>, so
 * you can chain methods as in <code>s.length(0).append("foo").append("bar")</code>;
 *
 * <li>you can write or print a mutable string without creating a
 * <code>String</code> by using {@link #write(Writer)}, {@link
 * #print(PrintWriter)} and {@link #println(PrintWriter)}; you can read it back
 * using {@link #read(Reader,int)}.
 *
 * <li>you can write <em>any</em> mutable string in (length-prefixed) UTF-8
 * format by using {@link #writeSelfDelimUTF8(DataOutput)}&mdash;you are not
 * limited to strings whose UTF-8 encoded length fits 16 bits;
 *
 * <li>you can {@link #wrap(char[]) wrap} any character array into a mutable string;
 *
 * <li>this class is not final: thus, you can add your own methods to
 * specialised versions.
 *
 * </ul>
 *
 * <P>Committing to use this class for such an ubiquitous data structure as
 * strings may seem dangerous, as standard string classes are by now tested and
 * stable. However, this class has been heavily regression (and torture) tested
 * on all methods, and we believe it is very reliable.
 *
 * <P>To simplify the transition to mutable strings, we have tried to make
 * mixing string classes simpler by providing polymorphic versions of all
 * methods accepting one or more strings&mdash;whenever you must specify a
 * string you can usually provide a <code>MutableString</code>, a
 * <code>String</code>, or a generic <code>CharSequence</code>.
 *
 * <P>Note that usually we provide a specific method for
 * <code>String</code>. This duplication may seem useless, as
 * <code>String</code> implements <code>CharSequence</code>. However, invoking
 * methods on an interface is slower than invoking methods on a class, and we
 * expect constant strings to appear often in such methods.
 *
 * <h3>The Reallocation Heuristic</h3>
 *
 * <P>Backing array reallocations use a heuristic based on looseness.  Whenever
 * an operation changes the length, compact strings are resized to fit
 * <em>exactly</em> the new content, whereas the capacity of a loose string is
 * never shortened, and enlargements maximise the new length required with the
 * double of the current capacity.
 *
 * <P>The effect of this policy is that loose strings will get large buffers
 * quickly, but compact strings will occupy little space and perform very well
 * in data structures using hash codes.
 *
 * <P>For instance, you can easily reuse a loose mutable string calling {@link
 * #length(int) length(0)} (which does <em>not</em> reallocate the backing
 * array).
 *
 * <P>In any case, you can call {@link #compact()} and {@link #loose()} to force 
 * the respective condition.
 *
 * <h3>Disadvantages</h3>
 *
 * <P>The main disadvantage of mutable strings is that their substrings
 * cannot share their backing arrays, so if you need to generate many
 * substrings you may want to use <code>String</code>. However, {@link
 * #subSequence(int,int) subSequence()} returns a {@link CharSequence} that
 * shares the backing array.
 *
 * <h3>Warnings</h3>
 *
 * There are a few differences with standard string classes you should be aware
 * of.
 *
 * <ol>
 *
 * <li><STRONG>This class is not synchronised</STRONG>.  If multiple threads
 * access an object of this class concurrently, and at least one of the threads
 * modifies it, it must be synchronised externally.
 *
 * <li>This class implements polymorphic versions of the {@link #equals(Object)
 * equals} method that compare the <em>content</em> of <code>String</code>s and
 * <code>CharSequence</code>s, so that you can easily do checks like
 *
 * <PRE>
 *         mutableString.equals( "Hello" ) 
 * </PRE> 
 *
 * Thus, you must <em>not</em> mix mutable strings with
 * <code>CharSequence</code>s in collections as equality between objects of
 * those types is not symmetric.
 * 
 * <li>When the length of a string or char array argument is zero,
 * some methods may just do nothing even if other parameters are out of
 * bounds.
 *
 * <li>The output of {@link #writeSelfDelimUTF8(DataOutput) writeSelfDelimUTF8()} is
 * <em>not</em> compatible with the usual Java {@link
 * DataOutput#writeUTF(String) writeUTF()}.
 *
 * <li>Even if this class is not final, most <em>methods</em> are declared
 * final for efficiency, so you cannot override them (why should you ever want
 * to override {@link #array()}?).
 *
 * </ol>
 *
 * @author Sebastiano Vigna
 * @author Paolo Boldi
 * @since 0.3
 */

public class MutableString implements Serializable, CharSequence, Appendable, Comparable<MutableString>, Cloneable {
	/** A mutable string containing <samp>null</samp>, used for implementing {@link Appendable}'s semantics. */
	private final static MutableString NULL = new MutableString( "null" );
	
	/** The backing array. */
	protected transient char[] array;

	/** This mutable string is compact iff this attribute is negative.
	 * It the string is compact, the attribute is its hash code (-1 denotes the invalid
	 * hash code). If the string is loose, the attribute is the number of 
	 * characters actually stored in the backing array. */
	protected transient int hashLength;

	public static final long serialVersionUID = -518929984008928417L;
    
	/** Creates a new loose empty mutable string with capacity 2. */
	public MutableString() {
		this( 2 );
	}

	/** Creates a new loose empty mutable string with given capacity.
	 *
	 * @param capacity the required capacity.
	 */
	public MutableString( final int capacity ) {
		array = capacity != 0 ? new char[ capacity ] : CharArrays.EMPTY_ARRAY;
	}

	/** Creates a new compact mutable string with given length.
	 *
	 * @param length the desired length of the new string.
	 */

	private void makeCompactMutableString( final int length ) {
		array = length != 0 ? new char[ length ] : CharArrays.EMPTY_ARRAY;
		hashLength = -1;
	}


	/** Creates a new compact mutable string copying a given mutable string.
	 *
	 * @param s the initial contents of the string.
	 */
	public MutableString( final MutableString s ) {
		makeCompactMutableString( s.length() );
		System.arraycopy( s.array, 0, array, 0, array.length );
    }

    /** Creates a new compact mutable string copying a given <code>String</code>.
     *
     * @param s the initial contents of the string.
     */
    public MutableString( final String s ) {
		makeCompactMutableString( s.length() );
		s.getChars( 0, array.length, array, 0 );
    }

    /** Creates a new compact mutable string copying a given <code>CharSequence</code>.
     *
     * @param s the initial contents of the string.
     */
    public MutableString( final CharSequence s ) {
		makeCompactMutableString( s.length() );
		getChars( s, 0, array.length, array, 0 );
    }

    /** Creates a new compact mutable string copying a given character array.
     *
     * @param a the initial contents of the string.
     */
    public MutableString( final char[] a ) {
		makeCompactMutableString( a.length );
		System.arraycopy( a, 0, array, 0, array.length );
    }

    /** Creates a new compact mutable string copying a part of a given character array.
     *
     * @param a a character array.
     * @param offset an offset into the array.
     * @param len how many characters to copy.
     */
    public MutableString( final char[] a, final int offset, final int len ) {
		makeCompactMutableString( len );
		System.arraycopy( a, offset, array, 0, len );
    }

    /** Creates a new compact mutable string by copying this one.
	 *
     * @return a compact copy of this mutable string.
     */
    public MutableString copy() {
		return new MutableString( this );
    }

    /** Creates a new compact mutable string by copying this one.
	 *
	 * <P>This method is identical to {@link #copy}, but the latter returns
	 * a more specific type.
	 *
     * @return a compact copy of this mutable string.
     */
    public Object clone() {
		return new MutableString( this );
    }


    /** Commodity static method implementing {@link
     *  java.lang.String#getChars(int,int,char[],int)} for a <code>CharSequence</code>s. 
     *
     * @param s a <code>CharSequence</code>.
     * @param start copy start from this index (inclusive).
     * @param end copy ends at this index (exclusive).
     * @param dest destination array.
     * @param destStart the first character will be copied in <code>dest[destStart]</code>.
     * @see java.lang.String#getChars(int,int,char[],int)
     */
	public static void getChars( final CharSequence s, final int start, final int end, final char[] dest, final int destStart ) {
		int j = destStart, i = start;
		while( i < end ) dest[ j++ ] = s.charAt( i++ );
    }


    /** Returns the number of characters in this mutable string.
     *
     * @return  the length of this mutable string.
     */
    final public int length() {
		return hashLength >= 0 ? hashLength : array.length;
    }

    /** Returns the current length of the backing array.
     *
     * @return  the current length of the backing array.
     */
    final public int capacity() {
		return array.length;
    }

    /** Gets the backing array.
     *
     * <P>For fast, repeated access to the characters of this mutable string,
     * you can obtain the actual backing array. Be careful, and if this mutable
     * string is compact do <em>not</em> modify its backing array without
     * calling {@link #changed()} immediately afterwards (or the cached hash
     * code will get out of sync, with unforeseeable consequences).
     * 
     * @see #changed()
	 * @return the backing array.
     */
    final public char[] array() { 
		return array; 
    }


    /** Characters with indices from <code>start</code> (inclusive) to
     * index <code>end</code> (exclusive) are copied from this 
     * mutable string into the array <code>dest</code>, starting
     * from index <code>destStart</code>.
     *
     * @param start copy start from this index (inclusive).
     * @param end copy ends at this index (exclusive).
     * @param dest destination array.
     * @param destStart the first character will be copied in <code>dest[destStart]</code>.
     *
	 * @see String#getChars(int,int,char[],int)
     * @throws NullPointerException if <code>dest</code> is <code>null</code>.
     * @throws IndexOutOfBoundsException  if any of the following is true:
     * <ul>
     * <li><code>start</code> or <code>end</code> is negative
     * <li><code>start</code> is greater than 
     * <code>end</code>
     * <li><code>end</code> is greater than 
     * {@link #length()}
     * <li><code>end-start+destStart</code> is greater than 
     * <code>dest.length</code>
     * </ul>
     */
    final public void getChars( final int start, final int end, final char[] dest, final int destStart ) {
		if ( end > length() ) throw new IndexOutOfBoundsException();
		System.arraycopy( array, start, dest, destStart, end - start );
    }

    /** Ensures that at least the given number of characters can be stored in this mutable string.
     *
     * <P>The new capacity of this string will be <em>exactly</em> equal to the
     * provided argument if this mutable string is compact (this differs
     * markedly from {@link java.lang.StringBuffer#ensureCapacity(int)
     * StringBuffer}). If this mutable string is loose, the provided argument
     * is maximised with the current capacity doubled.
     *
     * <P>Note that if the given argument is greater than the current length, you will make
     * this string loose (see the {@linkplain MutableString class description}).
     *
     * @param minimumCapacity we want at least this number of characters, but no more.
	 * @return this mutable string.
     */
    final public MutableString ensureCapacity( final int minimumCapacity ) {
		final int length = length();
		expand( minimumCapacity );
		if ( length < minimumCapacity ) hashLength = length;
		return this;
    }

    /** Ensures that at least the given number of characters can be stored in this string.
     *
     * <P>If necessary, enlarges the backing array. If the string is compact,
     * we expand it exactly to the given capacity; otherwise, expand to the
     * maximum between the given capacity and the double of the current capacity.
     *
     * <P>This method works even with a <code>null</code> backing array (which
     * will be considered of length 0).
     *
     * <P>After a call to this method, we may be in an inconsistent state: if
     * you expand a compact string, {@link #hashLength} will be negative,
     * but there will be spurious characters in the string. Be sure to
     * fill them suitably.
     *
     * @param minimumCapacity we want at least this number of characters.
     */
    private void expand( final int minimumCapacity ) {
		final int c = array == null ? 0 : array.length; // This can happen only deserialising.
		if ( minimumCapacity <= c && array != null ) return;
		final int length = hashLength >= 0 ? hashLength : c;
		final char[] newArray = 
			new char[ 
					 hashLength >= 0 && c * 2 > minimumCapacity
					 ? c * 2 // loose
					 : minimumCapacity // compact
			];
		if ( length != 0 ) System.arraycopy( array, 0, newArray, 0, length ); // We check because array could be null during deserialisation.
		array = newArray;
    }

    /** Ensures that <em>exactly</em> the given number of characters can be stored in this string.
     *
     * <P>If necessary, reallocates the backing array. If the new capacity is smaller than
	 * the string length, the string will be truncated.
     *
     * <P>After a call to this method, we may be in an inconsistent state: if
     * you expand a compact string, {@link #hashLength} will be negative,
     * but there will be additional NUL characters in the string. Be sure to
     * substitute them suitably.
     *
     * @param capacity we want exactly this number of characters.
     */
    private void setCapacity( int capacity ) {
		final int c = array.length;
		if ( capacity == c ) return;
		final int length = hashLength >= 0 ? hashLength : c;
		final char[] newArray = capacity != 0 ? new char[ capacity ] : CharArrays.EMPTY_ARRAY;
		System.arraycopy( array, 0, newArray, 0, length < capacity ? length : capacity );
		array = newArray;
    }



    /** Sets the length.
     *
     * <P>If the provided length is greater than that of the current string,
     * the string is padded with zeros. If it is shorter, the string is
     * truncated to the given length. We do <em>not</em> reallocate the backing
     * array, to increase object reuse.  Use rather {@link #compact()} for that
     * purpose.
     *
     * <P>Note that shortening a string will make it loose (see the {@linkplain
     * MutableString class description}).
     *
     * @param newLength the new length for this mutable string.
	 * @return this mutable string.
     * @see #compact()
     */
    final public MutableString length( final int newLength ) {
		if ( newLength < 0 ) throw new IllegalArgumentException( "Negative length (" + newLength + ")" );
		if ( hashLength < 0 ) {
			if ( array.length == newLength ) return this;
			hashLength = -1;
			setCapacity( newLength ); // For compact strings, length and capacity coincide.
		}
		else {
			final int length = hashLength;
			if ( newLength == length ) return this;
			if ( newLength > array.length ) expand( newLength ); // In this case, the array is already filled with zeroes.
			else if ( newLength > length ) java.util.Arrays.fill( array, length, newLength, '\0' );
			hashLength = newLength;
		}
		return this;
	}

    /** A nickname for {@link #length(int)}.
     *
     * @param newLength the new length for this mutable string.
	 * @return this mutable string.
     * @see #length(int)
     */
    final public MutableString setLength( final int newLength ) {
		return length( newLength );
    }

    /** Makes this mutable string compact (see the {@linkplain MutableString class description}).
     *
     * <P>Note that this operation may require reallocating
     * the backing array (of course, with a shorter length).
     *
     * @return this mutable string.
     * @see #isCompact()
     */
    final public MutableString compact() {
		if ( hashLength >= 0 ) {
			setCapacity( hashLength );
			hashLength = -1;
		}
		return this;
    }

    /** Makes this mutable string loose.
     *
     * @return this mutable string.
     * @see #isLoose()
     */
    final public MutableString loose() {
		if ( hashLength < 0 ) hashLength = array.length;
		return this;
    }

    /** Returns whether this mutable string is compact (see the {@linkplain MutableString class description}).
     *
     * @return whether this mutable string is compact.
     * @see #compact()
     */
    final public boolean isCompact() {
		return hashLength < 0;
    }

    /** Returns whether this mutable string is loose (see the {@linkplain MutableString class description}).
     *
     * @return whether this mutable string is loose.
     * @see #loose()
     */
    final public boolean isLoose() {
		return hashLength >= 0;
    }

    /** Invalidates the current cached hash code if this mutable string is compact.
     *
     * <P>You will need to call this method only if you change the backing
     * array of a compact mutable string {@linkplain #array() directly}.
     *
	 * @return this mutable string.
     */
    final public MutableString changed() { 
		if ( hashLength < 0 ) hashLength = -1; 
		return this;
    }

    /** Wraps a given character array in a compact mutable string.
     * 
     * <P>The returned mutable string will be compact and backed by the given character array.
     * 
     * @param a a character array.
     * @return a compact mutable string backed by the given array.
     */

    static public MutableString wrap( final char a[] ) {
		MutableString s = new MutableString( 0 );
		s.array = a;
		s.hashLength = -1;
		return s;
    }

    /** Wraps a given character array for a given length in a loose mutable string.
     * 
     * <P>The returned mutable string will be loose and backed by the given character array.
     * 
     * @param a a character array.
     * @param length a length.
     * @return a loose mutable string backed by the given array with the given length.
     */

    static public MutableString wrap( final char[] a, final int length ) {
		MutableString s = new MutableString( 0 );
		s.array = a;
		s.hashLength = length;
		return s;
    }


    /** Gets a character.
     *
     * <P>If you end up calling repeatedly this method, you
     * should consider using {@link #array()} instead.
     *
     * @param index the index of a character.
     * @return the chracter at that index.
     */
    final public char charAt( final int index ) {
		if ( index >= length() ) throw new StringIndexOutOfBoundsException( index );
		return array[ index ];
    }

    /** A nickname for {@link #charAt(int,char)}.
     *
     * @param index the index of a character.
     * @param c the new character.
	 * @return this mutable string.
     * @see #charAt(int,char)
     */
    final public MutableString setCharAt( final int index, final char c ) {
		charAt( index, c );
		return this;
    }

    /** Sets the character at the given index.
     *
     * <P>If you end up calling repeatedly this method, you should consider
     * using {@link #array()} instead.
     *
     * @param index the index of a character.
     * @param c the new character.
	 * @return this mutable string.
     */
    final public MutableString charAt( final int index, final char c ) {
		if ( index >= length() ) throw new StringIndexOutOfBoundsException( index );
		array[ index ] = c;
		changed();
		return this;
    }

    /** Returns the first character of this mutable string.
     *
	 * @return the first character.
     * @throws StringIndexOutOfBoundsException when called on the empty string.
     */
    final public char firstChar() {
		if ( length() == 0 ) throw new StringIndexOutOfBoundsException( 0 );
		return array[ 0 ];
    }

    /** Returns the last character of this mutable string.
	 *
     * @return the last character.
     * @throws ArrayIndexOutOfBoundsException when called on the empty string.
     */
    final public char lastChar() {
		return array[ length() - 1 ];
    }


    /** Converts this string to a new character array.
     *
     * @return a newly allocated character array with the same length and content of this mutable string.
     */
    final public char[] toCharArray() {
    	return CharArrays.copy( array, 0, length() );
    }

    
    /** Returns a substring of this mutable string.
     *
     * <P>The creation of a substring implies the creation of a new backing
     * array. The returned mutable string will be compact.
     * 
     * @param start first character of the substring (inclusive).
     * @param end last character of the substring (exclusive).
     * @return a substring defined as above.
     */
    final public MutableString substring( final int start, final int end ) {
		if ( end > length() ) throw new StringIndexOutOfBoundsException( end );
        return new MutableString( array, start, end - start );
    }

    /** Returns a substring of this mutable string.
     *
     * @param start first character of the substring (inclusive).
     * @return a substring ranging from the given position to the end of this string.
     * @see #substring(int,int)
     */
    final public MutableString substring( final int start ) { return substring( start, length() ); }

    /** A class representing a subsequence.
     *
     * <P>Subsequences represented by this class share the backing array. Equality
     * is content-based; hash codes are identical to those of a mutable string with the same content.
     */
    private class SubSequence implements CharSequence {
		final int from, to;
		
		private SubSequence( final int from, final int to ) {
			this.from = from;
			this.to = to;
		}
	
		public char charAt( int index ) { return array[ from + index ]; }
		public int length() { return to - from; }
		public CharSequence subSequence( final int start, final int end ) { 
			if ( start < 0 ) throw new StringIndexOutOfBoundsException( start );
			if ( end < start || end > length() ) throw new StringIndexOutOfBoundsException( end );
			return new SubSequence( this.from + start, this.from + end ); 
		}

		/** For convenience, the hash code of a subsequence is equal
		 * to that of a <code>String</code> with the same content with the
		 * 31st bit set.
		 *
		 * @return the hash code.
		 */
		public int hashCode() {
			int h = 0;
			final char[] a = array;
			for ( int i = from; i < to; i++ ) h = 31 * h + a[ i ];
			return h | ( 1 << 31 );
		}
	
		public boolean equals( Object o ) {
			if ( o instanceof CharSequence ) {
				CharSequence s = (CharSequence)o;
				int n = length();
				if ( n == s.length() ) {
					while( n-- != 0 ) if ( charAt( n ) != s.charAt( n ) ) return false;
					return true;
				}
			}
			return false;
		}

		public String toString() { return new String( array, from, to - from ); }
    }

    
    /** Returns a subsequence of this mutable string.
     *
     * <P>Subsequences <em>share the backing array</em>. Thus, you should
     * <em>not</em> use a subsequence after changing the characters of this
     * mutable string between <code>start</code> and <code>end</code>.
     *
     * <P>Equality of <code>CharSequence</code>s returned by this method is defined by
     * content equality, and hash codes are identical to mutable strings with the same
     * content. Thus, you <em>can</em> mix mutable strings and <code>CharSequence</code>s
     * returned by this method in data structures, as the contracts of {@link java.lang.Object#equals(Object) equals()} and
     * {@link java.lang.Object#hashCode() hashCode()} are honoured.
     * 
     * @param start first character of the subsequence (inclusive).
     * @param end last character of the subsequence (exclusive).
     * @return a subsequence defined as above.
     * @see #substring(int,int)
     */
    final public CharSequence subSequence( final int start, final int end ) { 
		if ( start < 0 ) throw new StringIndexOutOfBoundsException();
		if ( start > end || end > length() ) throw new StringIndexOutOfBoundsException();
		return new SubSequence( start, end );
    }

    /** Appends the given mutable string to this mutable string.
     *
     * @param s the mutable string to append.
     * @return  this mutable string.
     */
    final public MutableString append( MutableString s ) {
    	if ( s == null ) s = NULL;
    	final int l = s.length();
		if ( l == 0 ) return this;

		final int newLength = length() + l;
		expand( newLength );
		System.arraycopy( s.array, 0, array, newLength - l, l ); 
		hashLength = hashLength < 0 ? -1 : newLength;
		return this;
    }

    /**
	 * Appends the given <code>String</code> to this mutable string.
	 * 
	 * @param s a <code>String</code> (<code>null</code> is not allowed).
	 * @return this mutable string.
	 * @throws NullPointerException if the argument is <code>null</code>
	 */
	final public MutableString append( final String s ) {
		if ( s == null ) return append( NULL );
		final int l = s.length();
		if ( l == 0 ) return this;

		final int newLength = length() + l;
		expand( newLength );
		s.getChars( 0, l, array, newLength - l );
		hashLength = hashLength < 0 ? -1 : newLength;
		return this;
	}

    /**
	 * Appends the given <code>CharSequence</code> to this mutable string.
	 * 
	 * @param s a <code>CharSequence</code> or <code>null</code>.
	 * @return this mutable string.
	 * 
	 * @see Appendable#append(java.lang.CharSequence)
	 */
	final public MutableString append( final CharSequence s ) {
		if ( s == null ) return append( NULL );
		final int l = s.length();
		if ( l == 0 ) return this;

		final int newLength = length() + l;
		expand( newLength );
		getChars( s, 0, l, array, newLength - l );
		hashLength = hashLength < 0 ? -1 : newLength;
		return this;
	}

    /**
	 * Appends a subsequence of the given <code>CharSequence</code> to this mutable string.
	 * 
	 * <strong>Warning</strong>: the semantics of this method of that of
	 * {@link #append(char[], int, int)} are different.
	 * 
	 * @param s a <code>CharSequence</code> or <code>null</code>.
	 * @param start the index of the first character of the subsequence to append.
	 * @param end the index of the character after the last character in the subsequence.
	 * @return this mutable string.
	 * 
	 * @see Appendable#append(java.lang.CharSequence, int, int)
	 */
	final public MutableString append( final CharSequence s, final int start, final int end ) {
		if ( s == null ) return append( NULL, start, end );
		final int len = end - start;
		if ( len < 0 || start < 0 || end > s.length() )
			throw new IndexOutOfBoundsException( "start: " + start + " end: " + end + " length():" + s.length() );
		final int newLength = length() + len;
		expand( newLength );

		try {
			getChars( s, start, end, array, newLength - len );
		}
		catch ( IndexOutOfBoundsException e ) {
			if ( hashLength < 0 ) setCapacity( newLength - len );
			else hashLength = newLength - len;
			throw e;
		}
		if ( len != 0 ) hashLength = hashLength < 0 ? -1 : newLength;
		return this;
	}

	/**
	 * Appends the given character sequences to this mutable string using the given separator.
	 * 
	 * @param a an array.
	 * @param offset the index of the first character sequence to append.
	 * @param length the number of character sequences to append.
	 * @param separator a separator that will be appended inbetween the character sequences.
	 * @return this mutable string.
	 */
    // TODO: this needs tests
	final public MutableString append( final CharSequence[] a, final int offset, final int length, final CharSequence separator ) {
    	ObjectArrays.ensureOffsetLength( a, offset, length );
    	if ( length == 0 ) return this;
    	
    	// Precompute the length of the resulting string
    	int m = 0;
    	for( int i = 0; i < length; i++ ) m += a[ offset + i ].length();
    	final int separatorLength = separator.length();
    	m += ( length - 1 ) * separatorLength;

    	final int l = length();
    	ensureCapacity( l + m );
    	m = 0;
    	for( int i = 0; i < length; i++ ) {
    		if ( i != 0 ) {
    			getChars( separator, 0, separatorLength, array, l + m );
    			m += separatorLength;
    		}
    		getChars( a[ i ], 0, a[ i + offset ].length(), array, l + m );
    		m += a[ i ].length();
    	}
    	if ( hashLength < 0 ) hashLength = -1;
    	else hashLength = l + m;
    	return this;
	}

	/** Appends the given character sequences to this mutable string using the given separator.
	 *
	 * @param a an array.
	 * @param separator a separator that will be appended inbetween the character sequences.
	 * @return this mutable string.
	 */
	final public MutableString append( final CharSequence[] a, final CharSequence separator ) {
		return append( a, 0, a.length, separator );
	}
   	

	/** Appends the string representations of the given objects to this mutable string using the given separator.
	 * 
	 * @param a an array of objects.
	 * @param offset the index of the first object to append.
	 * @param length the number of objects to append.
	 * @param separator a separator that will be appended inbetween the string representations of the given objects.
	 * @return this mutable string.
	 */
	final public MutableString append( final Object[] a, final int offset, final int length, final CharSequence separator ) {
		String s[] = new String[ a.length ];
		for( int i = 0; i < length; i++ ) s[ i ] = a[ offset + i ].toString();
		return append( s, offset, length, separator );
	}
	
	/** Appends the string representations of the given objects to this mutable string using the given separator.
	 *
	 * @param a an array of objects.
	 * @param separator a separator that will be appended inbetween the string representations of the given objects.
	 * @return this mutable string.
	 */
	final public MutableString append( final Object[] a, final CharSequence separator ) {
		return append( a, 0, a.length, separator );
	}

    /** Appends the given character array to this mutable string.
     *
     * @param a an array to append.
     * @return this mutable string.
     */
	final public MutableString append( final char a[] ) {
		final int l = a.length;
		if ( l == 0 ) return this;

		final int newLength = length() + l;
		expand( newLength );
		System.arraycopy( a, 0, array, newLength - l, l );
		hashLength = hashLength < 0 ? -1 : newLength;
		return this;
	}
    
	/** Appends a part of the given character array to this mutable string.
	 *
	 * <strong>Warning</strong>: the semantics of this method of that of
	 * {@link #append(CharSequence, int, int)} are different.
	 * 
	 * @param a an array.
	 * @param offset the index of the first character to append.
	 * @param len the number of characters to append.
	 * @return this mutable string.
	 */
	final public MutableString append( final char[] a, final int offset, final int len ) {
		final int newLength = length() + len;
		expand( newLength );
		try {
			System.arraycopy( a, offset, array, newLength - len, len );
		}
		catch( IndexOutOfBoundsException e ) {
			if ( hashLength < 0 ) setCapacity( newLength - len );
			else hashLength = newLength - len;
			throw e;
		}
		if ( len != 0 ) hashLength = hashLength < 0 ? -1 : newLength;
		return this;
	}
    
	/** Appends the given character list to this mutable string.
	 *
	 * @param list the list to append.
	 * @return this mutable string.
	 */
	final public MutableString append( final CharList list ) {
		final int l = list.size();
		if ( l == 0 ) return this;

		final int newLength = length() + l;
		expand( newLength );
		list.getElements( 0, array, newLength - l, l );
		hashLength = hashLength < 0 ? -1 : newLength;
		return this;
	}
   
	/** Appends a part of the given character list to this mutable string.
	 *
	 * <strong>Warning</strong>: the semantics of this method of that of
	 * {@link #append(CharSequence, int, int)} are different.
	 * 
	 * @param list a character list.
	 * @param offset the index of the first character to append.
	 * @param len the number of characters to append.
	 * @return this mutable string.
	 */
	final public MutableString append( final CharList list, final int offset, final int len ) {
		final int newLength = length() + len;
		expand( newLength );
		try {
			list.getElements( offset, array, newLength - len, len );
		}
		catch( IndexOutOfBoundsException e ) {
			if ( hashLength < 0 ) setCapacity( newLength - len );
			else hashLength = newLength - len;
			throw e;
		}
		if ( len != 0 ) hashLength = hashLength < 0 ? -1 : newLength;
		return this;
	}
   
    /** Appends a boolean to this mutable string.
     *
     * @param b the boolean to be appended.
     * @return this mutable string.
     */
    final public MutableString append( boolean b ) { return append( String.valueOf( b ) ); }

    /** Appends a character to this mutable string.
     *
     * <P>Note that this method <em>will reallocate the backing array of a compact
     * mutable string for each character appended</em>.  Do not call it
     * lightly.
     *
     * @param c a character.
     * @return this mutable string.
     */
    final public MutableString append( char c ) {
		final int newLength = length() + 1;
		expand( newLength );
		array[ newLength - 1 ] = c;
		hashLength = hashLength < 0 ? -1 : newLength;
		return this;
    }

    /** Appends an integer to this mutable string.
     *
     * @param i the integer to be appended.
     * @return this mutable string.
     */
    final public MutableString append( final int i ) { return append( String.valueOf( i ) ); }

    /** Appends a long to this mutable string.
     *
     * @param l the long to be appended.
     * @return this mutable string.
     */
    final public MutableString append( final long l ) { return append( String.valueOf( l ) ); }

    /** Appends a float to this mutable string.
     *
     * @param f the float to be appended.
     * @return this mutable string.
     */
    final public MutableString append( final float f ) { return append( String.valueOf( f ) ); }

    /** Appends a double to this mutable string.
     *
     * @param d the double to be appended.
     * @return this mutable string.
     */
    final public MutableString append( final double d ) { return append( String.valueOf( d ) ); }

    /** Appends the string representation of an object to this mutable string.
     *
     * @param o the object to append.
     * @return  a reference to this mutable string.
     */
    final public MutableString append( final Object o ) {
		return append( String.valueOf( o ) );
    }


    /** Inserts a mutable string in this mutable string, starting from index <code>index</code>.
     *
     * @param index position at which to insert the <code>String</code>.
     * @param s the mutable string to be inserted.
     * @return this mutable string.
     * @throws NullPointerException if <code>s</code> is <code>null</code>
     * @throws IndexOutOfBoundsException if <code>index</code>
     * is negative or greater than {@link #length()}.
     */
    final public MutableString insert( final int index, final MutableString s ) {
		final int length = length();
        if  ( index > length ) throw new StringIndexOutOfBoundsException();
		final int l = s.length();
		if ( l == 0 ) return this;

		final int newLength = length + l;
		expand( newLength );
		System.arraycopy( array, index, array, index + l, length - index );
		System.arraycopy( s.array, 0, array, index, l );
		hashLength = hashLength < 0 ? -1 : newLength;
		return this;
    }

    /** Inserts a <code>String</code> in this mutable string, starting from index <code>index</code>.
     *
     * @param index position at which to insert the <code>String</code>.
     * @param s the <code>String</code> to be inserted.
     * @return this mutable string.
     * @throws NullPointerException if <code>s</code> is <code>null</code>
     * @throws IndexOutOfBoundsException if <code>index</code>
     * is negative or greater than {@link #length()}.
     */
    final public MutableString insert( final int index, final String s ) {
		final int length = length();
        if  ( index > length ) throw new StringIndexOutOfBoundsException();
		final int l = s.length();
		if ( l == 0 ) return this;

		final int newLength = length + l;
		expand( newLength );
		System.arraycopy( array, index, array, index + l, length - index );
		s.getChars( 0, l, array, index ); 
		hashLength = hashLength < 0 ? -1 : newLength;
		return this;
    }

    /** Inserts a <code>CharSequence</code> in this mutable string, starting from index <code>index</code>.
     *
     * @param index position at which to insert the <code>CharSequence</code>.
     * @param s the <code>CharSequence</code> to be inserted.
     * @return this mutable string.
     * @throws NullPointerException if <code>s</code> is <code>null</code>
     * @throws IndexOutOfBoundsException if <code>index</code>
     * is negative or greater than {@link #length()}.
     */
    final public MutableString insert( final int index, final CharSequence s ) {
		final int length = length();
        if  ( index > length ) throw new StringIndexOutOfBoundsException();
		final int l = s.length();
		if ( l == 0 ) return this;

		final int newLength = length + l;
		if ( newLength >= array.length ) expand( newLength );
		System.arraycopy( array, index, array, index + l, length - index );
		getChars( s, 0, l, array, index ); 
		hashLength = hashLength < 0 ? -1 : newLength;
		return this;
    }

    /** Inserts characters in this mutable string. All of the characters
     * of the array <code>c</code> are inserted in this mutable string,
     * and the first inserted character is going to have index <code>index</code>.
     *
     * @param index position at which to insert subarray.
     * @param c the character array.
     * @return this mutable string.
     * @throws NullPointerException if <code>c</code> is <code>null</code>
     * @throws IndexOutOfBoundsException  if <code>index</code>
     * is negative or greater than {@link #length()}.
     */
    final public MutableString insert( final int index, final char[] c ) {
		final int length = length();
        if  ( index > length ) throw new StringIndexOutOfBoundsException();
		final int l = c.length;
		if ( l == 0 ) return this;

		final int newLength = length + l;
		expand( newLength );
		System.arraycopy( array, index, array, index + l, length - index );
		System.arraycopy( c, 0, array, index, l );
		hashLength = hashLength < 0 ? -1 : newLength;
		return this;
    }

    /** Inserts characters in this mutable string. <code>len</code> characters
     * of the array <code>c</code>, with indices starting from
     * <code>offset</code>, are inserted in this mutable string,
     * and the first inserted character is going to have index <code>index</code>.
     *
     * @param index position at which to insert subarray.
     * @param c the character array.
     * @param offset the index of the first character of <code>c</code> to
     * to be inserted.
     * @param len the number of characters of <code>c</code> to
     * to be inserted.
     * @return this mutable string.
     * @throws NullPointerException if <code>c</code> is <code>null</code>
     * @throws IndexOutOfBoundsException  if <code>index</code>
     * is negative or greater than {@link #length()}, or
     * <code>offset</code> or <code>len</code> are negative, or
     * <code>offset+len</code> is greater than
     * <code>c.length</code>.
     */
    final public MutableString insert( final int index, final char[] c, final int offset, final int len ) {
		final int length = length();
        if ( index > length ) throw new StringIndexOutOfBoundsException();
		if ( offset < 0 || offset + len < 0 || offset + len > c.length ) throw new StringIndexOutOfBoundsException( offset );
		if ( len == 0 ) return this;

		final int newLength = length + len;
		expand( newLength );
		System.arraycopy( array, index, array, index + len, length - index );
		System.arraycopy( c, offset, array, index, len );
		hashLength = hashLength < 0 ? -1 : newLength;
		return this;
    }


    /** Inserts a boolean in this mutable string, starting from index <code>index</code>.
     *
     * @param index position at which to insert the <code>String</code>.
     * @param b the boolean to be inserted.
     * @return this mutable string.
     * @throws IndexOutOfBoundsException if <code>index</code>
     * is negative or greater than {@link #length()}.
     */
    final public MutableString insert( final int index, final boolean b ) { return insert( index, String.valueOf( b ) ); }

    /** Inserts a char in this mutable string, starting from index <code>index</code>.
     *
     * <P>Note that this method <em>will not expand the capacity of a compact
     * mutable string by more than one character</em>.  Do not call it
     * lightly.
     *
     * @param index position at which to insert the <code>String</code>.
     * @param c the char to be inserted.
     * @return this mutable string.
     * @throws IndexOutOfBoundsException if <code>index</code>
     * is negative or greater than {@link #length()}.
     */
    final public MutableString insert( final int index, final char c ) { return insert( index, String.valueOf( c ) ); }

    /** Inserts a double in this mutable string, starting from index <code>index</code>.
     *
     * @param index position at which to insert the <code>String</code>.
     * @param d the double to be inserted.
     * @return this mutable string.
     * @throws IndexOutOfBoundsException if <code>index</code>
     * is negative or greater than {@link #length()}.
     */
    final public MutableString insert( final int index, final double d ) { return insert( index, String.valueOf( d ) ); }

    /** Inserts a float in this mutable string, starting from index <code>index</code>.
     *
     * @param index position at which to insert the <code>String</code>.
     * @param f the float to be inserted.
     * @return this mutable string.
     * @throws IndexOutOfBoundsException if <code>index</code>
     * is negative or greater than {@link #length()}.
     */
    final public MutableString insert( final int index, final float f ) { return insert( index, String.valueOf( f ) ); }

    /** Inserts an int in this mutable string, starting from index <code>index</code>.
     *
     * @param index position at which to insert the <code>String</code>.
     * @param x the int to be inserted.
     * @return this mutable string.
     * @throws IndexOutOfBoundsException if <code>index</code>
     * is negative or greater than {@link #length()}.
     */
    final public MutableString insert( final int index, final int x ) { return insert( index, String.valueOf( x ) ); }

    /** Inserts a long in this mutable string, starting from index <code>index</code>.
     *
     * @param index position at which to insert the <code>String</code>.
     * @param l the long to be inserted.
     * @return this mutable string.
     * @throws IndexOutOfBoundsException if <code>index</code>
     * is negative or greater than {@link #length()}.
     */
    final public MutableString insert( final int index, final long l ) { return insert( index, String.valueOf( l ) ); }

    /** Inserts the string representation of an object in this mutable string, starting from index <code>index</code>.
     *
     * @param index position at which to insert the <code>String</code>.
     * @param o the object to be inserted.
     * @return this mutable string.
     * @throws IndexOutOfBoundsException if <code>index</code>
     * is negative or greater than {@link #length()}.
     */
    final public MutableString insert( final int index, final Object o ) { return insert( index, String.valueOf( o ) ); }



	/** Removes the characters of this mutable string with 
     * indices in the range from <code>start</code> (inclusive) to <code>end</code> 
     * (exclusive). If <code>end</code> is greater than or equal to the length
     * of this mutable string, all characters with indices greater
     * than or equal to <code>start</code> are deleted.
     *
     * @param start The beginning index (inclusive).
     * @param end The ending index (exclusive).
     * @return this mutable string.
     * @throws IndexOutOfBoundsException  if <code>start</code>
     * is greater than {@link #length()}, or greater than <code>end</code>.
     */
    final public MutableString delete( final int start, int end ) {
		final int length = length();
		if ( end > length ) end = length;
		if ( start > end ) throw new StringIndexOutOfBoundsException();

        final int l = end - start;
        if ( l > 0 ) {
            System.arraycopy( array, start + l, array, start, length - end );
			if ( hashLength < 0 ) {
				setCapacity( length - l );
				hashLength = -1;
			}
			else hashLength -= l;
        }
        return this;
    }

    /** Removes the character at the given index.
     *
     * <P>Note that this method <em>will reallocate the backing array of a compact
     * mutable string for each character deleted</em>.  Do not call it
     * lightly.
     *
     * @param index Index of character to remove
     * @return this mutable string.
     * @throws IndexOutOfBoundsException if <code>index</code>
     * is negative, or greater than or equal to {@link #length()}.
     */
    final public MutableString deleteCharAt( final int index ) {
		final int length = length();
        if ( index >= length ) throw new StringIndexOutOfBoundsException();
		System.arraycopy( array, index + 1, array, index, length - index - 1 );

		if ( hashLength < 0 ) {
			setCapacity( length - 1 );
			hashLength = -1;
		}
		else hashLength--;

        return this;
    }

    /** Removes all occurrences of the given character.
    *
    * @param c the character to remove.
    * @return this mutable string.
    */
    final public MutableString delete( final char c ) {
		final int length = length();
    	final char[] a = array;

    	int l = 0;
		for( int i = 0; i < length; i++ ) if ( a[ i ] != c ) a[ l++ ] = a[ i ];
		
		if ( l != length ) {
			if ( hashLength < 0 ) {
				hashLength = -1;
				array = CharArrays.trim( array, l );
			}
			else hashLength = l;
		}
    	return this;
    }
    

    /** Removes all occurrences of the given characters.
    *
    * @param s the set of characters to remove.
    * @return this mutable string.
    */
    final public MutableString delete( final CharSet s ) {
		final int length = length();
    	final char[] a = array;

    	int l = 0;
		for( int i = 0; i < length; i++ ) if ( ! s.contains( a[ i ] ) ) a[ l++ ] = a[ i ];
		
		if ( l != length ) {
			if ( hashLength < 0 ) {
				hashLength = -1;
				array = CharArrays.trim( array, l );
			}
			else hashLength = l;
		}
    	return this;
    }
    
    /** Removes all occurrences of the given characters.
    *
    * @param c an array containing the characters to remove.
    * @return this mutable string.
    */
    final public MutableString delete( final char[] c ) {
		final int n = c.length;
		if ( n == 0 ) return this;

		final char[] a = array;
		final int length = length();
		int i = length, k, bloomFilter = 0;

		k = n;
		while ( k-- != 0 ) bloomFilter |= 1 << ( c[ k ] & 0x1F );
		
       	int l = 0;
       	for( i = 0; i < length; i++ ) { 
			if ( ( bloomFilter & ( 1 << ( a[ i ] & 0x1F ) ) ) != 0 ) {
				k = n;
				while ( k-- != 0 ) if ( a[ i ] == c[ k ] ) break;
				if ( k >= 0 ) continue;
			}
			a[ l++ ] = a[ i ];
    	}
		
		if ( l != length ) {
			if ( hashLength < 0 ) {
				hashLength = -1;
				array = CharArrays.trim( array, l );
			}
			else hashLength = l;
		}
    	return this;
    }
    
    /** Replaces the characters with indices ranging from <code>start</code> (inclusive)
     * to <code>end</code> (exclusive) with the given mutable string.
     *
     * @param start The starting index (inclusive).
     * @param end The ending index (exclusive).
     * @param s The mutable string to be copied.
     * @return this mutable string.
     * @throws IndexOutOfBoundsException if <code>start</code> is negative,
     * greater than <code>length()</code>, or greater than <code>end</code>.
     */

    final public MutableString replace( final int start, int end, final MutableString s ) {
		final int length = length();
		if ( end > length ) end = length;
		if ( start > end ) throw new StringIndexOutOfBoundsException();
		final int l = s.length();
		final int newLength = length + l - end + start;
		if ( l == 0 && newLength == length ) return this;

		if ( newLength >= length ) {
			expand( newLength );
			System.arraycopy( array, end, array, start + l, length - end );
			System.arraycopy( s.array, 0, array, start, l );
			
			hashLength = hashLength < 0 ? -1 : newLength;
		}
		else {
			System.arraycopy( array, end, array, start + l, length - end );
			System.arraycopy( s.array, 0, array, start, l );
			if ( hashLength < 0 ) {
				setCapacity( newLength );
				hashLength = -1;
			}
			else hashLength = newLength;
		}
        return this;
    }

    /** Replaces the characters with indices ranging from <code>start</code> (inclusive)
     * to <code>end</code> (exclusive) with the given <code>String</code>.
     * 
     * @param start The starting index (inclusive).
     * @param end The ending index (exclusive).
     * @param s The <code>String</code> to be copied.
     * @return this mutable string.
     * @throws IndexOutOfBoundsException if <code>start</code> is negative,
     * greater than <code>length()</code>, or greater than <code>end</code>.
     */

    final public MutableString replace( final int start, int end, final String s ) {
		final int length = length();
		if ( end > length ) end = length;
		if ( start > end ) throw new StringIndexOutOfBoundsException();
		final int l = s.length();
		final int newLength = length + l - end + start;
		if ( l == 0 && newLength == length ) return this;

		if ( newLength >= length ) {
			expand( newLength );
			System.arraycopy( array, end, array, start + l, length - end );
			s.getChars( 0, l, array, start );

			hashLength = hashLength < 0 ? -1 : newLength;
		}
		else {
			System.arraycopy( array, end, array, start + l, length - end );
			s.getChars( 0, l, array, start );
			if ( hashLength < 0 ) {
				setCapacity( newLength );
				hashLength = -1;
			}
			else hashLength = newLength;
		}

        return this;
    }

    /** Replaces the characters with indices ranging from <code>start</code> (inclusive)
     * to <code>end</code> (exclusive) with the given <code>CharSequence</code>.
     * 
     * @param start The starting index (inclusive).
     * @param end The ending index (exclusive).
     * @param s The <code>CharSequence</code> to be copied.
     * @return this mutable string.
     * @throws IndexOutOfBoundsException if <code>start</code> is negative,
     * greater than <code>length()</code>, or greater than <code>end</code>.
     */ 
    final public MutableString replace( final int start, int end, final CharSequence s ) {
		final int length = length();
		if ( end > length ) end = length;
		if ( start > end ) throw new StringIndexOutOfBoundsException();
		final int l = s.length();
		final int newLength = length + l - end + start;
		if ( l == 0 && newLength == length ) return this;

		if ( newLength >= length ) {
			expand( newLength );
			System.arraycopy( array, end, array, start + l, length - end );
			getChars( s, 0, l, array, start );

			hashLength = hashLength < 0 ? -1 : newLength;
		}
		else {
			System.arraycopy( array, end, array, start + l, length - end );
			getChars( s, 0, l, array, start );
			if ( hashLength < 0 ) {
				setCapacity( newLength );
				hashLength = -1;
			}
			else hashLength = newLength;
		}
        return this;
    }


    /** Replaces the characters with indices ranging from <code>start</code> (inclusive)
     * to <code>end</code> (exclusive) with the given character.
     * 
     * @param start The starting index (inclusive).
     * @param end The ending index (exclusive).
     * @param c The character to be copied.
     * @return this mutable string.
     * @throws IndexOutOfBoundsException if <code>start</code> is negative,
     * greater than <code>length()</code>, or greater than <code>end</code>.
     */ 
    final public MutableString replace( final int start, int end, final char c ) {
		final int length = length();
		if ( end > length ) end = length;
		if ( start > end ) throw new StringIndexOutOfBoundsException();
		final int newLength = length + 1 - end + start;

		if ( newLength >= length ) {
			expand( newLength );
			// TODO: optimise for the case end == start + 1
			System.arraycopy( array, end, array, start + 1, length - end );
			array[ start ] = c;

			hashLength = hashLength < 0 ? -1 : newLength;
		}
		else {
			System.arraycopy( array, end, array, start + 1, length - end );
			array[ start ] = c;
			if ( hashLength < 0 ) {
				setCapacity( newLength );
				hashLength = -1;
			}
			else hashLength = newLength;
		}

        return this;
    }


    /** Replaces the content of this mutable string with the given mutable string.
     *
     * @param s the mutable string whose content will replace the present one.
     * @return this mutable string.
     */

    final public MutableString replace( final MutableString s ) {
		return replace( 0, Integer.MAX_VALUE, s );
	}

    /** Replaces the content of this mutable string with the given string.
     *
     * @param s the string whose content will replace the present one.
     * @return this mutable string.
     */

    final public MutableString replace( final String s ) {
		return replace( 0, Integer.MAX_VALUE, s );
	}

    /** Replaces the content of this mutable string with the given character sequence.
     *
     * @param s the character sequence whose content will replace the present one.
     * @return this mutable string.
     */

    final public MutableString replace( final CharSequence s ) {
		return replace( 0, Integer.MAX_VALUE, s );
	}

    /** Replaces the content of this mutable string with the given character.
     *
     * @param c the character whose content will replace the present content.
     * @return this mutable string.
     */

    final public MutableString replace( final char c ) {
		return replace( 0, Integer.MAX_VALUE, c );
	}

    /** Replaces each occurrence of a set of characters with a corresponding mutable string.
	 *
	 * <P>Each occurrences of the character <code>c[i]</code> in this mutable
	 * string will be replaced by <code>s[i]</code>.  Note that
	 * <code>c</code> and <code>s</code> must have the same length, and that
	 * <code>c</code> must not contain duplicates. Moreover, each replacement
	 * string must be nonempty.
	 *
	 * <P>This method uses a Bloom filter to avoid repeated linear scans of the
	 * character array for {@link #length()} times; however, this optimisation
	 * will be most effective with arrays of less than twenty characters,
	 * and, in fact, will very slightly slow down replacements with more than one hundred characters.
     * 
	 * <P>This method will try <em>at most</em> one reallocation.
	 *
     * @param c an array of characters to be replaced.
     * @param s an array of replacement mutable strings.
     * @return this mutable string.
	 * @throws IllegalArgumentException if one of the replacement strings is empty.
     */

    final public MutableString replace( final char[] c, final MutableString[] s ) {
		final int n = c.length;

		if ( n == 0 ) return this;

		final int length = length();

		char[] a = array;
		int i, j, k, l, newLength = length, bloomFilter = 0;

		k = n;
		while( k-- != 0 ) {
			bloomFilter |= 1 << ( c[ k ] & 0x1F );
			if ( s[ k ].length() == 0 ) throw new IllegalArgumentException( "You cannot use the empty string as a replacement" );
		}

		i = length;
		boolean found = false;
		while ( i-- != 0 ) {
			if ( ( bloomFilter & ( 1 << ( a[ i ] & 0x1F ) ) ) != 0 ) {
				k = n;
				while ( k-- != 0 ) if ( a[ i ] == c[ k ] ) break;
				if ( k >= 0 ) {
					newLength += s[ k ].length() - 1;
					found = true;
				}
			}
		}
		if ( ! found ) return this;
		
		expand( newLength );
		a = array;

		i = newLength; // index in the new string
		j = length; // index in the old string

		while( j-- != 0 ) {
			if ( ( bloomFilter & ( 1 << ( a[ j ] & 0x1F ) ) ) != 0 ) {
				k = n;
				while ( k-- != 0 ) if ( a[ j ] == c[ k ] ) break;
				if ( k >= 0 ) {
					l = s[ k ].length();
					System.arraycopy( s[ k ].array, 0, array, i -= l, l );
					continue;
				}
			}
			a[ --i ] = a[ j ];
		}

		hashLength = hashLength < 0 ? -1 : newLength;
        return this;
    }


    /** Replaces each occurrence of a set of characters with a corresponding string.
	 *
	 * <P>Each occurrences of the character <code>c[i]</code> in this mutable
	 * string will be replaced by <code>s[i]</code>.  Note that
	 * <code>c</code> and <code>s</code> must have the same length, and that
	 * <code>c</code> must not contain duplicates. Moreover, each replacement
	 * string must be nonempty.
	 *
	 * <P>This method uses a Bloom filter to avoid repeated linear scans of the
	 * character array for {@link #length()} times; however, this optimisation
	 * will be most effective with arrays of less than twenty characters,
	 * and, in fact, will very slightly slow down replacements with more than one hundred characters.
     * 
	 * <P>This method will try <em>at most</em> one reallocation.
	 *
     * @param c an array of characters to be replaced.
     * @param s an array of replacement strings.
     * @return this mutable string.
	 * @throws IllegalArgumentException if one of the replacement strings is empty.
     */

    final public MutableString replace( final char[] c, final String[] s ) {
		final int n = c.length;

		if ( n == 0 ) return this;

		final int length = length();

		char[] a = array;
		int i, j, k, l, newLength = length, bloomFilter = 0;

		k = n;
		while( k-- != 0 ) {
			bloomFilter |= 1 << ( c[ k ] & 0x1F );
			if ( s[ k ].length() == 0 ) throw new IllegalArgumentException( "You cannot use the empty string as a replacement" );
		}

		i = length;
		boolean found = false;
		while ( i-- != 0 ) {
			if ( ( bloomFilter & ( 1 << ( a[ i ] & 0x1F ) ) ) != 0 ) {
				k = n;
				while ( k-- != 0 ) if ( a[ i ] == c[ k ] ) break;
				if ( k >= 0 ) {
					newLength += s[ k ].length() - 1;
					found = true;
				}
			}
		}
		if ( ! found ) return this;

		expand( newLength );
		a = array;

		i = newLength; // index in the new string
		j = length; // index in the old string

		while( j-- != 0 ) {
			if ( ( bloomFilter & ( 1 << ( a[ j ] & 0x1F ) ) ) != 0 ) {
				k = n;
				while ( k-- != 0 ) if ( a[ j ] == c[ k ] ) break;
				if ( k >= 0 ) {
					l = s[ k ].length();
					getChars( s[ k ], 0, l, array, i -= l );
					continue;
				}
			}
			a[ --i ] = a[ j ];
		}

		hashLength = hashLength < 0 ? -1 : newLength;
        return this;
    }


    /** Replaces each occurrence of a set of characters with a corresponding character sequence.
	 *
	 * <P>Each occurrences of the character <code>c[i]</code> in this mutable
	 * string will be replaced by <code>s[i]</code>.  Note that
	 * <code>c</code> and <code>s</code> must have the same length, and that
	 * <code>c</code> must not contain duplicates. Moreover, each replacement
	 * sequence must be nonempty.
	 *
	 * <P>This method uses a Bloom filter to avoid repeated linear scans of the
	 * character array for {@link #length()} times; however, this optimisation
	 * will be most effective with arrays of less than twenty characters,
	 * and, in fact, will very slightly slow down replacements with more than one hundred characters.
     * 
	 * <P>This method will try <em>at most</em> one reallocation.
	 *
     * @param c an array of characters to be replaced.
     * @param s an array of replacement character sequences.
     * @return this mutable string.
	 * @throws IllegalArgumentException if one of the replacement sequences is empty.
     */

    final public MutableString replace( final char[] c, final CharSequence[] s ) {
		final int n = c.length;

		if ( n == 0 ) return this;

		final int length = length();

		char[] a = array;
		int i, j, k, l, newLength = length, bloomFilter = 0;

		k = n;
		while( k-- != 0 ) {
			bloomFilter |= 1 << ( c[ k ] & 0x1F );
			if ( s[ k ].length() == 0 ) throw new IllegalArgumentException( "You cannot use the empty string as a replacement" );
		}

		i = length;
		boolean found = false;
		while ( i-- != 0 ) {
			if ( ( bloomFilter & ( 1 << ( a[ i ] & 0x1F ) ) ) != 0 ) {
				k = n;
				while ( k-- != 0 ) if ( a[ i ] == c[ k ] ) break;
				if ( k >= 0 ) {
					newLength += s[ k ].length() - 1;
					found = true;
				}
			}
		}
		if ( ! found ) return this;

		expand( newLength );
		a = array;

		i = newLength; // index in the new string
		j = length; // index in the old string

		while( j-- != 0 ) {
			if ( ( bloomFilter & ( 1 << ( a[ j ] & 0x1F ) ) ) != 0 ) {
				k = n;
				while ( k-- != 0 ) if ( a[ j ] == c[ k ] ) break;
				if ( k >= 0 ) {
					l = s[ k ].length();
					getChars( s[ k ], 0, l, array, i -= l );
					continue;
				}
			}
			a[ --i ] = a[ j ];
		}

		hashLength = hashLength < 0 ? -1 : newLength;
        return this;
    }


    /** Replaces each occurrence of a set characters with a corresponding character.
	 *
	 * <P>Each occurrences of the character <code>c[i]</code> in this mutable
	 * string will be replaced by <code>r[i]</code>.  Note that
	 * <code>c</code> and <code>s</code> must have the same length, and that
	 * <code>c</code> must not contain duplicates.
	 *
	 * <P>This method uses a Bloom filter to avoid repeated linear scans of the
	 * character array for {@link #length()} times; however, this optimisation
	 * will be most effective with arrays of less than twenty characters,
	 * and, in fact, will very slightly slow down replacements with more than one hundred characters.
     * 
     * @param c an array of characters to be replaced.
     * @param r an array of replacement characters.
     * @return this mutable string.
     */

    final public MutableString replace( final char[] c, final char[] r ) {
		final int n = c.length;
		if ( n == 0 ) return this;

		final char[] a = array;
		int i = length(), k, bloomFilter = 0;

		k = n;
		while ( k-- != 0 ) bloomFilter |= 1 << ( c[ k ] & 0x1F );
		
		while ( i-- != 0 ) {
			if ( ( bloomFilter & ( 1 << ( a[ i ] & 0x1F ) ) ) != 0 ) {
				k = n;
				while ( k-- != 0 ) if ( a[ i ] == c[ k ] ) break;
				if ( k >= 0 ) a[ i ] = r[ k ];
			}
		}

		if ( hashLength < 0 ) hashLength = -1;
        return this;
    }


    /** Replaces characters following a replacement map.
	 *
	 * <P>Each occurrence of a key of <code>m</code>
	 * will be  substituted with the corresponding value.
	 * 
    * @param m a map specifiying the character replacements.
    * @return this mutable string.
    */

    final public MutableString replace( final Char2CharMap m ) {
		final int length = length();
    	final char[] a = array;

    	boolean found = false;
		for( int i = 0; i < length; i++ ) 
			if ( m.containsKey( a[ i ] ) ) {
				a[ i ] = m.get( a[ i ] );
				found = true;
			}
		
		if ( found && hashLength < 0 ) hashLength = -1;
    	return this;
    }


    /** Replaces each occurrence of a character with a corresponding mutable string.
	 *
	 * <P>Each occurrences of the character <code>c</code> in this mutable
	 * string will be replaced by <code>s</code>.  Note that <code>s</code>
	 * must be nonempty.
	 *
	 * <P>This method will try <em>at most</em> one reallocation.
	 *
     * @param c a character to be replaced.
     * @param s a replacement mutable string.
     * @return this mutable string.
	 * @throws IllegalArgumentException if the replacement string is empty.
     */

    final public MutableString replace( final char c, final MutableString s ) {
		final int length = length();

		char[] a = array;
		int i, j, l, newLength = length;

		if ( s.length() == 0 ) throw new IllegalArgumentException( "You cannot use the empty string as a replacement" );

		i = length;

		boolean found = false;
		while ( i-- != 0 ) 
			if ( a[ i ] == c ) {
				newLength += s.length() - 1;
				found = true;
			}
		if ( ! found ) return this;

		expand( newLength );
		a = array;
		
		i = newLength; // index in the new string
		j = length; // index in the old string

		while( j-- != 0 ) {
			if ( a[ j ] == c ) {
				l = s.length();
				System.arraycopy( s.array, 0, a, i -= l, l );
			}
			else a[ --i ] = a[ j ];
		}

		hashLength = hashLength < 0 ? -1 : newLength;
        return this;
    }

    /** Replaces each occurrence of a character with a corresponding string.
	 *
	 * <P>Each occurrences of the character <code>c</code> in this mutable
	 * string will be replaced by <code>s</code>.  Note that <code>s</code>
	 * must be nonempty.
	 *
	 * <P>This method will try <em>at most</em> one reallocation.
	 *
     * @param c a character to be replaced.
     * @param s a replacement string.
     * @return this mutable string.
	 * @throws IllegalArgumentException if the replacement sequence is empty.
     */

    final public MutableString replace( final char c, final String s ) {
		final int length = length();

		char[] a = array;
		int i, j, l, newLength = length;

		if ( s.length() == 0 ) throw new IllegalArgumentException( "You cannot use the empty string as a replacement" );

		i = length;

		boolean found = false;
		while ( i-- != 0 ) 
			if ( a[ i ] == c ) {
				newLength += s.length() - 1;
				found = true;
			}
		if ( ! found ) return this;
			
		expand( newLength );
		a = array;

		i = newLength; // index in the new string
		j = length; // index in the old string

		while( j-- != 0 ) {
			if ( a[ j ] == c ) {
				l = s.length();
				s.getChars( 0, l, array, i -= l );
			}
			else a[ --i ] = a[ j ];
		}

		hashLength = hashLength < 0 ? -1 : newLength;
        return this;
    }


    /** Replaces each occurrence of a character with a corresponding character sequence.
	 *
	 * <P>Each occurrences of the character <code>c</code> in this mutable
	 * string will be replaced by <code>s</code>.  Note that <code>s</code>
	 * must be nonempty.
	 *
	 * <P>This method will try <em>at most</em> one reallocation.
	 *
     * @param c a character to be replaced.
     * @param s a replacement character sequence.
     * @return this mutable string.
	 * @throws IllegalArgumentException if the replacement sequence is empty.
     */

    final public MutableString replace( final char c, final CharSequence s ) {
		final int length = length();

		char[] a = array;
		int i, j, l, newLength = length;

		if ( s.length() == 0 ) throw new IllegalArgumentException( "You cannot use the empty string as a replacement" );

		i = length;
		
		boolean found = false;
		while ( i-- != 0 ) 
			if ( a[ i ] == c ) {
				newLength += s.length() - 1;
				found = true;
			}
		if ( ! found ) return this;
						
		expand( newLength );
		a = array;

		i = newLength; // index in the new string
		j = length; // index in the old string

		while ( j-- != 0 ) {
			if ( a[ j ] == c ) {
				l = s.length();
				getChars( s, 0, l, array, i -= l );
			}
			else a[ --i ] = a[ j ];
		}

		hashLength = hashLength < 0 ? -1 : newLength;
        return this;
    }

    /** Replaces each occurrence of a character with a corresponding character.
	 *
     * @param c a character to be replaced.
     * @param r a replacement character.
     * @return this mutable string.
     */

    final public MutableString replace( final char c, final char r ) {
		int i = length();
		final char[] a = array;
		while ( i-- != 0 ) if ( a[ i ] == c ) a[ i ] = r;

		changed();
        return this;
    }

    /** Replaces each occurrence of a mutable string with a corresponding mutable string.
	 *
	 * <P>Each occurrences of the mutable string <code>s</code> in this mutable
	 * string will be replaced by <code>r</code>.  Note that <code>s</code>
	 * must be nonempty, unless <code>r</code> is empty, too.
	 *
	 * <P>If the replacement string is longer than the search string,
	 * occurrences of the search string are matched <em>from the end</em> of
	 * this mutable string (i.e., using {@link #lastIndexOf(MutableString,int)
	 * lastIndexOf()}). Otherwise, occurrences of the search string are matched
	 * <em>from the start</em> (i.e., using {@link #indexOf(MutableString,int)
	 * indexOf()}). This has no effect on the semantics, unless
	 * there are overlapping occurrences.
	 *
	 * <P>This method will try <em>at most</em> one reallocation.
	 *
     * @param s a mutable string to be replaced.
     * @param r a replacement mutable string.
     * @return this mutable string.
	 * @throws IllegalArgumentException if you try to replace the empty string with a nonempty string.
     */

    final public MutableString replace( final MutableString s, final MutableString r ) {
		final int length = length();
		final int ns = s.length();
		final int nr = r.length();

		if ( ns == 0 ) {
			if ( nr == 0 ) return this;
			throw new IllegalArgumentException( "You cannot replace the empty string with a nonempty string" );
		}

		final char[] ar = r.array;
		final char[] as = s.array;
		final int bloomFilter = buildFilter( s, ns );
		final int diff = ns - nr;

		int i, j, l;

		if ( diff >= 0 ) { // Replacement string is not longer than the search string.
			final char[] a = array;

			if ( ( i = indexOf( as, ns, 0, bloomFilter ) ) != -1 ) {
				System.arraycopy( ar, 0, a, i, nr );
				j = i + nr; // The current start of the hole.
				l = diff; // The current length of the hole.
				
				while( ( i = indexOf( as, ns, i + ns, bloomFilter ) ) != -1 ) {
					if ( diff != 0 ) System.arraycopy( a, j + l, a, j, i - j - l );
					l += diff;
					j = i + ns - l;
					System.arraycopy( ar, 0, a, j - nr, nr );
				}

				if ( diff != 0 ) System.arraycopy( a, j + l, a, j, length - l - j );
				l = length - l;
				
				if ( hashLength < 0 ) {
					hashLength = -1;
					if ( diff != 0 ) {
						final char[] newArray = new char[ l ];
						System.arraycopy( a, 0, newArray, 0, l );
						array = newArray;
					}
				}
				else hashLength = l;
			}
		}
		else { // Replacement string is longer than the search string.
			j = 0;
			i = length;
			while( ( i = lastIndexOf( as, ns, i - ns, bloomFilter ) ) != -1 ) j++;

			if ( j != 0 ) {
				int m = l = length + j * - diff;
				expand( m );
				final char[] a = array;
				
				i = j = length; 

				while( ( i = lastIndexOf( as, ns, i - ns, bloomFilter ) ) != -1 ) {
					System.arraycopy( a, i + ns, a, l -= j - i - ns, j - i - ns );
					System.arraycopy( ar, 0, a, l -= nr, nr );
					j = i;
				}

				if ( hashLength < 0 ) hashLength = -1;
				else hashLength = m;
			}
		}
		return this;
    }



    /** Replaces each occurrence of a string with a corresponding string.
	 *
	 * <P>Each occurrences of the string <code>s</code> in this mutable
	 * string will be replaced by <code>r</code>.  Note that <code>s</code>
	 * must be nonempty, unless <code>r</code> is empty, too.
	 *
	 * <P>This method will try <em>at most</em> one reallocation.
	 *
     * @param s a string to be replaced.
     * @param r a replacement string.
     * @return this mutable string.
	 * @throws IllegalArgumentException if you try to replace the empty string with a nonempty string.
	 * @see #replace(MutableString,MutableString)
     */

    final public MutableString replace( final String s, final String r ) {
		final int length = length();
		final int ns = s.length();
		final int nr = r.length();

		if ( ns == 0 ) {
			if ( nr == 0 ) return this;
			throw new IllegalArgumentException( "You cannot replace the empty string with a nonempty string" );
		}

		final int bloomFilter = buildFilter( s, ns );
		final int diff = ns - nr;

		int i, j, l;

		if ( diff >= 0 ) { // Replacement string is not longer than the search string.
			final char[] a = array;

			if ( ( i = indexOf( s, ns, 0, bloomFilter ) ) != -1 ) {
				r.getChars( 0, nr, a, i );

				j = i + nr; // The current start of the hole.
				l = diff; // The current length of the hole.
				
				while( ( i = indexOf( s, ns, i + ns, bloomFilter ) ) != -1 ) {
					if ( diff != 0 ) System.arraycopy( a, j + l, a, j, i - j - l );
					l += diff;
					j = i + ns - l;
					r.getChars( 0, nr, a, j - nr );
				}

				if ( diff != 0 ) System.arraycopy( a, j + l, a, j, length - l - j );
				l = length - l;
				
				if ( hashLength < 0 ) {
					hashLength = -1;
					if ( diff != 0 ) {
						final char[] newArray = new char[ l ];
						System.arraycopy( a, 0, newArray, 0, l );
						array = newArray;
					}
				}
				else hashLength = l;
			}
		}
		else { // Replacement string is longer than the search string.
			j = 0;
			i = length;
			while( ( i = lastIndexOf( s, ns, i - ns, bloomFilter ) ) != -1 ) j++;

			if ( j != 0 ) {
				int m = l = length + j * - diff;
				expand( m );
				final char[] a = array;
				
				i = j = length; 

				while( ( i = lastIndexOf( s, ns, i - ns, bloomFilter ) ) != -1 ) {
					System.arraycopy( a, i + ns, a, l -= j - i - ns, j - i - ns );
					r.getChars( 0, nr, a, l -= nr );
					j = i;
				}

				if ( hashLength < 0 ) hashLength = -1;
				else hashLength = m;
			}
		}
		return this;
    }





    /** Replaces each occurrence of a character sequence with a corresponding character sequence.
	 *
	 * <P>Each occurrences of the string <code>s</code> in this mutable
	 * string will be replaced by <code>r</code>.  Note that <code>s</code>
	 * must be nonempty, unless <code>r</code> is empty, too.
	 *
	 * <P>This method will try <em>at most</em> one reallocation.
	 *
     * @param s a character sequence to be replaced.
     * @param r a replacement character sequence.
     * @return this mutable string.
	 * @throws IllegalArgumentException if you try to replace the empty sequence with a nonempty sequence.
	 * @see #replace(MutableString,MutableString)
     */

    final public MutableString replace( final CharSequence s, final CharSequence r ) {
		final int length = length();
		final int ns = s.length();
		final int nr = r.length();

		if ( ns == 0 ) {
			if ( nr == 0 ) return this;
			throw new IllegalArgumentException( "You cannot replace the empty string with a nonempty string" );
		}

		final int bloomFilter = buildFilter( s, ns );
		final int diff = ns - nr;

		int i, j, l;

		if ( diff >= 0 ) { // Replacement string is not longer than the search string.
			final char[] a = array;

			if ( ( i = indexOf( s, ns, 0, bloomFilter ) ) != -1 ) {
				getChars( r, 0, nr, a, i );

				j = i + nr; // The current start of the hole.
				l = diff; // The current length of the hole.
				
				while( ( i = indexOf( s, ns, i + ns, bloomFilter ) ) != -1 ) {
					if ( diff != 0 ) System.arraycopy( a, j + l, a, j, i - j - l );
					l += diff;
					j = i + ns - l;
					getChars( r, 0, nr, a, j - nr );
				}

				if ( diff != 0 ) System.arraycopy( a, j + l, a, j, length - l - j );
				l = length - l;
				
				if ( hashLength < 0 ) {
					hashLength = -1;
					if ( diff != 0 ) {
						final char[] newArray = new char[ l ];
						System.arraycopy( a, 0, newArray, 0, l );
						array = newArray;
					}
				}
				else hashLength = l;
			}
		}
		else { // Replacement string is longer than the search string.
			j = 0;
			i = length;
			while( ( i = lastIndexOf( s, ns, i - ns, bloomFilter ) ) != -1 ) j++;

			if ( j != 0 ) {
				int m = l = length + j * - diff;
				expand( m );
				final char[] a = array;
				
				i = j = length; 

				while( ( i = lastIndexOf( s, ns, i - ns, bloomFilter ) ) != -1 ) {
					System.arraycopy( a, i + ns, a, l -= j - i - ns, j - i - ns );
					getChars( r, 0, nr, a, l -= nr );
					j = i;
				}

				if ( hashLength < 0 ) hashLength = -1;
				else hashLength = m;
			}
		}
		return this;
    }





    /** Returns the index of the first occurrence of the
     * specified character.
     *
     * @param c the character to look for.
     * @return the index of the first occurrence of <code>c</code>, or
     * <code>-1</code>, if the character never appears.
     */
    final public int indexOf( final char c ) {
		return indexOf( c, 0 );
    }

    /** Returns the index of the first occurrence of the
     * specified character, starting at the specified index. 
     *
     * @param c the character to look for.
     * @param from the index from which the search must start.
     * @return the index of the first occurrence of <code>c</code>, or
     * <code>-1</code>, if the character never appears with index greater than
     * or equal to <code>from</code>.
     */
    final public int indexOf( final char c, final int from ) {
		final int length = length();
		final char[] a = array;
		int i = from < 0 ? -1 : from - 1;
		while( ++i < length ) if ( a[ i ] == c ) return i;
		return -1;
    }

	/** Computes a Bloom filter for the given character array using the given number of characters.
	 *
	 * @param s a character array.
	 * @param n the length of the prefix of <code>s</code> to use in building the filter.
	 * @return the Bloom filter for the specified characters.
	 */

	static private int buildFilter( final char[] s, final int n ) {
		int i = n, bloomFilter = 0;
		while ( i-- != 0 ) bloomFilter |= 1 << ( s[ i ] & 0x1f );
		return bloomFilter;
	}

    /** Returns the index of the first occurrence of the specified pattern, starting at the specified index. 
	 *
	 * <P>This method is used internally by {@link MutableString} for different methods, such as
	 * {@link #indexOf(MutableString)} and {@link #replace(MutableString,MutableString)}.
     *
     * @param pattern the string to look for.
     * @param n the number of valid characters in <code>pattern</code>.
     * @param from the index from which the search must start.
     * @param bloomFilter the Bloom filter for <code>pattern</code>.
     * @return the index of the first occurrence of <code>pattern</code> after
     * the first <code>from</code> characters, or <code>-1</code>, if the string never
     * appears.
	 */

	private int indexOf( final char[] pattern, final int n, final int from, final int bloomFilter ) {
		final int m1 = length() - 1;
		final char[] a = array, p = pattern;
		final char last = p[ n - 1 ];

		int i, j, k;

		i = ( from < 0 ? 0 : from ) + n - 1;
		while ( i < m1 ) {
			if ( a[ i ] == last ) {
				j = n - 1;
				k = i;
				while( j-- != 0 && a[ --k ] == p[ j ] );
				if ( j < 0 ) return k;
			}
			if ( ( bloomFilter & 1 << ( a[ ++i ] & 0x1f ) ) == 0 ) i += n;
		}

		// We unroll the last iteration because we cannot access a[ ++i ].
		if ( i == m1 ) {
			j = n;
			while( j-- != 0 ) if ( a[ i-- ] != p[ j ] ) return -1;
			return i + 1;
		}

		return -1;
	}


    /** Returns the index of the first occurrence of the specified mutable string, starting at the specified index. 
     *
	 * <P>This method uses a lightweight combination of Sunday's QuickSearch (a
	 * simplified but very effective variant of the Boyer&mdash;Moore search
	 * algorithm) and Bloom filters. More precisely, instead of recording the
	 * last occurrence of all characters in the pattern, we simply record in a
	 * small Bloom filter which characters belong to the pattern. Every time
	 * there is a mismatch, we look at the character <em>immediately
	 * following</em> the pattern: if it is not in the Bloom filter, besides
	 * moving to the next position we can additionally skip a number of
	 * characters equal to the length of the pattern.
	 *
	 * <P>Unless called with a pattern that saturates the filter, this method
	 * will usually outperform that of <code>String</code>.
	 *
     * @param pattern the mutable string to look for.
     * @param from the index from which the search must start.
     * @return the index of the first occurrence of <code>pattern</code> after
     * the first <code>from</code> characters, or <code>-1</code>, if the string never
     * appears.
	 */

    final public int indexOf( final MutableString pattern, final int from ) {

		final int n = pattern.length();

		if ( n == 0 ) return from > length() ? length() : ( from < 0 ? 0 : from );
		if ( n == 1 ) return indexOf( pattern.array[ n - 1 ], from );

		return indexOf( pattern.array, n, from, buildFilter( pattern.array, n ) );
    }


    /** Returns the index of the first occurrence of the specified mutable string.
	 *
     * @param pattern the mutable string to look for.
     * @return the index of the first occurrence of <code>pattern</code>, or
     * <code>-1</code>, if the string never appears.
	 * @see #indexOf(MutableString,int)
	 */
	final public int indexOf( final MutableString pattern ) {
		return indexOf( pattern, 0 );
	}


	/** Computes a Bloom filter for the given character sequence using the given number of characters.
	 *
	 * @param s a character sequence.
	 * @param n the length of the prefix of <code>s</code> to use in building the filter.
	 * @return the Bloom filter for the specified characters.
	 */
	static private int buildFilter( final CharSequence s, final int n ) {
		int i = n, bloomFilter = 0;
		while ( i-- != 0 ) bloomFilter |= 1 << ( s.charAt( i ) & 0x1f );
		return bloomFilter;
	}

    /** Returns the index of the first occurrence of the specified character sequence, starting at the specified index. 
	 *
	 * <P>This method is used internally by {@link MutableString} for different methods, such as
	 * {@link #indexOf(CharSequence)} and {@link #replace(CharSequence,CharSequence)}.
     *
     * @param pattern the character sequence to look for.
     * @param n the number of valid characters in <code>pattern</code>.
     * @param from the index from which the search must start.
     * @param bloomFilter the Bloom filter for <code>pattern</code>.
     * @return the index of the first occurrence of <code>pattern</code> after
     * the first <code>from</code> characters, or <code>-1</code>, if the string never
     * appears.
	 */

	private int indexOf( final CharSequence pattern, final int n, final int from, final int bloomFilter ) {
		final int m1 = length() - 1;
		final char[] a = array;
		final char last = pattern.charAt( n - 1 );

		int i, j, k;

		i = ( from < 0 ? 0 : from ) + n - 1;
		while ( i < m1 ) {
			if ( a[ i ] == last ) {
				j = n - 1;
				k = i;
				while( j-- != 0 && a[ --k ] == pattern.charAt( j ) );
				if ( j < 0 ) return k;
			}
			if ( ( bloomFilter & 1 << ( a[ ++i ] & 0x1f ) ) == 0 ) i += n;
		}

		// We unroll the last iteration because we cannot access a[ ++i ].
		if ( i == m1 ) {
			j = n;
			while( j-- != 0 ) if ( a[ i-- ] != pattern.charAt( j ) ) return -1;
			return i + 1;
		}

		return -1;
	}



    /** Returns the index of the first occurrence of the specified character sequence, starting at the specified index. 
	 *
	 * <P>Searching for a character sequence is slightly slower than {@link
	 * #indexOf(MutableString,int) searching for a mutable string}, as every
	 * character of the pattern must be accessed through a method
	 * call. Please consider wrapping <code>pattern</code> in a mutable string, or use
	 * {@link TextPattern} for repeated searches.
     *
     * @param pattern the character sequence to look for.
     * @param from the index from which the search must start.
     * @return the index of the first occurrence of <code>pattern</code> after
     * the first <code>from</code> characters, or <code>-1</code>, if the string never
     * appears.
	 * @see #indexOf(MutableString,int)
	 */

    final public int indexOf( final CharSequence pattern, final int from ) {
		final int n = pattern.length();

		if ( n == 0 ) return from > length() ? length() : ( from < 0 ? 0 : from );
		if ( n == 1 ) return indexOf( pattern.charAt( n - 1 ), from );

		return indexOf( pattern, n, from, buildFilter( pattern, n ) );
    }


    /** Returns the index of the first occurrence of the specified character sequence.
	 *
     * @param pattern the character sequence to look for.
     * @return the index of the first occurrence of <code>pattern</code>, or
     * <code>-1</code>, if the string never appears.
	 * @see #indexOf(CharSequence,int)
	 */
	final public int indexOf( final CharSequence pattern ) {
		return indexOf( pattern, 0 );
	}

	/** Returns the index of the first occurrence of the 
	 * specified text pattern, starting at the specified index.
	 * 
	 * <P>To use this method, you have to {@linkplain TextPattern#TextPattern(CharSequence, int) create
	 * a text pattern first}.
	 * 
	 * @param pattern a compiled text pattern to be searched for.
	 * @param from the index from which the search must start.
	 * @return the index of the first occurrence of <code>pattern</code>, or
	 * <code>-1</code>, if no such character ever appears.
	 */

	public int indexOf( final TextPattern pattern, final int from ) {
		return pattern.search( array(), from );
	}


	/** Returns the index of the first occurrence of the specified text pattern, starting at the specified index.
	 * 
	 * @param pattern a compiled text pattern to be searched for.
	 * @return the index of the first occurrence of <code>pattern</code>, or
	 * <code>-1</code>, if no such character ever appears.
	 * @see #indexOf(TextPattern, int)
	 */

	public int indexOf( final TextPattern pattern ) {
		return indexOf( pattern, 0 );
	}


	/** Returns the index of the first occurrence of any of the specified characters, starting at the specified index.
	 *
	 * @param s a set of characters to be searched for.
	 * @param from the index from which the search must start.
	 * @return the index of the first occurrence of any of the characters in <code>s</code>, or
	 * <code>-1</code>, if no such character ever appears.
	 */

	public int indexOfAnyOf( final CharSet s, final int from ) {
		final int n = s.size();
	
		if ( n == 0 ) return -1;
		if ( n == 1 ) return indexOf( s.iterator().nextChar(), from );
	
		final char[] a = array;
		final int length = length();

		int i = ( from < 0 ? 0 : from ) - 1;

		while ( ++i < length ) if ( s.contains( a[ i ] ) ) return i;
		
		return -1;
	}
	
	
	/** Returns the index of the first occurrence of any of the specified characters.
	 * 
	 * @param s a set of characters to be searched for.
	 * @return the index of the first occurrence of any of the characters in <code>s</code>, or
	 * <code>-1</code>, if no such character ever appears.
	 * @see #indexOfAnyOf(CharSet,int)
	 */

	public int indexOfAnyOf( final CharSet s ) {
		return indexOfAnyOf( s, 0 );
	}



	/** Returns the index of the first occurrence of any of the specified characters, starting at the specified index.
	 * 
	 * <P>This method uses a Bloom filter to avoid repeated linear scans of the
	 * array <code>c</code>; however, this optimisation
	 * will be most effective with arrays of less than twenty characters,
	 * and, in fact, will very slightly slow down searches for more than one hundred characters.
	 * 
     * @param n the number of valid characters in <code>c</code>.
	 * @param c an array of characters to be searched for.
     * @param from the index from which the search must start.
     * @param bloomFilter the Bloom filter for the characters in <code>c</code>.
     * @return the index of the first occurrence of any of the characters in <code>c</code>, or
     * <code>-1</code>, if no such character ever appears.
	 */

	private int indexOfAnyOf( final char[] c, final int n, final int from, final int bloomFilter ) {
		final int m = length();
		if ( n == 0 ) return -1;

		final char[] a = array;

		int i = ( from < 0 ? 0 : from ) - 1, k;

		while ( ++i < m ) {
			if ( ( bloomFilter & ( 1 << ( a[ i ] & 0x1F ) ) ) != 0 ) {
				k = n;
				while ( k-- != 0 ) if ( a[ i ] == c[ k ] ) return i;
			}
		}
		
		return -1;
	}

	/** Returns the index of the first occurrence of any of the specified characters, starting at the specified index.
	 *
 	 * <P>This method uses a Bloom filter to avoid repeated linear scans of the
	 * array <code>c</code>; however, this optimisation
	 * will be most effective with arrays of less than twenty characters,
	 * and, in fact, will very slightly slow down searches for more than one hundred characters.
	 * 
	 * @param c an array of characters to be searched for.
	 * @param from the index from which the search must start.
	 * @return the index of the first occurrence of any of the characters in <code>c</code>, or
	 * <code>-1</code>, if no such character ever appears.
	 */

	public int indexOfAnyOf( final char[] c, final int from ) {
		final int n = c.length;
	
		if ( n == 0 ) return -1;
		if ( n == 1 ) return indexOf( c[ 0 ], from );
	
		return indexOfAnyOf( c, n, from, buildFilter( c, n ) );
	}
	
	
	/** Returns the index of the first occurrence of any of the specified characters.
	 * 
	 * @param c an array of characters to be searched for.
	 * @return the index of the first occurrence of any of the characters in <code>c</code>, or
	 * <code>-1</code>, if no such character ever appears.
	 * @see #indexOfAnyOf(char[],int)
	 */

	public int indexOfAnyOf( final char[] c ) {
		return indexOfAnyOf( c, 0 );
	}


	/** Returns the index of the first occurrence of any character, except those specified, starting at the specified index.
	 *
	 * @param s a set of characters to be searched for.
	 * @param from the index from which the search must start.
	 * @return the index of the first occurrence of any of the characters <em>not</em> in <code>s</code>, or
	 * <code>-1</code>, if no such character ever appears.
	 */

	public int indexOfAnyBut( final CharSet s, final int from ) {
	
		final char[] a = array;
		final int length = length();

		int i = ( from < 0 ? 0 : from ) - 1;

		while ( ++i < length ) if ( ! s.contains( a[ i ] ) ) return i;
		
		return -1;
	}
	
	
	/** Returns the index of the first occurrence of any character, except those specified.
	 * 
	 * @param s a set of characters to be searched for.
	 * @return the index of the first occurrence of any of the characters <em>not</em> in <code>s</code>, or
	 * <code>-1</code>, if no such character ever appears.
	 * @see #indexOfAnyBut(CharSet,int)
	 */

	public int indexOfAnyBut( final CharSet s ) {
		return indexOfAnyOf( s, 0 );
	}



	/** Returns the index of the first occurrence of any character, except those specified, starting at the specified index.
	 * 
	 * <P>This method uses a Bloom filter to avoid repeated linear scans of the
	 * array <code>c</code>; however, this optimisation
	 * will be most effective with arrays of less than twenty characters,
	 * and, in fact, will very slightly slow down searches for more than one hundred characters.
	 * 
     * @param n the number of valid characters in <code>c</code>.
	 * @param c an array of characters to be searched for.
     * @param from the index from which the search must start (must be nonnegative).
     * @param bloomFilter the Bloom filter for the characters in <code>c</code>.
     * @return the index of the first occurrence of any of the characters <em>not</em> in <code>c</code>, or
     * <code>-1</code>, if no such character ever appears.
	 */

	private int indexOfAnyBut( final char[] c, final int n, final int from, final int bloomFilter ) {
		final int m = length();
		if ( n == 0 ) return from < m ? from : -1;

		final char[] a = array;

		int i = ( from < 0 ? 0 : from ) - 1, k;

		while ( ++i < m ) {
			if ( ( bloomFilter & ( 1 << ( a[ i ] & 0x1F ) ) ) != 0 ) {
				k = n;
				while ( k-- != 0 ) if ( a[ i ] == c[ k ] ) break;
				if ( k == -1 ) return i;
			}
			else return i;
		}
		
		return -1;
	}

	/** Returns the index of the first occurrence of any character, except those specified, starting at the specified index.
	 *
 	 * <P>This method uses a Bloom filter to avoid repeated linear scans of the
	 * array <code>c</code>; however, this optimisation
	 * will be most effective with arrays of less than twenty characters,
	 * and, in fact, will very slightly slow down searches for more than one hundred characters.
	 * 
	 * @param c an array of characters to be searched for.
	 * @param from the index from which the search must start.
	 * @return the index of the first occurrence of any of the characters <em>not</em> in <code>c</code>, or
	 * <code>-1</code>, if no such character ever appears.
	 */

	public int indexOfAnyBut( final char[] c, final int from ) {
		final int n = c.length;
		
		return indexOfAnyBut( c, n, from < 0 ? 0 : from, buildFilter( c, n ) );
	}
	
	
	/** Returns the index of the first occurrence of any character, except those specified.
	 * 
	 * @param c an array of characters to be searched for.
	 * @return the index of the first occurrence of any of the characters <em>not</em> in <code>c</code>, or
	 * <code>-1</code>, if no such character ever appears.
	 * @see #indexOfAnyBut(char[],int)
	 */

	public int indexOfAnyBut( final char[] c ) {
		return indexOfAnyOf( c, 0 );
	}


    /** Returns the index of the last occurrence of the
     * specified character.
     *
     * @param c the character to look for.
     * @return the index of the last occurrence of <code>c</code>, or
     * <code>-1</code>, if the character never appears.
     */
    final public int lastIndexOf( final char c ) {
		final char[] a = array;
		int i = length();
		while( i-- != 0 ) if ( a[ i ] == c ) return i;
		return -1;
    }

    /** Returns the index of the last occurrence of the specified character, searching backward starting at the specified index.
     *
     * @param c the character to look for.
     * @param from the index from which the search must start.
     * @return the index of the last occurrence of <code>c</code>, or
     * <code>-1</code>, if the character never appears with index smaller than
     * or equal to <code>from</code>.
     */
    final public int lastIndexOf( final char c, final int from ) {
		final char[] a = array;
		if ( from < 0 ) return -1;
		int i = length();
		if ( from < i ) i = from + 1;
		if ( i < 0 ) return -1;
		while( i-- != 0 ) if ( a[ i ] == c ) return i;
		return -1;
    }


    /** Returns the index of the last occurrence of the specified pattern, searching backward starting at the specified index.
	 *
	 * <P>This method is used internally by {@link MutableString} for different methods, such as
	 * {@link #lastIndexOf(MutableString)} and {@link #replace(MutableString,MutableString)}.
     *
     * @param pattern the string to look for.
     * @param n the number of valid characters in <code>pattern</code>.
     * @param from the index from which the search must start.
     * @param bloomFilter the Bloom filter for <code>pattern</code>.
     * @return the index of the last occurrence of <code>pattern</code>, or
     * <code>-1</code>, if the <code>pattern</code> never appears with index
     * smaller than or equal to <code>from</code>.
	 */

	private int lastIndexOf( final char[] pattern, final int n, final int from, final int bloomFilter ) {
		final char[] a = array, p = pattern;
		final char first = p[ 0 ];

		int i, j, k;
		i = length() - n;
		if ( from < i ) i = from;

		while ( i > 0 ) {
			if ( a[ i ] == first ) {
				j = n - 1;
				k = 0;
				while( j-- != 0 && a[ ++i ] == p[ ++k ] );
				if ( j < 0 ) return i - k;
				i -= k;
			}
			if ( ( bloomFilter & 1 << ( a[ --i ] & 0x1f ) ) == 0 ) i -= n;
		}

		// We unroll the last iteration because we cannot access a[ ++i ].
		if ( i == 0 ) {
			j = n;
			while( j-- != 0 ) if ( a[ j ] != p[ j ] ) return -1;
			return 0;
		}

		return -1;
	}

    /** Returns the index of the last occurrence of the specified mutable string, searching backward starting at the specified index.
     *
     * @param pattern the mutable string to look for.
     * @param from the index from which the search must start.
     * @return the index of the last occurrence of <code>pattern</code>, or
     * <code>-1</code>, if the <code>pattern</code> never appears with index
     * smaller than or equal to <code>from</code>.
	 */

    final public int lastIndexOf( final MutableString pattern, final int from ) {
		final int n = pattern.length();

		if ( from < 0 ) return -1;
		if ( n == 0 ) return from > length() ? length() : from;
		if ( n == 1 ) return lastIndexOf( pattern.array[ 0 ], from );

		return lastIndexOf( pattern.array, n, from, buildFilter( pattern.array, n ) );
    }


    /** Returns the index of the last occurrence of the specified mutable string.
     *
     * @param pattern the mutable string to look for.
     * @return the index of the last occurrence of <code>pattern</code>, or
     * <code>-1</code>, if the <code>pattern</code> never appears.
	 * @see #lastIndexOf(MutableString,int)
	 */
	final public int lastIndexOf( final MutableString pattern ) {
		return lastIndexOf( pattern, length() );
	}


    /** Returns the index of the last occurrence of the specified pattern, searching backward starting at the specified index.
	 *
	 * <P>This method is used internally by {@link MutableString} for different methods, such as
	 * {@link #lastIndexOf(MutableString)} and {@link #replace(MutableString,MutableString)}.
     *
     * @param pattern the string to look for.
     * @param n the number of valid characters in <code>pattern</code>.
     * @param from the index from which the search must start.
     * @param bloomFilter the Bloom filter for <code>pattern</code>.
     * @return the index of the last occurrence of <code>pattern</code>, or
     * <code>-1</code>, if the <code>pattern</code> never appears with index
     * smaller than or equal to <code>from</code>.
	 */

	private int lastIndexOf( final CharSequence pattern, final int n, final int from, final int bloomFilter ) {
		final char[] a = array;
		final char first = pattern.charAt( 0 );

		int i, j, k;
		i = length() - n;
		if ( from < i ) i = from;

		while ( i > 0 ) {
			if ( a[ i ] == first ) {
				j = n - 1;
				k = 0;
				while( j-- != 0 && a[ ++i ] == pattern.charAt( ++k ) );
				if ( j < 0 ) return i - k;
				i -= k;
			}
			if ( ( bloomFilter & 1 << ( a[ --i ] & 0x1f ) ) == 0 ) i -= n;
		}

		// We unroll the last iteration because we cannot access a[ ++i ].
		if ( i == 0 ) {
			j = n;
			while( j-- != 0 ) if ( a[ j ] != pattern.charAt( j ) ) return -1;
			return 0;
		}

		return -1;
	}



    /** Returns the index of the last occurrence of the specified character sequence, searching backward starting at the specified index.
     *
     * @param pattern the character sequence to look for.
     * @param from the index from which the search must start.
     * @return the index of the last occurrence of <code>pattern</code>, or
     * <code>-1</code>, if the <code>pattern</code> never appears with index
     * smaller than or equal to <code>from</code>.
	 * @see #lastIndexOf(MutableString,int)
	 */

    final public int lastIndexOf( final CharSequence pattern, final int from ) {
		final int n = pattern.length();

		if ( from < 0 ) return -1;
		if ( n == 0 ) return from > length() ? length() : from;
		if ( n == 1 ) return lastIndexOf( pattern.charAt( 0 ), from );

		return lastIndexOf( pattern, n, from, buildFilter( pattern, n ) );
    }


    /** Returns the index of the last occurrence of the specified character sequence.
     *
     * @param pattern the character sequence to look for.
     * @return the index of the last occurrence of <code>pattern</code>, or
     * <code>-1</code>, if the <code>pattern</code> never appears.
	 * @see #lastIndexOf(CharSequence,int)
	 */
	final public int lastIndexOf( final CharSequence pattern ) {
		return lastIndexOf( pattern, length() );
	}


	/** Returns the index of the last occurrence of any of the specified characters, searching backwards starting at the specified index.
	 * 
 	 * <P>This method uses a Bloom filter to avoid repeated linear scans of the
	 * array <code>c</code>; however, this optimisation
	 * will be most effective with arrays of less than twenty characters,
	 * and, in fact, will very slightly slow down searches for more than one hundred characters.
	 * 
	 * @param s a set of characters to be searched for.
	 * @param from the index from which the search must start.
	 * @return the index of the last occurrence of any character in <code>s</code>, or
	 * <code>-1</code>, if no character in <code>s</code> ever appears with index
	 * smaller than or equal to <code>from</code>.
	 */

	public int lastIndexOfAnyOf( final CharSet s, final int from ) {

		if ( from < 0 ) return -1;
		final int n = s.size();
		if ( n == 0 ) return -1;
		if ( n == 1 ) return lastIndexOf( s.iterator().nextChar(), from );
	

		final char[] a = array;

		int i = length();
		if ( from < i ) i = from + 1;

		while ( i-- > 0 ) if ( s.contains( a[ i ] ) ) return i;
		
		return -1;
	}
	
	/** Returns the index of the last occurrence of any of the specified characters.
	 * 
	 * @param s a set of characters to be searched for.
	 * @return the index of the last occurrence of any of the characters in <code>s</code>, or
	 * <code>-1</code>, if no such character ever appears.
	 * @see #lastIndexOfAnyOf(CharSet, int)
	 */

	public int lastIndexOfAnyOf( final CharSet s ) {
		return lastIndexOfAnyOf( s, length() );
	}

	/** Returns the index of the last occurrence of any of the specified characters, searching backwards starting at the specified index.
	 * 
 	 * <P>This method uses a Bloom filter to avoid repeated linear scans of the
	 * array <code>c</code>; however, this optimisation
	 * will be most effective with arrays of less than twenty characters,
	 * and, in fact, will very slightly slow down searches for more than one hundred characters.
	 * 
	 * @param c an array of characters to be searched for.
	 * @param n the length of <code>c</code>.
	 * @param from the index from which the search must start.
	 * @param bloomFilter the Bloom filter for the characters in <code>c</code>.
	 * @return the index of the last occurrence of any character in <code>c</code>, or
	 * <code>-1</code>, if no character in <code>c</code> ever appears with index
	 * smaller than or equal to <code>from</code>.
	 */

	private int lastIndexOfAnyOf( final char[] c, final int n, final int from, final int bloomFilter ) {
		if ( n == 0 ) return -1;

		final char[] a = array;

		int i = length(), k;
		if ( from < i ) i = from + 1;

		while ( i-- > 0 ) {
			if ( ( bloomFilter & ( 1 << ( a[ i ] & 0x1F ) ) ) != 0 ) {
				k = n;
				while ( k-- != 0 ) if ( a[ i ] == c[ k ] ) return i;
			}
		}
		
		return -1;
	}

	/** Returns the index of the last occurrence of any of the specified characters, searching backwards starting at the specified index.
	 * 
 	 * <P>This method uses a Bloom filter to avoid repeated linear scans of the
	 * array <code>c</code>; however, this optimisation
	 * will be most effective with arrays of less than twenty characters,
	 * and, in fact, will very slightly slow down searches for more than one hundred characters.
	 * 
	 * @param c an array of characters to be searched for.
	 * @param from the index from which the search must start.
	 * @return the index of the last occurrence of any character in <code>c</code>, or
	 * <code>-1</code>, if no character in <code>c</code> ever appears with index
	 * smaller than or equal to <code>from</code>.
	 */

	public int lastIndexOfAnyOf( final char[] c, final int from ) {
		final int n = c.length;
	
		if ( from < 0 ) return -1;
		if ( n == 0 ) return -1;
		if ( n == 1 ) return lastIndexOf( c[ 0 ], from );
	
		return lastIndexOfAnyOf( c, n, from, buildFilter( c, n ) );
	}
	
	/** Returns the index of the last occurrence of any of the specified characters.
	 * 
	 * @param c an array of characters to be searched for.
	 * @return the index of the last occurrence of any of the characters in <code>c</code>, or
	 * <code>-1</code>, if no such character ever appears.
	 * @see #lastIndexOfAnyOf(char[], int)
	 *	 */

	public int lastIndexOfAnyOf( final char[] c ) {
		return lastIndexOfAnyOf( c, length() );
	}






	/** Returns the index of the last occurrence of any character, except those specified, starting at the specified index.
	 *
	 * <P>This method uses a Bloom filter to avoid repeated linear scans of the
	 * array <code>c</code>; however, this optimisation
	 * will be most effective with arrays of less than twenty characters,
	 * and, in fact, will very slightly slow down searches for more than one hundred characters.
	 * 
	 * @param s a set of characters to be searched for.
	 * @param from the index from which the search must start.
	 * @return the index of the last occurrence of any of the characters <em>not</em> in <code>c</code>, or
	 * <code>-1</code>, if no such character ever appears.
	 */

	public int lastIndexOfAnyBut( final CharSet s, final int from ) {

		if ( from < 0 ) return -1;
		final char[] a = array;

		int i = length();
		if ( s.size() == 0 ) return from < i ? from : i - 1;
		if ( from < i ) i = from + 1;

		while ( i-- > 0 ) if ( ! s.contains( a[ i ] ) ) return i;
		
		return -1;
	}
	

	/** Returns the index of the last occurrence of any character, except those specified.
	 * 
	 * @param s a set of characters to be searched for.
	 * @return the index of the last occurrence of any of the characters <em>not</em> in <code>s</code>, or
	 * <code>-1</code>, if no such character ever appears.
	 * @see #lastIndexOfAnyBut(CharSet,int)
	 */

	public int lastIndexOfAnyBut( final CharSet s ) {
		return lastIndexOfAnyBut( s, length() );
	}





	/** Returns the index of the last occurrence of any character, except those specified, starting at the specified index.
	 * 
	 * <P>This method uses a Bloom filter to avoid repeated linear scans of the
	 * array <code>c</code>; however, this optimisation
	 * will be most effective with arrays of less than twenty characters,
	 * and, in fact, will very slightly slow down searches for more than one hundred characters.
	 * 
     * @param n the number of valid characters in <code>c</code>.
	 * @param c an array of characters to be searched for.
     * @param from the index from which the search must start (must be nonnegative).
     * @param bloomFilter the Bloom filter for the characters in <code>c</code>.
     * @return the index of the last occurrence of any of the characters <em>not</em> in <code>c</code>, or
     * <code>-1</code>, if no such character ever appears.
	 */

	private int lastIndexOfAnyBut( final char[] c, final int n, final int from, final int bloomFilter ) {
		final char[] a = array;
		int i = length(), k;
		if ( i == 0 ) return -1;
		if ( n == 0 ) return from < i ? from : i - 1;

		if ( from < i ) i = from + 1;

		while ( i-- != 0 ) {
			if ( ( bloomFilter & ( 1 << ( a[ i ] & 0x1F ) ) ) != 0 ) {
				k = n;
				while ( k-- != 0 ) if ( a[ i ] == c[ k ] ) break;
				if ( k == - 1 ) return i;
			}
			else return i;
		}
		
		return -1;
	}

	/** Returns the index of the last occurrence of any character, except those specified, starting at the specified index.
	 *
 	 * <P>This method uses a Bloom filter to avoid repeated linear scans of the
	 * array <code>c</code>; however, this optimisation
	 * will be most effective with arrays of less than twenty characters,
	 * and, in fact, will very slightly slow down searches for more than one hundred characters.
	 * 
	 * @param c an array of characters to be searched for.
	 * @param from the index from which the search must start.
	 * @return the index of the last occurrence of any of the characters <em>not</em> in <code>c</code>, or
	 * <code>-1</code>, if no such character ever appears.
	 */

	public int lastIndexOfAnyBut( final char[] c, int from ) {
		if ( from < 0 ) return -1;
		final int n = c.length;
		return lastIndexOfAnyBut( c, n, from, buildFilter( c, n ) );
	}
	
	
	/** Returns the index of the last occurrence of any character, except those specified.
	 * 
	 * @param c an array of characters to be searched for.
	 * @return the index of the last occurrence of any of the characters <em>not</em> in <code>c</code>, or
	 * <code>-1</code>, if no such character ever appears.
	 * @see #lastIndexOfAnyBut(char[],int)
	 */

	public int lastIndexOfAnyBut( final char[] c ) {
		return lastIndexOfAnyBut( c, 0 );
	}

	/** Spans a segment of this mutable string made of the specified characters.
	 * 
	 * @param s a set of characters.
	 * @param from the index from which to span.
	 * @return the length of the maximal subsequence of this mutable string made of 
	 * characters in <code>s</code> starting at <code>from</code>.
	 * @see #cospan(CharSet, int)
	 */

	public int span( final CharSet s, int from ) {
		final int length = length();
		if ( s.size() == 0 ) return 0;
		final char[] a = array;

		if ( from < 0 ) from = 0;
		int i = from - 1;

		while ( ++i < length ) if ( ! s.contains( a[ i ] ) ) break; 
		
		return i - from;
	}

	/** Spans the initial segment of this mutable string made of the specified characters.
	 *
	 * @param s a set of characters.
	 * @return the length of the maximal initial subsequence of this mutable string made of 
	 * characters in <code>s</code>.
	 * @see #span(CharSet, int)
	 */

	public int span( final CharSet s ) {
		return span( s, 0 );
	}


	/** Spans a segment of this mutable string made of the specified characters.
	 * 
	 * <P>This method uses a Bloom filter to avoid repeated linear scans of the
	 * array <code>c</code>; however, this optimisation
	 * will be most effective with arrays of less than twenty characters,
	 * and, in fact, will very slightly slow down searches for more than one hundred characters.
	 * 
	 * @param c an array of characters.
	 * @param from the index from which to span.
	 * @return the length of the maximal subsequence of this mutable string made of 
	 * characters in <code>c</code> starting at <code>from</code>.
	 * @see #cospan(char[], int)
	 */

	public int span( final char[] c, int from ) {
		final int length = length(), n = c.length;
		if ( n == 0 ) return 0;
		final int bloomFilter = buildFilter( c, n );
		final char[] a = array;

		if ( from < 0 ) from = 0;
		int i = from - 1, k;

		while ( ++i < length ) {
			if ( ( bloomFilter & ( 1 << ( a[ i ] & 0x1F ) ) ) != 0 ) {
				k = n;
				while ( k-- != 0 ) if ( a[ i ] == c[ k ] ) break;
				if ( k == -1 ) return i - from;
			}
			else return i - from;
		}
		
		return i - from;
	}

	/** Spans the initial segment of this mutable string made of the specified characters.
	 *
	 * @param c an array of characters.
	 * @return the length of the maximal initial subsequence of this mutable string made of 
	 * characters in <code>c</code>.
	 * @see #span(char[], int)
	 */

	public int span( final char[] c ) {
		return span( c, 0 );
	}


	/** Spans a segment of this mutable string made of the complement of the specified characters.
	 * 
	 * @param s a set of characters.
	 * @param from the index from which to span.
	 * @return the length of the maximal subsequence of this mutable string made of 
	 * characters <em>not</em> in <code>s</code> starting at <code>from</code>.
	 * @see #span(CharSet, int)
	 */

	public int cospan( final CharSet s, int from ) {
		final int length = length();
		if ( s.size() == 0 ) return from < 0 ? length : ( from < length ? length - from : 0 );
		final char[] a = array;

		if ( from < 0 ) from = 0;
		int i = from - 1;

		while ( ++i < length ) if ( s.contains( a[ i ] ) ) break;
		
		return i - from;
	}

	/** Spans the initial segment of this mutable string made of the complement of the specified characters.
	 *
	 * @param s a set of characters.
	 * @return the length of the maximal initial subsequence of this mutable string made of 
	 * characters <em>not</em> in <code>s</code>.
	 * @see #cospan(CharSet, int)
	 */

	public int cospan( final CharSet s ) {
		return cospan( s, 0 );
	}


	/** Spans a segment of this mutable string made of the complement of the specified characters.
	 * 
	 * <P>This method uses a Bloom filter to avoid repeated linear scans of the
	 * array <code>c</code>; however, this optimisation
	 * will be most effective with arrays of less than twenty characters,
	 * and, in fact, will very slightly slow down searches for more than one hundred characters.
	 * 
	 * @param c an array of characters.
	 * @param from the index from which to span.
	 * @return the length of the maximal subsequence of this mutable string made of 
	 * characters <em>not</em> in <code>c</code> starting at <code>from</code>.
	 * @see #span(char[], int)
	 */

	public int cospan( final char[] c, int from ) {
		final int length = length(), n = c.length;
		if ( n == 0 ) return from < 0 ? length : ( from < length ? length - from : 0 );
		final int bloomFilter = buildFilter( c, n );
		final char[] a = array;

		if ( from < 0 ) from = 0;
		int i = from - 1, k;

		while ( ++i < length ) {
			if ( ( bloomFilter & ( 1 << ( a[ i ] & 0x1F ) ) ) != 0 ) {
				k = n;
				while ( k-- != 0 ) if ( a[ i ] == c[ k ] ) break;
				if ( k != -1 ) return i - from;
			}
		}
		
		return i - from;
	}

	/** Spans the initial segment of this mutable string made of the complement of the specified characters.
	 *
	 * @param c an array of characters.
	 * @return the length of the maximal initial subsequence of this mutable string made of 
	 * characters <em>not</em> in <code>c</code>.
	 * @see #cospan(char[], int)
	 */

	public int cospan( final char[] c ) {
		return cospan( c, 0 );
	}


	/** Returns whether this mutable string starts with the given mutable string.
	 *
	 * @param prefix a mutable string.
	 * @return <code>true</code> if this mutable string starts with <code>prefix</code>.
	 */

	final public boolean startsWith( final MutableString prefix ) {
		final int l = prefix.length();
		if ( l > length() ) return false;
		int i = l;
		final char[] a1 = prefix.array;
		final char[] a2 = array;
		while( i-- != 0 ) if ( a1[ i ] != a2[ i ] ) return false;
		return true;
	}

	/** Returns whether this mutable string starts with the given string.
	 *
	 * @param prefix a string.
	 * @return <code>true</code> if this mutable string starts with <code>prefix</code>.
	 */

	final public boolean startsWith( final String prefix ) {
		final int l = prefix.length();
		if ( l > length() ) return false;
		int i = l;
		final char[] a = array;
		while( i-- != 0 ) if ( prefix.charAt( i ) != a[ i ] ) return false;
		return true;
	}

	/** Returns whether this mutable string starts with the given character sequence.
	 *
	 * @param prefix a character sequence.
	 * @return <code>true</code> if this mutable string starts with <code>prefix</code>.
	 */

	final public boolean startsWith( final CharSequence prefix ) {
		final int l = prefix.length();
		if ( l > length() ) return false;
		int i = l;
		final char[] a = array;
		while( i-- != 0 ) if ( prefix.charAt( i ) != a[ i ] ) return false;
		return true;
	}

	/** Returns whether this mutable string starts with the given mutable string disregarding case.
	 * 
	 *
	 * @param prefix a mutable string.
	 * @return <code>true</code> if this mutable string starts with <code>prefix</code> up to case.
	 */

	final public boolean startsWithIgnoreCase( final MutableString prefix ) {
		final int l = prefix.length();
		if ( l > length() ) return false;
		int i = l;
		final char[] a1 = prefix.array;
		final char[] a2 = array;
		char c, d;
		while( i-- != 0 ) {
			c = Character.toLowerCase( Character.toUpperCase( a1[ i ] ) );
			d = Character.toLowerCase( Character.toUpperCase( a2[ i ] ) );
			if ( c != d ) return false;
		}
		return true;
	}

	/** Returns whether this mutable string starts with the given string disregarding case.
	 *
	 * @param prefix a string.
	 * @return <code>true</code> if this mutable string starts with <code>prefix</code> up to case.
	 */

	final public boolean startsWithIgnoreCase( final String prefix ) {
		final int l = prefix.length();
		if ( l > length() ) return false;
		int i = l;
		final char[] a = array;
		char c, d;
		while( i-- != 0 ) {
			c = Character.toLowerCase( Character.toUpperCase( a[ i ] ) );
			d = Character.toLowerCase( Character.toUpperCase( prefix.charAt( i ) ) );
			if ( c != d ) return false;
		}
		return true;
	}

	/** Returns whether this mutable string starts with the given character sequence disregarding case.
	 *
	 * @param prefix a character sequence.
	 * @return <code>true</code> if this mutable string starts with <code>prefix</code> up to case.
	 */

	final public boolean startsWithIgnoreCase( final CharSequence prefix ) {
		final int l = prefix.length();
		if ( l > length() ) return false;
		int i = l;
		final char[] a = array;
		char c, d;
		while( i-- != 0 ) {
			c = Character.toLowerCase( Character.toUpperCase( a[ i ] ) );
			d = Character.toLowerCase( Character.toUpperCase( prefix.charAt( i ) ) );
			if ( c != d ) return false;
		}
		return true;
	}

	/** Returns whether this mutable string ends with the given mutable string.
	 *
	 * @param suffix a mutable string.
	 * @return <code>true</code> if this mutable string ends with <code>suffix</code>.
	 */

	final public boolean endsWith( final MutableString suffix ) {
		final int l = suffix.length();
		int length = length();
		if ( l > length ) return false;
		int i = l;
		final char[] a1 = suffix.array;
		final char[] a2 = array;
		while( i-- != 0 ) if ( a1[ i ] != a2[ --length ] ) return false;
		return true;
	}

	/** Returns whether this mutable string ends with the given string.
	 *
	 * @param suffix a string.
	 * @return <code>true</code> if this mutable string ends with <code>suffix</code>.
	 */

	final public boolean endsWith( final String suffix ) {
		final int l = suffix.length();
		int length = length();
		if ( l > length ) return false;
		int i = l;
		final char[] a = array;
		while( i-- != 0 ) if ( suffix.charAt( i ) != a[ --length ] ) return false;
		return true;
	}

	/** Returns whether this mutable string ends with the given character sequence.
	 *
	 * @param suffix a character sequence.
	 * @return <code>true</code> if this mutable string ends with <code>prefix</code>.
	 */

	final public boolean endsWith( final CharSequence suffix ) {
		final int l = suffix.length();
		int length = length();
		if ( l > length ) return false;
		int i = l;
		final char[] a = array;
		while( i-- != 0 ) if ( suffix.charAt( i ) != a[ --length ] ) return false;
		return true;
	}

	/** Returns whether this mutable string ends with the given mutable string disregarding case.
	 *
	 * @param suffix a mutable string.
	 * @return <code>true</code> if this mutable string ends with <code>suffix</code> up to case.
	 */

	final public boolean endsWithIgnoreCase( final MutableString suffix ) {
		final int l = suffix.length();
		int length = length();
		if ( l > length ) return false;
		int i = l;
		final char[] a1 = suffix.array;
		final char[] a2 = array;
		char c, d;
		while( i-- != 0 ) {
			c = Character.toLowerCase( Character.toUpperCase( a1[ i ] ) );
			d = Character.toLowerCase( Character.toUpperCase( a2[ --length ] ) );
			if ( c != d ) return false;
		}
		return true;
	}

	/** Returns whether this mutable string ends with the given string disregarding case.
	 *
	 * @param suffix a string.
	 * @return <code>true</code> if this mutable string ends with <code>suffix</code> up to case.
	 */

	final public boolean endsWithIgnoreCase( final String suffix ) {
		final int l = suffix.length();
		int length = length();
		if ( l > length ) return false;
		int i = l;
		final char[] a = array;
		char c, d;
		while( i-- != 0 ) {
			c = Character.toLowerCase( Character.toUpperCase( suffix.charAt( i ) ) );
			d = Character.toLowerCase( Character.toUpperCase( a[ --length ] ) );
			if ( c != d ) return false;
		}
		return true;
	}

	/** Returns whether this mutable string ends with the given character sequence disregarding case.
	 *
	 * @param suffix a character sequence.
	 * @return <code>true</code> if this mutable string ends with <code>prefix</code> up to case.
	 */

	final public boolean endsWithIgnoreCase( final CharSequence suffix ) {
		final int l = suffix.length();
		int length = length();
		if ( l > length ) return false;
		int i = l;
		final char[] a = array;
		char c, d;
		while( i-- != 0 ) {
			c = Character.toLowerCase( Character.toUpperCase( suffix.charAt( i ) ) );
			d = Character.toLowerCase( Character.toUpperCase( a[ --length ] ) );
			if ( c != d ) return false;
		}
		return true;
	}



    /** Converts all of the characters in this mutable string to lower
     * case using the rules of the default locale.
	 * @return this mutable string.
     */
    final public MutableString toLowerCase() {
		int n = length();
		final char[] a = array;
		while( n-- != 0 ) a[ n ] = Character.toLowerCase( a[ n ] );
		changed();
		return this;
    }

    /** Converts all of the characters in this mutable string to upper
     * case using the rules of the default locale. 
	 * @return this mutable string.
     */
    final public MutableString toUpperCase() {
		int n = length();
		final char[] a = array;
		while( n-- != 0 ) a[ n ] = Character.toUpperCase( a[ n ] );
		changed();
		return this;
    }


    /** Trims all leading and trailing whitespace from this string. 
     * <em>Whitespace</em> here is any character smaller than <samp>'\u0020'</samp> (the ASCII space).
     *
	 * @return this mutable string.
     */
    final public MutableString trim() {
		final int length = length();
		final char[] a = array;
		int i = 0;
	
		if ( length == 0 ) return this;
		while ( i < length && a[ i ] <= ' ' ) i++;
		if ( i == length ) {
			if ( hashLength < 0 ) {
				hashLength = -1;
				array = CharArrays.EMPTY_ARRAY;
				return this;
			}
			hashLength = 0;
			return this;
		}

		int j = length;

		while ( a[ --j ] <= ' ' );
		final int newLength = j - i + 1;
		if ( length == newLength ) return this;

		System.arraycopy( array, i, array, 0, newLength ); 

		if ( hashLength < 0 ) {
			setCapacity( newLength );
			hashLength = -1;
		}
		else hashLength = newLength;

		return this;
    }


	/** Trims all leading whitespace from this string. 
     * <em>Whitespace</em> here is any character smaller than <samp>'\u0020'</samp> (the ASCII space).
	 *
	 * @return this mutable string.
	 */
	final public MutableString trimLeft() {
		final int length = length();
		final char[] a = array;
		int i = 0;
	
		if ( length == 0 ) return this;
		while ( i < length && a[ i ] <= ' ' ) i++;

		if ( i == length ) {
			if ( hashLength < 0 ) {
				hashLength = -1;
				array = CharArrays.EMPTY_ARRAY;
				return this;
			}
			hashLength = 0;
		}

		final int newLength = length - i;
		if ( length == newLength ) return this;

		System.arraycopy( array, i, array, 0, newLength ); 

		if ( hashLength < 0 ) {
			setCapacity( newLength );
			hashLength = -1;
		}
		else hashLength = newLength;

		return this;
	}

	/** Trims all trailing whitespace from this string. 
     * <em>Whitespace</em> here is any character smaller than <samp>'\u0020'</samp> (the ASCII space).
	 *
	 * @return this mutable string.
	 */
	final public MutableString trimRight() {
		final int length = length();
		final char[] a = array;
	
		if ( length == 0 ) return this;
		
		int j = length;

		while ( j-- != 0 ) if ( a[ j ] > ' ' ) break;
		final int newLength = j + 1;

		if ( length == newLength ) return this;

		if ( hashLength < 0 ) {
			setCapacity( newLength );
			hashLength = -1;
		}
		else hashLength = newLength;

		return this;
	}

    /** Squeezes and normalises spaces in this mutable string. All subsequences of consecutive
     * characters satisfying {@link Character#isSpaceChar(char)} (or
     * {@link Character#isWhitespace(char)} if <code>squeezeOnlyWhitespace</code> is true) 
     * will be transformed into a single space.
     * 
     * @param squeezeOnlyWhitespace if true, a space is defined by {@link Character#isWhitespace(char)};
     * otherwise, a space is defined by {@link Character#isSpaceChar(char)}.
     * @return this mutable string.
     */
    final public MutableString squeezeSpaces( final boolean squeezeOnlyWhitespace ) {
    	final int length = length();
    	final char[] a = array;

    	int i = 0, j = 0;
    	while( i < length ) {
    		if ( ! ( squeezeOnlyWhitespace ? Character.isWhitespace( a[ i ] ) : Character.isSpaceChar( a[ i ] ) ) ) a[ j++ ] = a[ i++ ];
    		else {
    			a[ j++ ] = ' ';
        		while ( i < length && ( squeezeOnlyWhitespace ? Character.isWhitespace( a[ i ] ) : Character.isSpaceChar( a[ i ] ) ) ) i++;
       		}
    	}

    	if ( length == j ) return this;

		if ( hashLength < 0 ) {
			setCapacity( j );
			hashLength = -1;
		}
		else hashLength = j;
    	
    	return this;
    }

    /** Squeezes and normalises whitespace in this mutable string. All subsequences of consecutive
     * characters satisfying {@link Character#isWhitespace(char)} will be transformed
     * into a single space.
     * @return this mutable string.
     * @see #squeezeSpace()
     */
    final public MutableString squeezeWhitespace() {
    	return squeezeSpaces( true );
    }

    /** Squeezes and normalises spaces in this mutable string. All subsequences of consecutive
     * characters satisfying {@link Character#isSpaceChar(char)} will be transformed
     * into a single space.
     * @return this mutable string.
     * @see #squeezeWhitespace()
     */
    final public MutableString squeezeSpace() {
    	return squeezeSpaces( false );
    }

	
    /** The characters in this mutable string get reversed. 
     *
     * @return this mutable string.
     */
    final public MutableString reverse() {
		final int k = length() - 1;
		final char[] a = array;
		char c;

		int i = ( k - 1 ) / 2  + 1;

		while( i-- != 0 ) {
			c = a[ i ]; a[ i ] = a[ k - i ]; a[ k - i ] = c;
		}
		changed();
		return this;
    }
    

    /** Writes this mutable string to a {@link Writer}.
     *
     * @param w a {@link Writer}.
     * @throws IOException if thrown by the provided {@link Writer}.
     */
    final public void write( final Writer w ) throws IOException {
		if ( hashLength < 0 ) w.write( array );
		else w.write( array, 0, hashLength );
    }

    /** Reads a mutable string that has been written by {@link #write(Writer)} from a {@link Reader}.
     *
     * <P>If <code>length</code> is smaller than the current capacity or the
     * number of characters actually read is smaller than <code>length</code>
     * the string will become loose.
     *
     * @param r a {@link Reader}.
     * @param length the number of characters to read.
     * @return The number of characters read, or -1 if the end of the stream has been reached.
     * @throws IOException if thrown by the provided {@link Reader}.
     */
    final public int read( final Reader r, final int length ) throws IOException {
		final boolean compact = hashLength < 0;
		expand( length );
		hashLength = 0; // In case of an exception, we empty the string.
		final int result = r.read( array, 0, length );
		// If the string was compact and we can make it again compact, we do it.
		if ( result < length ) hashLength = result == -1 ? 0 : result;
		else hashLength = compact && length == array.length ? -1 : length;
		return result;
    }


    /** Prints this mutable string to a {@link PrintWriter}.
     * @param w a {@link PrintWriter}.
     */
    final public void print( final PrintWriter w ) {
		if ( hashLength < 0 ) w.write( array );
		else w.write( array, 0, hashLength );
    }

    /** Prints this mutable string to a {@link PrintWriter} and then terminates the line.
     * @param w a {@link PrintWriter}.
     */
    final public void println( final PrintWriter w ) {
		print( w );
		w.println();
    }

    /** Prints this mutable string to a {@link PrintStream}.
     * @param s a {@link PrintStream}.
     */
    final public void print( final PrintStream s ) {
		if ( hashLength < 0 ) s.print( array );
		else s.print( toString() );
    }

    /** Prints this mutable string to a {@link PrintStream} and then terminates the line.
     * @param s a {@link PrintStream}.
     */
    final public void println( final PrintStream s ) {
		print( s );
		s.println();
    }

    /** Writes this mutable string in UTF-8 encoding.
     *
     * <P>The string is coded <em>in UTF-8</em>, not in
     * the {@linkplain DataOutput#writeUTF(String) Java modified UTF
     * representation}. Thus, an ASCII NUL is represented by a single zero. Watch out!
     *
     * <P>This method does not try to do any caching (in particular, it does
     * not create any object). On non-buffered data outputs it might be very
     * slow.
     *
     * @param s a data output.
	 * @throws IOException if <code>s</code> does.
     */


    final public void writeUTF8( final DataOutput s ) throws IOException {
		writeUTF8( s, length() );
    }

    /** Writes this mutable string in UTF-8 encoding.
     *
     * <P>This method is not particularly efficient; in particular, it does not
     * do any buffering (it will call {@link DataOutput#write(int)} for each
     * byte). Have a look at {@link java.nio.charset.Charset} for more efficient ways of
     * encoding strings.
     *
     * @param s a data output.
     * @param length the length of this string (i.e., {@link #length()}).
	 * @throws IOException if <code>s</code> does.
     */
    private void writeUTF8( final DataOutput s, final int length ) throws IOException {
		final char[] a = array;
		char c;

		for ( int i = 0; i < length; i++ ) {
			c = a[ i ];
			if ( c <= 127 ) s.write( c ); // ASCII
			else if ( c >= 0x800 ) {
				s.write( (byte) ( 0xE0 | ( ( c >> 12 ) & 0x0F ) ) );
				s.write( (byte) ( 0x80 | ( ( c >> 6 ) & 0x3F ) ) );
				s.write( (byte) ( 0x80 | ( ( c >> 0 ) & 0x3F ) ) );
			}
			else {
				s.write( (byte) ( 0xC0 | ( ( c >> 6 ) & 0x1F ) ) );
				s.write( (byte) ( 0x80 | ( ( c >> 0 ) & 0x3F ) ) );
			}
		}
    }

    /** Reads a mutable string in UTF-8 encoding.
     *
     * <P>This method does not try to do any read-ahead (in particular, it does
     * not create any object). On non-buffered data inputs it might be very
     * slow.
     *
     * <P>This method is able to read only strings containing UTF-8 sequences
     * of at most 3 bytes. Longer sequences will cause a {@link UTFDataFormatException}.
     *
     * <P>If <code>length</code> is smaller than the current capacity,
     * the string will become loose.
     *
     * @param s a data input.
     * @param length the number of characters to read.
     * @return this mutable string.
	 * @throws UTFDataFormatException on UTF-8 sequences longer than three octects.
	 * @throws IOException if <code>s</code> does.
     */

    final public MutableString readUTF8( final DataInput s, final int length ) throws IOException {
		final boolean compact = hashLength < 0;
		expand( length );
		final char[] a = array;

		int b, c, d;

		for( int i = 0; i < length; i++ ) {
			b = s.readByte() & 0xFF;
			switch ( b >> 4 ) {
			case 0: 
			case 1:
			case 2: 
			case 3: 
			case 4: 
			case 5: 
			case 6: 
			case 7:
				a[ i ] = (char)b; // ASCII
				break;
			case 12: 
			case 13:
				c = s.readByte() & 0xFF;
				if ( ( c & 0xC0 ) != 0x80 ) throw new UTFDataFormatException(); 
				a[ i ] = (char)( ( ( b & 0x1F) << 6 ) | ( c & 0x3F ) );
				break;
			case 14:
				c = s.readByte() & 0xFF;
				d = s.readByte() & 0xFF;
				if ( ( c & 0xC0 ) != 0x80 || ( d & 0xC0 ) != 0x80 ) throw new UTFDataFormatException(); 
				a[ i ] = (char)( ( ( b & 0x0F ) << 12) | ( ( c & 0x3F ) << 6 ) | ( ( d & 0x3F ) << 0 ) );
				break;
			default:
				throw new UTFDataFormatException();		  
			}
		}

		// If the string was compact and we can make it again compact, we do it.
		hashLength = compact && length == a.length ? -1 : length;
		return this;
    }


    /** Writes this mutable string to a {@link DataOutput} as a 
     * length followed by a UTF-8 encoding.
     *
     * <P>The purpose of this method and of {@link
     * #readSelfDelimUTF8(DataInput)} is to provide a simple, ready-to-use
     * (even if not particularly efficient) method for storing arbitrary
     * mutable strings in a self-delimiting way.
     * 
     * <P>You can save <em>any</em> mutable string using this method. The length
	 * will be written in packed 7-bit format, that is, as a list of blocks
	 * of 7 bits (from higher to lower) in which the seventh bit determines
	 * whether there is another block in the list. For strings shorter than
	 * 2<sup>7</sup> characters, the length will be packed in one byte, for strings
	 * shorter than 2<sup>14</sup> in 2 bytes and so on.
     *
     * <P>The string following the length is coded <em>in UTF-8</em>, not in
     * the {@linkplain DataOutput#writeUTF(String) Java modified UTF
     * representation}. Thus, an ASCII NUL is represented by a single zero. Watch out!
     *
     * @param s a data output.
     * @see #writeUTF8(DataOutput)
	 * @throws IOException if <code>s</code> does.
     */


    final public void writeSelfDelimUTF8( final DataOutput s ) throws IOException {
		int length = length();
		if ( length < 1 << 7 ) s.writeByte( length );
		else if ( length < 1 << 14 ) {
			s.writeByte( length >>> 7 & 0x7F | 0x80 );
			s.writeByte( length & 0x7F );
		}
		else if ( length < 1 << 21 ) {
			s.writeByte( length >>> 14 & 0x7F | 0x80 );
			s.writeByte( length >>> 7 & 0x7F | 0x80 );
			s.writeByte( length & 0x7F );
		}
		else if ( length < 1 << 28 ) {
			s.writeByte( length >>> 21 & 0x7F | 0x80 );
			s.writeByte( length >>> 14 & 0x7F | 0x80 );
			s.writeByte( length >>> 7 & 0x7F | 0x80 );
			s.writeByte( length & 0x7F );
		}
		else {
			s.writeByte( length >>> 28 & 0x7F | 0x80 );
			s.writeByte( length >>> 21 & 0x7F | 0x80 );
			s.writeByte( length >>> 14 & 0x7F | 0x80 );
			s.writeByte( length >>> 7 & 0x7F | 0x80 );
			s.writeByte( length & 0x7F );
		}

		writeUTF8( s, length );
    }

    /** Reads a mutable string that has been written by {@link #writeSelfDelimUTF8(DataOutput) writeSelfDelimUTF8()}
     * from a {@link DataInput}.
     *
     * @param s a data input.
     * @see #readUTF8(DataInput,int)
     * @return this mutable string.
	 * @throws UTFDataFormatException on UTF-8 sequences longer than three octects.
	 * @throws IOException if <code>s</code> does.
     */

    final public MutableString readSelfDelimUTF8( final DataInput s ) throws IOException {
		int length = 0, b;

		while( ( b = s.readByte() ) < 0 ) {
			length |= b & 0x7F;
			length <<= 7; 
		}
		length |= b;

		readUTF8( s, length );
		return this;
    }

    /** Writes this mutable string in UTF-8 encoding.
     * @param s an output stream.
	 * @throws IOException if <code>s</code> does.
	 * @see #writeUTF8(DataOutput)
    */


   final public void writeUTF8( final OutputStream s ) throws IOException {
		writeUTF8( s, length() );
   }

   /** Writes this mutable string in UTF-8 encoding.
    *
    * @param s an output stream.
    * @param length the length of this string (i.e., {@link #length()}).
	* @throws IOException if <code>s</code> does.
	* @see #writeUTF8(DataOutput, int)
    */
   private void writeUTF8( final OutputStream s, final int length ) throws IOException {
		final char[] a = array;
		char c;

		for ( int i = 0; i < length; i++ ) {
			c = a[ i ];
			if ( c <= 127 ) s.write( c ); // ASCII
			else if ( c >= 0x800 ) {
				s.write( (byte) ( 0xE0 | ( ( c >> 12 ) & 0x0F ) ) );
				s.write( (byte) ( 0x80 | ( ( c >> 6 ) & 0x3F ) ) );
				s.write( (byte) ( 0x80 | ( ( c >> 0 ) & 0x3F ) ) );
			}
			else {
				s.write( (byte) ( 0xC0 | ( ( c >> 6 ) & 0x1F ) ) );
				s.write( (byte) ( 0x80 | ( ( c >> 0 ) & 0x3F ) ) );
			}
		}
   }

   /** Skips a string encoded by {@link #writeSelfDelimUTF8(OutputStream)}.
    *
    * @param s an input stream.
    * @throws UTFDataFormatException on UTF-8 sequences longer than three octects.
    * @throws IOException if <code>s</code> does, or we try to read beyond end-of-file.
    * @return the length of the skipped string.
    * @see #writeSelfDelimUTF8(OutputStream)
    */
   public static int skipSelfDelimUTF8( final InputStream s ) throws IOException {
		int length = 0, b, c, d;

		for(;;) {
			if ( ( b = s.read() ) < 0 ) throw new EOFException();
			if ( ( b & 0x80 ) == 0 ) break;
			length |= b & 0x7F;
			length <<= 7; 
		}
		length |= b;

		for( int i = 0; i < length; i++ ) {
			if ( ( b = s.read() ) == -1 ) throw new EOFException();
			b &= 0xFF;
			switch ( b >> 4 ) {
			case 0: 
			case 1:
			case 2: 
			case 3: 
			case 4: 
			case 5: 
			case 6: 
			case 7:
				// ASCII
				break;
			case 12: 
			case 13:
				if ( ( c = s.read() ) == -1 ) throw new EOFException();
				c &= 0xFF;
				if ( ( c & 0xC0 ) != 0x80 ) throw new UTFDataFormatException(); 
				break;
			case 14:
				if ( ( c = s.read() ) == -1 ) throw new EOFException();
				c &= 0xFF;
				if ( ( d = s.read() ) == -1 ) throw new EOFException();
				d &= 0xFF;
				if ( ( c & 0xC0 ) != 0x80 || ( d & 0xC0 ) != 0x80 ) throw new UTFDataFormatException(); 
				break;
			default:
				throw new UTFDataFormatException();		  
			}
		}
		
		return length;
   }

   /** Reads a mutable string in UTF-8 encoding.
   *
   * @param s an input stream.
   * @param length the number of characters to read.
   * @return this mutable string.
   * @throws UTFDataFormatException on UTF-8 sequences longer than three octects.
   * @throws IOException if <code>s</code> does, or we try to read beyond end-of-file.
   * @see #readUTF8(DataInput, int)
   */

  final public MutableString readUTF8( final InputStream s, final int length ) throws IOException {
		final boolean compact = hashLength < 0;
		expand( length );
		final char[] a = array;

		int b, c, d;

		for( int i = 0; i < length; i++ ) {
			if ( ( b = s.read() ) == -1 ) throw new EOFException();
			b &= 0xFF;
			switch ( b >> 4 ) {
			case 0: 
			case 1:
			case 2: 
			case 3: 
			case 4: 
			case 5: 
			case 6: 
			case 7:
				a[ i ] = (char)b; // ASCII
				break;
			case 12: 
			case 13:
				if ( ( c = s.read() ) == -1 ) throw new EOFException();
				c &= 0xFF;
				if ( ( c & 0xC0 ) != 0x80 ) throw new UTFDataFormatException(); 
				a[ i ] = (char)( ( ( b & 0x1F) << 6 ) | ( c & 0x3F ) );
				break;
			case 14:
				if ( ( c = s.read() ) == -1 ) throw new EOFException();
				c &= 0xFF;
				if ( ( d = s.read() ) == -1 ) throw new EOFException();
				d &= 0xFF;
				if ( ( c & 0xC0 ) != 0x80 || ( d & 0xC0 ) != 0x80 ) throw new UTFDataFormatException(); 
				a[ i ] = (char)( ( ( b & 0x0F ) << 12) | ( ( c & 0x3F ) << 6 ) | ( ( d & 0x3F ) << 0 ) );
				break;
			default:
				throw new UTFDataFormatException();		  
			}
		}

		// If the string was compact and we can make it again compact, we do it.
		hashLength = compact && length == a.length ? -1 : length;
		return this;
  }


   /** Writes this mutable string to an {@link OutputStream} as a 
    * length followed by a UTF-8 encoding.
    *
    * @param s an output stream.
    * @see #writeUTF8(DataOutput)
	* @throws IOException if <code>s</code> does.
	* @see #writeSelfDelimUTF8(DataOutput)
    */


   final public void writeSelfDelimUTF8( final OutputStream s ) throws IOException {
		int length = length();
		if ( length < 1 << 7 ) s.write( length );
		else if ( length < 1 << 14 ) {
			s.write( length >>> 7 & 0x7F | 0x80 );
			s.write( length & 0x7F );
		}
		else if ( length < 1 << 21 ) {
			s.write( length >>> 14 & 0x7F | 0x80 );
			s.write( length >>> 7 & 0x7F | 0x80 );
			s.write( length & 0x7F );
		}
		else if ( length < 1 << 28 ) {
			s.write( length >>> 21 & 0x7F | 0x80 );
			s.write( length >>> 14 & 0x7F | 0x80 );
			s.write( length >>> 7 & 0x7F | 0x80 );
			s.write( length & 0x7F );
		}
		else {
			s.write( length >>> 28 & 0x7F | 0x80 );
			s.write( length >>> 21 & 0x7F | 0x80 );
			s.write( length >>> 14 & 0x7F | 0x80 );
			s.write( length >>> 7 & 0x7F | 0x80 );
			s.write( length & 0x7F );
		}

		writeUTF8( s, length );
   }

   /** Reads a mutable string that has been written by {@link #writeSelfDelimUTF8(OutputStream) writeSelfDelimUTF8()}
    * from an {@link InputStream}.
    *
    * @param s an input stream.
    * @see #readUTF8(DataInput,int)
    * @return this mutable string.
	* @throws UTFDataFormatException on UTF-8 sequences longer than three octects.
	* @throws IOException if <code>s</code> does.
    * @throws IOException if <code>s</code> does, or we try to read beyond end-of-file.
    * @see #readSelfDelimUTF8(DataInput)
    */

   final public MutableString readSelfDelimUTF8( final InputStream s ) throws IOException {
		int length = 0, b;

		for(;;) {
			if ( ( b = s.read() ) < 0 ) throw new EOFException();
			if ( ( b & 0x80 ) == 0 ) break;
			length |= b & 0x7F;
			length <<= 7; 
		}
		length |= b;

		readUTF8( s, length );
		return this;
   }


    
    /** Compares this mutable string to another object.
     *
     * <P>This method will return <code>true</code> iff its argument
     * is a <code>CharSequence</code> containing the same characters of this
     * mutable string.
     *
     * <P>A potentially nasty consequence is that equality is not symmetric.
     * See the discussion in the {@linkplain MutableString class description}.
     *
     * @param o an {@link java.lang.Object}.
     * @return true if the argument is a <code>CharSequence</code>s that contains the same characters of this mutable string.
     */
    final public boolean equals( final Object o ) {
    	if ( o == null ) return false;
		if ( o instanceof MutableString ) return equals( (MutableString)o );
		if ( o instanceof String ) return equals( (String)o );
		if ( o instanceof CharSequence ) return equals( (CharSequence)o );
		return false;
    }

    /** Type-specific version of {@link #equals(Object) equals()}.
     * This version of the {@link #equals(Object)} method will be called
     * on mutable strings.
     * 
     * @param s a mutable string.
     * @return true if the two mutable strings contain the same characters.
     * @see #equals(Object)
     */
    final public boolean equals( final MutableString s ) {
		if ( s == this ) return true;
		int n = length();
		if ( n == s.length() ) {
			final char[] a1 = array, a2 = s.array;
			while( n-- != 0 ) if ( a1[ n ] != a2[ n ] ) return false;
	    
			return true;
		}
		return false;
    }

    /** Type-specific version of {@link #equals(Object) equals()}.
     *
     * This version of the {@link #equals(Object)} method will be called
     * on <code>String</code>s. It is guaranteed that it will return <code>true</code>
     * iff the mutable string and the <code>String</code> contain the same characters.
     * Thus, you can use expressions like
     * <PRE>
     * mutableString.equals( "Hello" )
     * </PRE>
     * to check against string contants.
     *
     * @param s a <code>String</code>.
     * @return true if the <code>String</code> contain the same characters of this mutable string.
     * @see #equals(Object)
     */
    final public boolean equals( final String s ) {
		int n = length();
		if ( n == s.length() ) {
			final char[] a = array;
			while( n-- != 0 ) if ( a[ n ] != s.charAt( n ) ) return false;
	    
			return true;
		}
		return false;
    }

    /** Type-specific version of {@link #equals(Object) equals()}.
     *
     * This version of the {@link #equals(Object)} method will be called
     * on character sequences. It is guaranteed that it will return <code>true</code>
     * iff this mutable string and the character sequence contain the same characters.
     *
     * @param s a character sequence.
     * @return true if the character sequence contains the same characters of this mutable string.
     * @see #equals(Object)
     */
    final public boolean equals( final CharSequence s ) {
		int n = length();
		if ( n == s.length() ) {
			final char[] a = array;
			while( n-- != 0 ) if ( a[ n ] != s.charAt( n ) ) return false;
	    
			return true;
		}
		return false;
    }

    /** Checks two mutable strings for equality ignoring case.
     *
     * @param s a mutable string.
     * @return true if the two mutable strings contain the same characters up to case.
     * @see java.lang.String#equalsIgnoreCase(String)
     */ 

    final public boolean equalsIgnoreCase( final MutableString s ) {
		if ( this == s ) return true;
		if ( s == null ) return false;

		final int n = length();
		if ( n == s.length() ) {
			final char[] a1 = array;
			final char[] a2 = s.array;
			for( int i = 0; i < n; i++ ) {
				if ( a1[ i ] != a2[ i ] 
					 && Character.toLowerCase( a1[ i ] ) != Character.toLowerCase( a2[ i ] )
					 && Character.toUpperCase( a1[ i ] ) != Character.toUpperCase( a2[ i ] ) ) 
					return false;
			}
			return true;
		}
		return false;
    }

    /** Type-specific version of {@link #equalsIgnoreCase(MutableString) equalsIgnoreCase()}.
     *
     * @param s a string.
     * @return true if the string contains the same characters of this mutable string up to case.
     * @see #equalsIgnoreCase(MutableString)
     */ 

    final public boolean equalsIgnoreCase( final String s ) {
		if ( s == null ) return false;

		final int n = length();
		if ( n == s.length() ) {
			final char[] a = array;
			char c;
			for( int i = 0; i < n; i++ ) {
				if ( a[ i ] != ( c = s.charAt( i ) )
					 && Character.toLowerCase( a[ i ] ) != Character.toLowerCase( c )
					 && Character.toUpperCase( a[ i ] ) != Character.toUpperCase( c ) ) 
					return false;
			}
			return true;
		}
		return false;
    }

    /** Type-specific version of {@link #equalsIgnoreCase(MutableString) equalsIgnoreCase()}.
     * 
     * @param s a character sequence.
     * @return true if the character sequence contains the same characters of this mutable string up to case.
     * @see #equalsIgnoreCase(MutableString)
     */ 

    final public boolean equalsIgnoreCase( final CharSequence s ) {
		if ( s == null ) return false;

		final int n = length();
		if ( n == s.length() ) {
			final char[] a = array;
			char c;
			for( int i = 0; i < n; i++ ) {
				if ( a[ i ] != ( c = s.charAt( i ) )
					 && Character.toLowerCase( a[ i ] ) != Character.toLowerCase( c )
					 && Character.toUpperCase( a[ i ] ) != Character.toUpperCase( c ) ) 
					return false;
			}
			return true;
		}
		return false;
    }


    /** Compares this mutable string to another mutable string performing a lexicographical comparison.
     *
     * @param s a mutable string.
     * @return a negative integer, zero, or a positive integer as this mutable
     * string is less than, equal to, or greater than the specified mutable
     * string.
     */

    final public int compareTo( final MutableString s ) {
		final int l1 = length();
		final int l2 = s.length();
		final int n = l1 < l2 ? l1 : l2;
		final char[] a1 = array;
		final char[] a2 = s.array;

		for( int i = 0; i < n; i++ ) if ( a1[ i ] != a2[ i ] ) return a1[ i ] - a2[ i ];

		return l1 - l2;
    }

    /** Compares this mutable string to a string performing a lexicographical comparison.
     *
     * @param s a <code>String</code>.
     * @return a negative integer, zero, or a positive integer as this mutable
     * string is less than, equal to, or greater than the specified
     * <code>String</code>.
     */ 
    final public int compareTo( final String s ) {
		final int l1 = length();
		final int l2 = s.length();
		final int n = l1 < l2 ? l1 : l2;
		final char[] a = array;

		for( int i = 0; i < n; i++ ) if ( a[ i ] != s.charAt( i ) ) return a[ i ] - s.charAt( i );

		return l1 - l2;
    }

    /** Compares this mutable string to a character sequence performing a lexicographical comparison.
    *
     * @param s a character sequence.
     * @return a negative integer, zero, or a positive integer as this mutable
     * string is less than, equal to, or greater than the specified character sequence.
     */ 
    final public int compareTo( final CharSequence s ) {
		final int l1 = length();
		final int l2 = s.length();
		final int n = l1 < l2 ? l1 : l2;
		final char[] a = array;

		for( int i = 0; i < n; i++ ) if ( a[ i ] != s.charAt( i ) ) return a[ i ] - s.charAt( i );

		return l1 - l2;
    }


    /** Compares this mutable string to another object disregarding case. If the
     * argument is a character sequence, this method performs a lexicographical comparison; otherwise,
     * it throws a <code>ClassCastException</code>.
     *
     * <P>A potentially nasty consequence is that comparisons are not symmetric.
     * See the discussion in the {@linkplain MutableString class description}.
     *
	 *
     * @param s a mutable string.
     * @return a negative integer, zero, or a positive integer as this mutable
     * string is less than, equal to, or greater than the specified mutable
     * string once case differences have been eliminated.
     * @see java.lang.String#compareToIgnoreCase(String)
     */

    final public int compareToIgnoreCase( final MutableString s ) {
		final int l1 = length();
		final int l2 = s.length();
		final int n = l1 < l2 ? l1 : l2;
		final char[] a1 = array;
		final char[] a2 = s.array;
		char c, d;

		for( int i = 0; i < n; i++ ) {
			c = Character.toLowerCase( Character.toUpperCase( a1[ i ] ) );
			d = Character.toLowerCase( Character.toUpperCase( a2[ i ] ) );
			if ( c != d ) return c - d;
		}

		return l1 - l2;
    }

    /** Type-specific version of {@link #compareToIgnoreCase(MutableString) compareToIgnoreCase()}.
     * @param s a mutable string.
     * @return a negative integer, zero, or a positive integer as this mutable
     * string is less than, equal to, or greater than the specified
     * string once case differences have been eliminated.
     * @see #compareToIgnoreCase(MutableString)
     */

    final public int compareToIgnoreCase( final String s ) {
		final int l1 = length();
		final int l2 = s.length();
		final int n = l1 < l2 ? l1 : l2;
		final char[] a = array;
		char c, d;

		for( int i = 0; i < n; i++ ) {
			c = Character.toLowerCase( Character.toUpperCase( a[ i ] ) );
			d = Character.toLowerCase( Character.toUpperCase( s.charAt( i ) ) );
			if ( c != d ) return c - d;
		}

		return l1 - l2;
    }

    /** Type-specific version of {@link #compareToIgnoreCase(MutableString) compareToIgnoreCase()}.
     * @param s a mutable string.
     * @return a negative integer, zero, or a positive integer as this mutable
     * string is less than, equal to, or greater than the specified
     * character sequence once case differences have been eliminated.
     * @see #compareToIgnoreCase(MutableString)
     */

    final public int compareToIgnoreCase( final CharSequence s ) {
		final int l1 = length();
		final int l2 = s.length();
		final int n = l1 < l2 ? l1 : l2;
		final char[] a = array;
		char c, d;

		for( int i = 0; i < n; i++ ) {
			c = Character.toLowerCase( Character.toUpperCase( a[ i ] ) );
			d = Character.toLowerCase( Character.toUpperCase( s.charAt( i ) ) );
			if ( c != d ) return c - d;
		}

		return l1 - l2;
    }

    /** Returns a hash code for this mutable string. 
     *
     * <P>The hash code of a mutable string is the same as that of a
     * <code>String</code> with the same content, but with the leftmost bit
     * set.
     *
     * <P>A compact mutable string caches its hash code, so it is
     * very efficient on data structures that check hash codes before invoking
     * {@link java.lang.Object#equals(Object) equals()}.
     *
     * @return  a hash code array for this object.
     * @see java.lang.String#hashCode()
     */
    final public int hashCode() {
		int h = hashLength;
		if ( h >= -1 ) {
			final char[] a = array;
			final int l = length();
            for ( int i = h = 0; i < l; i++ ) h = 31 * h + a[ i ];
			h |= ( 1 << 31 );
            if ( hashLength == -1 ) hashLength = h;
        }
        return h;
    }

    final public String toString() { 
		return new String( array, 0, length() ); 
    }
    
    /** Writes a mutable string in serialised form.
     *
     * <P>The serialised version of a mutable string is made of its
     * length followed by its characters (in UTF-16 format). Note that the
     * compactness state is forgotten.
     *
     * <P>Because of limitations of {@link ObjectOutputStream}, this method must
     * write one character at a time, and does not try to do any caching (in
     * particular, it does not create any object). On non-buffered data outputs
     * it might be very slow.
     *
     * @param s a data output.
     */

    private void writeObject( final ObjectOutputStream s ) throws IOException {
		s.defaultWriteObject();
		final int length = length();
		final char[] a = array;
		s.writeInt( length );
		for( int i = 0; i < length; i++ ) s.writeChar( a[ i ] );
    }

    /** Reads a mutable string in serialised form. 
     *
     * <P>Mutable strings produced by this method are always compact; this seems
     * reasonable, as stored strings are unlikely going to be changed.
     *
     * <P>Because of limitations of {@link ObjectInputStream}, this method must
     * read one character at a time, and does not try to do any read-ahead (in
     * particular, it does not create any object). On non-buffered data inputs
     * it might be very slow.
     * 
     * @param s a data input.
     */

    private void readObject( final ObjectInputStream s ) throws IOException, ClassNotFoundException {
		s.defaultReadObject();
		final int length = s.readInt();
		// The new string will be compact.
		hashLength = -1;
		expand( length );
		final char[] a = array;
		for( int i = 0; i < length; i++ ) a[ i ] = s.readChar();
    }
}
