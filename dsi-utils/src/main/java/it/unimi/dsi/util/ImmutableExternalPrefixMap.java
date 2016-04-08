package it.unimi.dsi.util;

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

import it.unimi.dsi.bits.BitVector;
import it.unimi.dsi.bits.PrefixCoderTransformationStrategy;
import it.unimi.dsi.compression.Decoder;
import it.unimi.dsi.compression.HuTuckerCodec;
import it.unimi.dsi.compression.PrefixCodec;
import it.unimi.dsi.compression.PrefixCoder;
import it.unimi.dsi.fastutil.booleans.BooleanIterator;
import it.unimi.dsi.fastutil.chars.Char2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.AbstractObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.FileLinesCollection;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.MutableString;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;
import com.martiansoftware.jsap.stringparsers.ForNameStringParser;

// TODO: implement interfaces correctly (e.g., using the iterator)

/** An immutable prefix map mostly stored in external memory.
 *
 * An {@link it.unimi.dsi.util.ImmutableExternalPrefixMap} compresses words using
 * a {@link it.unimi.dsi.compression.HuTuckerCodec} and approximates
 * intervals using an {@link it.unimi.dsi.util.ImmutableBinaryTrie} that uses the same codec.
 * 
 * <P>This class releases on a <em>dump stream</em> most of the data that
 * would be contained in the corresponding internal-memory map. 
 * More precisely, each 
 * block (with user-definable length, possibly the size of a basic disk I/O operation)
 * is filled as much as possible with strings front coded and compressed with a 
 * {@link it.unimi.dsi.compression.HuTuckerCodec}. 
 * Each block starts with the length of the first string in unary, followed by the encoding of the
 * string. Then, for each string we write in unary the length of the common prefix (in characters)
 * with the previous string, the length of the remaining suffix (in characters)
 * and finally the encoded suffix. Note that if the encoding of a string is longer than a block, the string will occupy more than one block.
 * 
 * <P>We keep track using an {@link ImmutableBinaryTrie}
 * of the strings at the start of each block: thus, we are able to retrieve the interval corresponding
 * to a given prefix by calling {@link ImmutableBinaryTrie#getApproximatedInterval(BooleanIterator) getApproximatedInterval()}
 * and scanning at most two blocks.
 * 
 * <h3>Self-contained or non-self-contained</h3>
 * 
 * <P>There are two kinds of external prefix maps: self-contained and non-self-contained.
 * In the first case, you get a serialised object that you can load at any time. The dump
 * stream is serialised with the object and expanded at each deserialisation in the Java temporary directory.
 * If you deserialise a map several times, you will get correspondingly many copies of
 * the dump stream in the temporary directory. The dump streams are deleted when the JVM
 * exits. This mechanism is not very efficient, but since this class implements several
 * interfaces it is essential that clients can make the thing work in a standard way.
 * 
 * <P>Alternatively, you can give at creation time a filename for the dump stream. 
 * The resulting non-self-contained external prefix map
 * can be serialised, but after deserialisation 
 * you need to set back the {@linkplain #setDumpStream(CharSequence) dump stream filename}
 * or even directly the {@linkplain #setDumpStream(InputBitStream) dump stream} (for instance, to 
 * an {@linkplain it.unimi.dsi.io.OutputBitStream#OutputBitStream(byte[]) output bit stream
 * wrapping a byte array where the dump stream has been loaded}). You can deserialise many
 * copies of an external prefix map, letting all copies share the same dump stream. 
 * 
 * <P>This data structure is not synchronised, and concurrent reads may cause problems
 * because of clashes in the usage of the underlying input bit stream. It would not
 * be a good idea in any case to open a new stream for each caller, as that would 
 * certainly lead to disk thrashing. 
 * 
 * <P>The {@linkplain #main(String[]) main method} of this class
 * helps in building large external prefix maps.
 * 
 * @author Sebastiano Vigna
 * @since 0.9.3
 */
public class ImmutableExternalPrefixMap extends AbstractPrefixMap implements Serializable {
	final private static boolean DEBUG = false;
	final private static boolean ASSERTS = false;

	public static final long serialVersionUID = 1L;
	
	/** The standard block size (in bytes). */
	public final static int STD_BLOCK_SIZE = 1024;
	
	/** The in-memory data structure used to approximate intervals.. */
	final protected ImmutableBinaryTrie<CharSequence> intervalApproximator;
	/** The block size of this  (in bits). */
	final protected long blockSize;
	/** A decoder used to read data from the dump stream. */
	final protected Decoder decoder;
	/** A map (given by an array) from symbols in the coder to characters. */
	final protected char[] symbol2char;
	/** A map from characters to symbols of the coder. */
	final protected Char2IntOpenHashMap char2symbol;
	/** The number of terms in this map. */
	final protected int size;
	/** The index of the first word in each block, plus an additional entry containing {@link #size}. */
	final protected int[] blockStart;
	/** An array parallel to {@link #blockStart} giving the offset in blocks in the dump file
	 * of the corresponding word in {@link #blockStart}. If there are no overflows, this will just
	 * be an initial segment of the natural numbers, but overflows cause jumps. */
	final protected int[] blockOffset;
	/** Whether this map is self-contained. */
	final protected boolean selfContained;
	/** The length in bytes of the dump stream, both for serialisation purposes and for minimal checks. */
	final private long dumpStreamLength;
	/** The filename of the temporary dump stream, or of the dump stream created by the constructor or by readObject(). */
	private transient String tempDumpStreamFilename;
	/** If true, the creation of the last <code>DumpStreamIterator</code> was not
	 * followed by a call to any get method. */
	protected transient boolean iteratorIsUsable;
	/** A reference to the dump stream. */
	protected transient InputBitStream dumpStream;
	
	/** map external map.
	 * 
	 * <P>This constructor does not assume that strings returned by <code>terms.iterator()</code>
	 * will be distinct. Thus, it can be safely used with {@link FileLinesCollection}.
	 * 
	 * @param terms an iterable whose iterator will enumerate in lexicographical order the terms for the map.
	 * @param blockSizeInBytes the block size (in bytes).
	 * @param dumpStreamFilename the name of the dump stream, or <code>null</code> for a map
	 * with an automatic dump stream.
	 */
	
	public ImmutableExternalPrefixMap( final Iterable<? extends CharSequence> terms, final int blockSizeInBytes, final CharSequence dumpStreamFilename ) throws IOException {
		this.blockSize = blockSizeInBytes * 8;
		this.selfContained = dumpStreamFilename == null;
		// First of all, we gather frequencies for all Unicode characters
		int[] frequency = new int[ Character.MAX_VALUE + 1 ]; 
		int maxWordLength = 0;
		CharSequence s;
		int count = 0;

		final MutableString prevTerm = new MutableString();

		for( Iterator<? extends CharSequence> i = terms.iterator(); i.hasNext(); ) {
			s = i.next();
			maxWordLength = Math.max( s.length(), maxWordLength );
			for( int j = s.length(); j-- != 0; ) frequency[ s.charAt( j ) ]++;
			if ( count > 0 && prevTerm.compareTo( s ) >= 0 ) throw new IllegalArgumentException( "The provided term collection is not sorted, or contains duplicates [" + prevTerm + ", " + s + "]" );
			count++;
			prevTerm.replace( s );
		}
		
		size = count;
		
		// Then, we compute the number of actually used characters
		count = 0;
		for( int i = frequency.length; i-- != 0; ) if ( frequency[ i ] != 0 ) count++;

		/* Now we remap used characters in f, building at the same time maps from 
		 * symbol to characters and from characters to symbols. */
		
		int[] packedFrequency = new int[ count ];
		symbol2char = new char[ count ];
		char2symbol = new Char2IntOpenHashMap( count );
		char2symbol.defaultReturnValue( -1 );
		
		for( int i = frequency.length, k = count; i-- != 0; ) {
			if ( frequency[ i ] != 0 ) {
				packedFrequency[ --k ] = frequency[ i ];
				symbol2char[ k ] = (char)i;
				char2symbol.put( (char)i, k );
			}
		}
		
		char2symbol.trim();
		
		// We now build the coder used to code the strings
		
		final PrefixCoder prefixCoder;
		final PrefixCodec codec;
		final BitVector[] codeWord;

		if ( packedFrequency.length != 0 ) {
			codec = new HuTuckerCodec( packedFrequency );
			prefixCoder = codec.coder();
			decoder = codec.decoder();
			codeWord = prefixCoder.codeWords();
		}
		else {
			// This handles the case of a collection without words
			codec = null;
			prefixCoder = null;
			decoder = null;
			codeWord = null;
		}
		
		packedFrequency = frequency = null;

		// We now compress all strings using the given codec mixed with front coding
		final OutputBitStream output;
		if ( selfContained ) {
			final File temp = File.createTempFile( this.getClass().getName(), ".dump" );
			temp.deleteOnExit();
			tempDumpStreamFilename = temp.toString();
			output = new OutputBitStream( temp, blockSizeInBytes );
		}
		else output = new OutputBitStream( tempDumpStreamFilename = dumpStreamFilename.toString(), blockSizeInBytes );
		
		// This array will contain the delimiting words (the ones at the start of each block)
		boolean isDelimiter;
		
		int length, prevTermLength = 0, bits;
		int prefixLength = 0, termCount = 0;
		int currBuffer = 0;
		
		final IntArrayList blockStarts = new IntArrayList();
		final IntArrayList blockOffsets = new IntArrayList();
		final ObjectArrayList<MutableString> delimiters = new ObjectArrayList<MutableString>();
		prevTerm.length( 0 );
		
		for( Iterator<?> i = terms.iterator(); i.hasNext(); ) {
			s = (CharSequence) i.next();
			length = s.length();

			isDelimiter = false;
			
			// We compute the common prefix and the number of bits that are necessary to code the next term.
			bits = 0;
			for( prefixLength = 0; prefixLength < length && prefixLength < prevTermLength && prevTerm.charAt( prefixLength ) == s.charAt( prefixLength ); prefixLength++ );
			for( int j = prefixLength; j < length; j++ ) bits += codeWord[ char2symbol.get( s.charAt( j ) ) ].size();
			
			//if ( bits + length + 1 > blockSize ) throw new IllegalArgumentException( "The string \"" + s + "\" is too long to be encoded with block size " + blockSizeInBytes );
			
			// If the next term would overflow the block, and we are not at the start of a block, we align.
			if ( output.writtenBits() % blockSize != 0 && output.writtenBits() / blockSize != ( output.writtenBits() + ( length - prefixLength + 1 ) + ( prefixLength + 1 ) + bits - 1 ) / blockSize ) {
				// We align by writing 0es.
				if ( DEBUG ) System.err.println( "Aligning away " + ( blockSize - output.writtenBits() % blockSize ) + " bits..." );
				for( int j = (int)( blockSize - output.writtenBits() % blockSize ); j-- != 0; ) output.writeBit( 0 );
				if ( ASSERTS ) assert output.writtenBits() % blockSize == 0;
			}

			if ( output.writtenBits() % blockSize == 0 ) {
				isDelimiter = true;
				prefixLength = 0;
				blockOffsets.add( (int)( output.writtenBits() / blockSize ) );
			}
			
			// Note that delimiters do not get the prefix length, as it's 0.
			if ( ! isDelimiter ) output.writeUnary( prefixLength );
			output.writeUnary( length - prefixLength );

			// Write the next coded suffix on output.
			for( int j = prefixLength; j < length; j++ ) {
				BitVector c = codeWord[ char2symbol.get( s.charAt( j ) ) ];
				for( int k = 0; k < c.size(); k++ ) output.writeBit( c.getBoolean( k ) );
			}
			
			if ( isDelimiter ) {
				if ( DEBUG ) System.err.println( "First string of block " + blockStarts.size() + ": " + termCount + " (" + s + ")" );
				// The current word starts a new block
				blockStarts.add( termCount );
				// We do not want to rely on s being immutable.
				delimiters.add( new MutableString( s ) );
			}
			
			currBuffer = 1 - currBuffer;
			prevTerm.replace( s );
			prevTermLength = length;
			termCount++;
		}
		
		output.align();
		dumpStreamLength = output.writtenBits() / 8;
		output.close();
		
		intervalApproximator = prefixCoder == null ? null : new ImmutableBinaryTrie<CharSequence>( delimiters, new PrefixCoderTransformationStrategy( prefixCoder, char2symbol, false ) );

		blockStarts.add( size );
		blockStart = blockStarts.toIntArray();
		blockOffset = blockOffsets.toIntArray();
		
		// We use a buffer of the same size of a block, hoping in fast I/O. */
		dumpStream = new InputBitStream( tempDumpStreamFilename, blockSizeInBytes );
	}

	/** Creates an external map with block size {@link #STD_BLOCK_SIZE} and specified dump stream.
	 * 
	 * <P>This constructor does not assume that strings returned by <code>terms.iterator()</code>
	 * will be distinct. Thus, it can be safely used with {@link FileLinesCollection}.
	 * 
	 * @param terms a collection whose iterator will enumerate in lexicographical order the terms for the map.
	 * @param dumpStreamFilename the name of the dump stream, or <code>null</code> for a map
	 * with an automatic dump stream.
	 */
	
	public ImmutableExternalPrefixMap( final Iterable<? extends CharSequence> terms, final CharSequence dumpStreamFilename ) throws IOException {
		this( terms, STD_BLOCK_SIZE, dumpStreamFilename );
	}

	/** Creates an external map with specified block size.
	 * 
	 * <P>This constructor does not assume that strings returned by <code>terms.iterator()</code>
	 * will be distinct. Thus, it can be safely used with {@link FileLinesCollection}.
	 * 
	 * @param blockSizeInBytes the block size (in bytes).
	 * @param terms a collection whose iterator will enumerate in lexicographical order the terms for the map.
	 */
	
	public ImmutableExternalPrefixMap( final Iterable<? extends CharSequence> terms, final int blockSizeInBytes ) throws IOException {
		this( terms, blockSizeInBytes, null );
	}
	
	/** Creates an external prefix map with block size {@link #STD_BLOCK_SIZE}.
	 * 
	 * <P>This constructor does not assume that strings returned by <code>terms.iterator()</code>
	 * will be distinct. Thus, it can be safely used with {@link FileLinesCollection}.
	 * 
	 * @param terms a collection whose iterator will enumerate in lexicographical order the terms for the map.
	 */
	
	public ImmutableExternalPrefixMap( final Iterable<? extends CharSequence> terms ) throws IOException {
		this( terms, null );
	}

	private void safelyCloseDumpStream() {
		try {
			if ( this.dumpStream != null ) this.dumpStream.close();
		} 
		catch ( IOException ignore ) {}
	}
	
	private void ensureNotSelfContained() {
		if ( selfContained ) throw new IllegalStateException( "You cannot set the dump file of a self-contained external prefix map" );
	}
	
	private boolean isEncodable( final CharSequence s ) {
		for( int i = s.length(); i-- != 0; ) if ( ! char2symbol.containsKey( s.charAt( i ) ) ) return false;
		return true;
	}

	
	
	/** Sets the dump stream of this external prefix map to a given filename.
	 *
	 * <P>This method sets the dump file used by this map, and should be only
	 * called after deserialisation, providing exactly the file generated at
	 * creation time. Essentially anything can happen if you do not follow the rules.
	 *
	 * <P>Note that this method will attempt to close the old stream, if present.
	 *   
	 * @param dumpStreamFilename the name of the dump file.
	 * @see #setDumpStream(InputBitStream)
	 */
	
	public void setDumpStream( final CharSequence dumpStreamFilename ) throws FileNotFoundException{
		ensureNotSelfContained();
		safelyCloseDumpStream();
		iteratorIsUsable = false;
		final long newLength = new File( dumpStreamFilename.toString() ).length();
		if ( newLength != dumpStreamLength )
			throw new IllegalArgumentException( "The size of the new dump file (" + newLength + ") does not match the original length (" + dumpStreamLength + ")" );
		dumpStream = new InputBitStream( dumpStreamFilename.toString(), (int)( blockSize / 8 ) );
	}

	
	/** Sets the dump stream of this external prefix map to a given input bit stream.
	 *
	 * <P>This method sets the dump file used by this map, and should be only
	 * called after deserialisation, providing a repositionable stream containing
	 * exactly the file generated at
	 * creation time. Essentially anything can happen if you do not follow the rules.
	 *  
	 * <P>Using this method you can load an external prefix map in core memory, enjoying
	 * the compactness of the data structure, but getting much more speed. 
	 * 
	 * <P>Note that this method will attemp to close the old stream, if present.
	 *   
	 * @param dumpStream a repositionable input bit stream containing exactly the dump stream generated
	 * at creation time.
	 * @see #setDumpStream(CharSequence)
	 */
	public void setDumpStream( final InputBitStream dumpStream ) {
		ensureNotSelfContained();
		safelyCloseDumpStream();
		iteratorIsUsable = false;
		this.dumpStream = dumpStream;
	}

	private void ensureStream() {
		if ( dumpStream == null ) throw new IllegalStateException( "This external prefix map has been deserialised, but no dump stream has been set" );
	}
	
	public Interval getInterval( final CharSequence prefix ) {
		ensureStream();
		// If prefix contains any character not coded by the prefix coder, we can return the empty interval.
		if ( ! isEncodable( prefix ) ) return Intervals.EMPTY_INTERVAL;

		// We recover the left extremes of the intervals where extensions of prefix could possibly lie.
		Interval interval = intervalApproximator.getApproximatedInterval( prefix );
		// System.err.println( "Approximate interval: " + interval + " , terms: [" + blockStart[ interval.left ] + ", " + blockStart[ interval.right ] + "]" );

		if ( interval == Intervals.EMPTY_INTERVAL ) return interval;
		try {
			dumpStream.position( blockOffset[ interval.left ] * blockSize );
			dumpStream.readBits( 0 );
			iteratorIsUsable = false;
			MutableString s = new MutableString();
			int suffixLength, prefixLength = -1, count = blockStart[ interval.left ], blockEnd = blockStart[ interval.left + 1 ], start = -1, end = -1;

			/* We scan the dump file, stopping if we exhaust the block */
			while( count < blockEnd ) {
				if ( prefixLength < 0 ) prefixLength = 0;
				else prefixLength = dumpStream.readUnary();
				suffixLength = dumpStream.readUnary();
				s.delete( prefixLength, s.length() );
				s.length( prefixLength + suffixLength );
				for( int i = 0; i < suffixLength; i++ ) s.charAt( i + prefixLength, symbol2char[ decoder.decode( dumpStream ) ] );
				if ( s.startsWith( prefix ) ) {
					start = count;
					break; 
				}
				count++;
			}
			
			/* If we did not find our string, there are two possibilities: if the
			 * interval contains one point, there is no string extending prefix. But
			 * if  the interval  is larger, the first string of the second block in the
			 * interval must be an extension of prefix. */
			if ( start < 0 && interval.length() == 1 ) return Intervals.EMPTY_INTERVAL;
			else start = count;
			
			end = start + 1;
			//assert dumpStream.readBits() <= blockSize;

			/* If the interval contains more than one point, the last string with
			 * given prefix is necessarily contained in the last block, and we
			 * must restart the search process. */
			if ( interval.length() > 1  ) {
				dumpStream.position( blockOffset[ interval.right ] * blockSize );
				dumpStream.readBits( 0 );
				s.length( 0 );
				end = blockStart[ interval.right ];
				blockEnd = blockStart[ interval.right + 1 ];
				prefixLength = -1;
			}
			
			
			while( end < blockEnd ) {
				if ( prefixLength < 0 ) prefixLength = 0;
				else prefixLength = dumpStream.readUnary();
				suffixLength = dumpStream.readUnary();
				s.delete( prefixLength, s.length() );
				s.length( prefixLength + suffixLength );
				for( int i = 0; i < suffixLength; i++ ) s.charAt( i + prefixLength, symbol2char[ decoder.decode( dumpStream ) ] );
				if ( ! s.startsWith( prefix ) ) break;
				end++;
			}
			
			return Interval.valueOf( start, end - 1 );
		} catch (IOException rethrow ) {
			throw new RuntimeException( rethrow );
		}
		
	}
	
	protected MutableString getTerm( final int index, final MutableString s ) {
		ensureStream();
		// We perform a binary search to find the  block to which s could possibly belong.
		int block = Arrays.binarySearch( blockStart, index );
		if ( block < 0 ) block = - block - 2;

		try {
			dumpStream.position( blockOffset[ block ] * blockSize );
			dumpStream.readBits( 0 );
			iteratorIsUsable = false;
			int suffixLength, prefixLength = -1;

			for( int i = index - blockStart[ block ] + 1; i-- != 0; ) { 
				if ( prefixLength < 0 ) prefixLength = 0;
				else prefixLength = dumpStream.readUnary();
				suffixLength = dumpStream.readUnary();
				s.delete( prefixLength, s.length() );
				s.length( prefixLength + suffixLength );
				for( int j = 0; j < suffixLength; j++ ) s.charAt( j + prefixLength, symbol2char[ decoder.decode( dumpStream ) ] );
			}
			
			return s;
		}
		catch( IOException rethrow ) {
			throw new RuntimeException( rethrow );
		}
	}

	private long getIndex( final Object o ) {
		final CharSequence term = (CharSequence)o;
		ensureStream();
		// If term contains any character not coded by the prefix coder, we can return -1
		if ( ! isEncodable( term ) ) return -1;

		/* If term is in the map, any string extending term must follow term. Thus,
		 * term can be in the map only if it can be found in the left block
		 * of an approximated interval for itself. */
		Interval interval = intervalApproximator.getApproximatedInterval( term );
		if ( interval == Intervals.EMPTY_INTERVAL ) return -1;
		try {
			dumpStream.position( blockOffset[ interval.left ] * blockSize );
			dumpStream.readBits( 0 );
			iteratorIsUsable = false;
			MutableString s = new MutableString();
			int suffixLength, prefixLength = -1, count = blockStart[ interval.left ], blockEnd = blockStart[ interval.left + 1 ];

			/* We scan the dump file, stopping if we exhaust the block */
			while( count < blockEnd ) {
				if ( prefixLength < 0 ) prefixLength = 0;
				else prefixLength = dumpStream.readUnary();
				suffixLength = dumpStream.readUnary();
				s.delete( prefixLength, s.length() );
				s.length( prefixLength + suffixLength );
				for( int i = 0; i < suffixLength; i++ ) s.charAt( i + prefixLength, symbol2char[ decoder.decode( dumpStream ) ] );
				if ( s.equals( term ) ) return count;
				count++;
			}
			
			return -1;
		}
		catch (IOException rethrow ) {
			throw new RuntimeException( rethrow );
		}
	}
	

	public boolean containsKey( final Object term ) {
		return getIndex( term ) != -1;
	}
	
	public long getLong( final Object o ) {
		final long result = getIndex( o );
		return result == -1 ? defRetValue : result;
	}

	/** An iterator over the dump stream. It does not use the interval approximator&mdash;it just scans the file. */
	
	private final class DumpStreamIterator extends AbstractObjectIterator<CharSequence> {
		/** The current block being enumerated. */
		private int currBlock = -1;
		/** The index of next term that will be returned. */
		private int index;
		/** The mutable string used to return the result. */
		final MutableString s = new MutableString();

		private DumpStreamIterator() {
			try {
				dumpStream.position( 0 );
			}
			catch ( IOException e ) {
				throw new RuntimeException( e );
			}
			dumpStream.readBits( 0 );
			iteratorIsUsable = true;
		}
		
		public boolean hasNext() {
			if ( ! iteratorIsUsable ) throw new IllegalStateException( "Get methods of this map have caused a stream repositioning" );
			return index < size;
		}

		public CharSequence next() {
			if ( ! hasNext() ) throw new NoSuchElementException();
			try {
				final int prefixLength;
				if ( index == blockStart[ currBlock + 1 ] ) {
					if ( dumpStream.readBits() % blockSize != 0 ) dumpStream.skip( blockSize - dumpStream.readBits() % blockSize );
					currBlock++;
					prefixLength = 0;
				}
				else prefixLength = dumpStream.readUnary();
				final int suffixLength = dumpStream.readUnary();
				s.delete( prefixLength, s.length() );
				s.length( prefixLength + suffixLength );
				for ( int i = 0; i < suffixLength; i++ )
					s.charAt( i + prefixLength, symbol2char[ decoder.decode( dumpStream ) ] );
				index++;
				return s;
			}
			catch ( IOException e ) {
				throw new RuntimeException( e );
			}
		}

	}
	
	/** Returns an iterator over the map.
	 * 
	 * <P>The iterator returned by this method scans directly the dump stream. 
	 * 
	 * <P>Note that the returned iterator uses <em>the same stream</em> as all get methods. Calling such methods while
	 * the iterator is being used will produce an {@link IllegalStateException}.
	 * 
	 * @return an iterator over the map that just scans the dump stream.
	 */
	
	public ObjectIterator<CharSequence> iterator() {
		return new DumpStreamIterator();
	}
	
	public int size() {
		return size;
	}

	private void writeObject( final ObjectOutputStream s ) throws IOException {
		s.defaultWriteObject();
		if ( selfContained ) {
			final FileInputStream fis = new FileInputStream( tempDumpStreamFilename );
			IOUtils.copy( fis, s );
			fis.close();
		}
	}

	private void readObject( final ObjectInputStream s ) throws IOException, ClassNotFoundException {
		s.defaultReadObject();
		if ( selfContained ) {
			final File temp = File.createTempFile( this.getClass().getName(), ".dump" );
			temp.deleteOnExit();
			tempDumpStreamFilename = temp.toString();
			// TODO: propose Jakarta CopyUtils extension with length control and refactor.
			FileOutputStream fos = new FileOutputStream( temp );
			final byte[] b = new byte[ 64 * 1024 ];
			int len;
			while( ( len = s.read( b ) ) >= 0 ) fos.write( b, 0, len );			fos.close();
			dumpStream = new InputBitStream( temp, (int)( blockSize / 8 ) );
		}
	}

	@SuppressWarnings("unchecked")
	public static void main( final String[] arg ) throws ClassNotFoundException, IOException, JSAPException, SecurityException, NoSuchMethodException {

		final SimpleJSAP jsap = new SimpleJSAP( ImmutableExternalPrefixMap.class.getName(), "Builds an external map reading from standard input a newline-separated list of terms or a serialised term list. If the dump stream name is not specified, the map will be self-contained.", 
				new Parameter[] {
					new FlaggedOption( "blockSize", JSAP.INTSIZE_PARSER, ( STD_BLOCK_SIZE / 1024 ) + "Ki", JSAP.NOT_REQUIRED, 'b', "block-size", "The size of a block in the dump stream." ),
					new Switch( "serialised", 's', "serialised", "The data source (file or standard input) provides a serialised java.util.List of terms." ),
					new Switch( "zipped", 'z', "zipped", "Standard input is compressed in gzip format." ),
					new FlaggedOption( "termFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'o', "offline", "Read terms from this file instead of standard input." ),					
					new FlaggedOption( "encoding", ForNameStringParser.getParser( Charset.class ), "UTF-8", JSAP.NOT_REQUIRED, 'e', "encoding", "The term list encoding." ),
					new UnflaggedOption( "map", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename for the serialised map." ),
					new UnflaggedOption( "dump", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "An optional dump stream (the resulting map will not be self-contained)." )
			}
		);

		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;
		
		Collection<? extends CharSequence> termList;
		
		final String termFile = jsapResult.getString( "termFile" );
		final Charset encoding = (Charset)jsapResult.getObject( "encoding" );
		final boolean zipped = jsapResult.getBoolean( "zipped" );
		final boolean serialised = jsapResult.getBoolean( "serialised" );

		if ( zipped && serialised ) throw new IllegalArgumentException( "The zipped and serialised options are incompatible" );

		if ( serialised ) termList = (List<? extends CharSequence>) ( termFile != null ? BinIO.loadObject( termFile ) : BinIO.loadObject( System.in ) );
		else {
			if ( termFile != null ) termList = new FileLinesCollection( termFile, encoding.name(), zipped );
			else {
				final ObjectArrayList<MutableString> list = new ObjectArrayList<MutableString>();
				termList = list;
				final FastBufferedReader terms = new FastBufferedReader( new InputStreamReader( 
						zipped ? new GZIPInputStream( System.in ) : System.in, encoding.name() ) );
				final MutableString term = new MutableString();
				while( terms.readLine( term ) != null ) list.add( term.copy() );
				terms.close();
			}
		}

		BinIO.storeObject( new ImmutableExternalPrefixMap( termList, jsapResult.getInt( "blockSize" ), jsapResult.getString( "dump" ) ), jsapResult.getString( "map" ) );
	}
}
