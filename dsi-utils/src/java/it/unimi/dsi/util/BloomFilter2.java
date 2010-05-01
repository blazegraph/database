package it.unimi.dsi.util;

/*       
 * MG4J: Managing Gigabytes for Java
 *
 * Copyright (C) 2004-2006 Sebastiano Vigna 
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

import it.unimi.dsi.bits.Fast;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Random;

/**
 * A Bloom filter derived directly from
 * <code>it.unimi.dsi.mg4j.util.BloomFilter</code> in the mg4j project. The
 * primary changes are:
 * <ul>
 * <li>Removed use of the MersenneTwister random number generator to remove a
 * dependency on the Colt project.</li>
 * <li>Removed the main() routine, including all of the various dependencies
 * that it was dragging in.</li>
 * <li>Removed the use of LongArrays from the <code>fastutil</code> project.
 * The code uses {@link Arrays#fill(long[], long)} instead to clear the state on
 * reset.</li>
 * <li>Custom serialization (not compatible, new serialization UUID).</li>
 * <li>Made various public fields private since they can not be final with an
 * {@link Externalizable} implementation and added access methods for their
 * data.</li>
 * <li>Changed the class name to avoid possible class path confusions. </li>
 * </ul>
 * 
 * <P>
 * Instances of this class represent a set of character sequences or
 * primitive-type arrays (with false positives) using a Bloom filter. Because of
 * the way Bloom filters work, you cannot remove elements.
 * 
 * <p>
 * The intended usage is that character sequences and arrays should <em>not</em>
 * be mixed (albeit in principle it could work). A Bloom filter is rather
 * agnostic with respect to the data it contains&mdash;all it needs is a
 * sequence of hash functions that can be applied to the data.
 * 
 * <P>
 * Bloom filters have an expected error rate, depending on the number of hash
 * functions used, on the filter size and on the number of elements in the
 * filter. This implementation uses a variable optimal number of hash functions,
 * depending on the expected number of elements. More precisely, a Bloom filter
 * for <var>n</var> character sequences with <var>d</var> hash functions will
 * use ln 2 <var>d</var><var>n</var> &#8776; 1.44 <var>d</var><var>n</var>
 * bits; false positives will happen with probability 2<sup>-<var>d</var></sup>.
 * The maximum number of bits supported is {@link #MAX_BITS}.
 * 
 * <P>
 * Hash functions are generated at creation time using a mix of universal
 * hashing and shift-add-xor hashing (for the latter, see <i>Performance in
 * practice of string hashing functions</i>, by M.V. Ramakrishna and Justin
 * Zobel, <i>Proc. of the Fifth International Conference on Database Systems for
 * Advanced Applications</i>, 1997, pages 215&minus;223).
 * 
 * <p>
 * Each hash function uses {@link #NUMBER_OF_WEIGHTS} random integers, which are
 * cyclically multiplied by the character codes in a character sequence. The
 * resulting integers are then summed with a shifted hash and XOR-ed together.
 * 
 * <P>
 * This class exports access methods that are similar to those of
 * {@link java.util.Set}, but it does not implement that interface, as too many
 * non-optional methods would be unimplementable (e.g., iterators).
 * 
 * <P>
 * A main method makes it easy to create serialised Bloom filters starting from
 * a list of terms.
 * 
 * @author Sebastiano Vigna, who wrote the original implementation.
 * @author thompsonbry@users.sourceforge.net, who hacked the implementation down
 *         to have fewer dependencies.
 */
public class BloomFilter2 implements Externalizable {
    /**
     * 
     */
    private static final long serialVersionUID = -880117129471136320L;

    private transient static final boolean DEBUG = false;
//    private static final long serialVersionUID = 2L;

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        final int version = in.readInt();
        if (version != VERSION0)
            throw new IOException("Unknown version=" + version);
        size = in.readInt();
        m = in.readLong();
        d = in.readInt();
        bits = new long[(int)m / Long.SIZE];
        for(int i=0; i<bits.length; i++) {
            bits[i] = in.readLong();
        }
        weight = new int[d][NUMBER_OF_WEIGHTS];
        for(int i=0; i<d; i++) {
            weight[i] = new int[NUMBER_OF_WEIGHTS];
            for(int j=0; j<NUMBER_OF_WEIGHTS; j++) {
                weight[i][j] = in.readInt();
            }
        }
        init = new int[d];
        for(int i=0; i<d; i++) {
            init[i] = in.readInt();
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        
        out.writeInt(VERSION0);
        out.writeInt(size);
        out.writeLong(m);
        out.writeInt(d);
        // m/Long.SIZE [bits]
        for(int i=0; i<bits.length; i++) {
            out.writeLong(bits[i]);
        }
        // weight[d][#weights]
        for (int i = 0; i < d; i++) {
            for (int j = 0; j < NUMBER_OF_WEIGHTS; j++) {
                out.writeInt(weight[i][j]);
            }
        }
        // init[d]
        for (int i = 0; i < d; i++) {
            out.writeInt(init[i]);
        }
        
    }
    /**
     * Initial {@link Externalizable} serialization format.
     */
    private static transient final int VERSION0 = 0x0;
    
    /** The number of elements currently in the filter. It may be
     * smaller than the actual number of additions because of false positives. */
    private int size;

    /** The maximum number of bits in a filter (limited by array size and bits in a long). */
    public transient static final long MAX_BITS = (long)Long.SIZE * Integer.MAX_VALUE;

    private transient final static long LOG2_LONG_SIZE = 6;
    private transient final static long BIT_INDEX_MASK = ( 1 << LOG2_LONG_SIZE ) - 1;

    /** The number of weights used to create hash functions. */
    final public static transient int NUMBER_OF_WEIGHTS = 16;
    /** The number of bits in this filter. */
    private long m;
    /** The number of bits in this filter. */
    final public long m() {return m;}
    /** The number of hash functions used by this filter. */
    private int d;
    /** The number of hash functions used by this filter. */
    final public int d() {return d;}
    /** The underlying bit vector. */
    private long[] bits;
    /** The random integers used to generate the hash functions. */
    private int[][] weight;
    /** The random integers used to initialise the hash functions. */
    private int[] init;

    /** The natural logarithm of 2, used in the computation of the number of bits. */
    transient private final static double NATURAL_LOG_OF_2 = Math.log( 2 );

    /**
     * De-serialization ctor.
     */
    public BloomFilter2() {
        
    }
    
    /** Creates a new high-precision Bloom filter a given expected number of elements.
     *
     * <p>This constructor uses a number of hash functions that is logarithmic in the number
     * of expected elements. This usually results in no false positives at all.
     *  
     * @param n the expected number of elements.
     */
    public BloomFilter2( final int n ) {
        this( n, Fast.mostSignificantBit( n ) + 1 );
    }
    
    /** Creates a new Bloom filter with given number of hash functions and expected number of elements.
     * 
     * @param n the expected number of elements.
     * @param d the number of hash functions; under obvious uniformity and indipendence assumptions,
     * if the filter has not more than <code>n</code> elements,
     * false positives will happen with probability 2<sup>-<var>d</var></sup>.
     */
    public BloomFilter2( final int n, final int d ) {
        this.d = d;
        final long wantedNumberOfBits = (long)Math.ceil( n * ( d / NATURAL_LOG_OF_2 ) );
        if ( wantedNumberOfBits > MAX_BITS ) throw new IllegalArgumentException( "The wanted number of bits (" + wantedNumberOfBits + ") is larger than " + MAX_BITS );
        bits = new long[ (int)( ( wantedNumberOfBits + Long.SIZE - 1 ) / Long.SIZE ) ];
        m = bits.length * (long)Long.SIZE;

        if ( DEBUG ) System.err.println( "Number of bits: " + m );
        
        // The purpose of Random().nextInt() is to generate a different seed at each invocation.
//        final MersenneTwister mersenneTwister = new MersenneTwister( new Random().nextInt() );
        final Random rand = new Random();
        weight = new int[ d ][];
        init = new int[ d ];
        for( int i = 0; i < d; i++ ) {
            weight[ i ] = new int[ NUMBER_OF_WEIGHTS ];
            init[ i ] = rand.nextInt();
            for( int j = 0; j < NUMBER_OF_WEIGHTS; j++ )
                 weight[ i ][ j ] = rand.nextInt();
        }
    }

    /** Returns the value of the bit with the specified index in the specified array.
     * 
     * <p>This method (and its companion {@link #set(long[], long)}) are static
     * so that the bit array can be cached by the caller in a local variable.
     * 
     * @param index the bit index.
     * @return the value of the bit of index <code>index</code>.
     */
    private static boolean get( long[] bits, long index ) {
        return ( bits[ (int)( index >> LOG2_LONG_SIZE ) ] & ( 1L << ( index & BIT_INDEX_MASK ) ) ) != 0;
    }

    /** Sets the bit with specified index in the specified array.
     * 
     * @param index the bit index.
     * @see #get(long[], long)
     */
    private boolean set( long[] bits, long index ) {
        final int unit = (int)( index >> LOG2_LONG_SIZE );
        final long mask = 1L << ( index & BIT_INDEX_MASK );
        final boolean result = ( bits[ unit ] & mask ) != 0;
        bits[ unit ] |= mask;
        return result;
    }

    /** Hashes the given sequence with the given hash function.
     * 
     * @param s a character sequence.
     * @param l the length of <code>s</code>.
     * @param k a hash function index (smaller than {@link #d}).
     * @return the position in the filter corresponding to <code>s</code> for the hash function <code>k</code>.
     */
    private long hash( final CharSequence s, final int l, final int k ) {
        final int[] w = weight[ k ];
        long h = init[ k ];
        int i = l;
        while( i-- != 0 ) h ^= ( h << 5 ) + s.charAt( i ) * w[ i % NUMBER_OF_WEIGHTS ] + ( h >>> 2 );
        return ( h & 0x7FFFFFFFFFFFFFFFL ) % m; 
    }

    /** Hashes the given byte array with the given hash function.
     * 
     * @param a a byte array.
     * @param l the length of <code>s</code>.
     * @param k a hash function index (smaller than {@link #d}).
     * @return the position in the filter corresponding to <code>a</code> for the hash function <code>k</code>.
     */
    private long hash( final byte[] a, final int l, final int k ) {
        final int[] w = weight[ k ];
        long h = init[ k ];
        int i = l;
        while( i-- != 0 ) h ^= ( h << 5 ) + a[ i ] * w[ i % NUMBER_OF_WEIGHTS ] + ( h >>> 2 );
        return ( h & 0x7FFFFFFFFFFFFFFFL ) % m; 
    }

    /** Hashes the given short array with the given hash function.
     * 
     * @param a a short array.
     * @param l the length of <code>s</code>.
     * @param k a hash function index (smaller than {@link #d}).
     * @return the position in the filter corresponding to <code>a</code> for the hash function <code>k</code>.
     */
    private long hash( final short[] a, final int l, final int k ) {
        final int[] w = weight[ k ];
        long h = init[ k ];
        int i = l;
        while( i-- != 0 ) h ^= ( h << 5 ) + a[ i ] * w[ i % NUMBER_OF_WEIGHTS ] + ( h >>> 2 );
        return ( h & 0x7FFFFFFFFFFFFFFFL ) % m; 
    }

    /** Hashes the given character array with the given hash function.
     * 
     * @param a a character array.
     * @param l the length of <code>s</code>.
     * @param k a hash function index (smaller than {@link #d}).
     * @return the position in the filter corresponding to <code>a</code> for the hash function <code>k</code>.
     */
    private long hash( final char[] a, final int l, final int k ) {
        final int[] w = weight[ k ];
        long h = init[ k ];
        int i = l;
        while( i-- != 0 ) h ^= ( h << 5 ) + a[ i ] * w[ i % NUMBER_OF_WEIGHTS ] + ( h >>> 2 );
        return ( h & 0x7FFFFFFFFFFFFFFFL ) % m; 
    }

    /** Hashes the given int array with the given hash function.
     * 
     * @param a an int array.
     * @param l the length of <code>s</code>.
     * @param k a hash function index (smaller than {@link #d}).
     * @return the position in the filter corresponding to the slice of <code>a</code> for the hash function <code>k</code>.
     */
    private long hash( final int[] a, final int l, final int k ) {
        final int[] w = weight[ k ];
        long h = init[ k ];
        int i = l;
        while( i-- != 0 ) h ^= ( h << 5 ) + a[ i ] * w[ i % NUMBER_OF_WEIGHTS ] + ( h >>> 2 );
        return ( h & 0x7FFFFFFFFFFFFFFFL ) % m; 
    }

    /** Hashes the given long array with the given hash function.
     * 
     * @param a a long array.
     * @param l the length of <code>s</code>.
     * @param k a hash function index (smaller than {@link #d}).
     * @return the position in the filter corresponding to <code>a</code> for the hash function <code>k</code>.
     */
    private long hash( final long[] a, final int l, final int k ) {
        final int[] w = weight[ k ];
        long h = init[ k ];
        int i = l;
        while( i-- != 0 ) h ^= ( h << 5 ) + a[ i ] * w[ i % NUMBER_OF_WEIGHTS ] + ( h >>> 2 );
        return ( h & 0x7FFFFFFFFFFFFFFFL ) % m; 
    }

    /** Hashes the given float array with the given hash function.
     * 
     * @param a a float array.
     * @param l the length of <code>s</code>.
     * @param k a hash function index (smaller than {@link #d}).
     * @return the position in the filter corresponding to <code>a</code> for the hash function <code>k</code>.
     */
    private long hash( final float[] a, final int l, final int k ) {
        final int[] w = weight[ k ];
        long h = init[ k ];
        int i = l;
        while( i-- != 0 ) h ^= ( h << 5 ) + Float.floatToRawIntBits( a[ i ] ) * w[ i % NUMBER_OF_WEIGHTS ] + ( h >>> 2 );
        return ( h & 0x7FFFFFFFFFFFFFFFL ) % m; 
    }

    /** Hashes the given double array with the given hash function.
     * 
     * @param a a double array.
     * @param l the length of <code>s</code>.
     * @param k a hash function index (smaller than {@link #d}).
     * @return the position in the filter corresponding to <code>a</code> for the hash function <code>k</code>.
     */
    private long hash( final double[] a, final int l, final int k ) {
        final int[] w = weight[ k ];
        long h = init[ k ];
        int i = l;
        while( i-- != 0 ) h ^= ( h << 5 ) + Double.doubleToRawLongBits( a[ i ] ) * w[ i % NUMBER_OF_WEIGHTS ] + ( h >>> 2 );
        return ( h & 0x7FFFFFFFFFFFFFFFL ) % m; 
    }


    /** Checks whether the given character sequence is in this filter. 
     * 
     * <P>Note that this method may return true on a character sequence that has
     * not been added to the filter. This will happen with probability 2<sup>-<var>d</var></sup>,
     * where <var>d</var> is the number of hash functions specified at creation time, if
     * the number of the elements in the filter is less than <var>n</var>, the number
     * of expected elements specified at creation time.
     * 
     * @param s a character sequence.
     * @return true if <code>s</code> (or some element
     * with the same hash sequence as <code>s</code>) is in the filter.
     */

    public boolean contains( final CharSequence s ) {
        int i = d, l = s.length();
        long bits[] = this.bits;
        while( i-- != 0 ) if ( ! get( bits, hash( s, l, i ) ) ) return false;
        return true;
    }

    /** Checks whether the given byte array is in this filter. 
     * 
     * @param a a byte array.
     * @return true if <code>a</code> (or some element
     * with the same hash sequence as <code>a</code>) is in the filter.
     * @see #contains(CharSequence)
     */

    public boolean contains( final byte[] a ) {
        int i = d, l = a.length;
        long bits[] = this.bits;
        while( i-- != 0 ) if ( ! get( bits, hash( a, l, i ) ) ) return false;
        return true;
    }

    /** Checks whether the given short array is in this filter. 
     * 
     * @param a a short array.
     * @return true if <code>a</code> (or some element
     * with the same hash sequence as <code>a</code>) is in the filter.
     * @see #contains(CharSequence)
     */

    public boolean contains( final short[] a ) {
        int i = d, l = a.length;
        long bits[] = this.bits;
        while( i-- != 0 ) if ( ! get( bits, hash( a, l, i ) ) ) return false;
        return true;
    }

    /** Checks whether the given character array is in this filter. 
     * 
     * @param a a character array.
     * @return true if <code>a</code> (or some element
     * with the same hash sequence as <code>a</code>) is in the filter.
     * @see #contains(CharSequence)
     */

    public boolean contains( final char[] a ) {
        int i = d, l = a.length;
        long bits[] = this.bits;
        while( i-- != 0 ) if ( ! get( bits, hash( a, l, i ) ) ) return false;
        return true;
    }

    /** Checks whether the given int array is in this filter. 
     * 
     * @param a an int array.
     * @return true if <code>a</code> (or some element
     * with the same hash sequence as <code>a</code>) is in the filter.
     * @see #contains(CharSequence)
     */

    public boolean contains( final int[] a ) {
        int i = d, l = a.length;
        long bits[] = this.bits;
        while( i-- != 0 ) if ( ! get( bits, hash( a, l, i ) ) ) return false;
        return true;
    }


    /** Checks whether the given long array is in this filter. 
     * 
     * @param a a long array.
     * @return true if <code>a</code> (or some element
     * with the same hash sequence as <code>a</code>) is in the filter.
     * @see #contains(CharSequence)
     */

    public boolean contains( final long[] a ) {
        int i = d, l = a.length;
        long bits[] = this.bits;
        while( i-- != 0 ) if ( ! get( bits, hash( a, l, i ) ) ) return false;
        return true;
    }

    /** Checks whether the given float array is in this filter. 
     * 
     * @param a a float array.
     * @return true if <code>a</code> (or some element
     * with the same hash sequence as <code>a</code>) is in the filter.
     * @see #contains(CharSequence)
     */

    public boolean contains( final float[] a ) {
        int i = d, l = a.length;
        long bits[] = this.bits;
        while( i-- != 0 ) if ( ! get( bits, hash( a, l, i ) ) ) return false;
        return true;
    }

    /** Checks whether the given double array is in this filter. 
     * 
     * @param a a double array.
     * @return true if <code>a</code> (or some element
     * with the same hash sequence as <code>a</code>) is in the filter.
     * @see #contains(CharSequence)
     */

    public boolean contains( final double[] a ) {
        int i = d, l = a.length;
        long bits[] = this.bits;
        while( i-- != 0 ) if ( ! get( bits, hash( a, l, i ) ) ) return false;
        return true;
    }

    /** Adds a character sequence to the filter.
     * 
     * @param s a character sequence.
     * @return true if this filter was modified (i.e., neither <code>s</code> nor any
     * other element with the same hash sequence as <code>s</code> was already in this filter).
     */

    public boolean add( final CharSequence s ) {
        int i = d, l = s.length();
        long bits[] = this.bits;
        boolean alreadySet = true;
        while( i-- != 0 ) alreadySet &= set( bits, hash( s, l, i ) );
        if ( ! alreadySet ) size++;
        return ! alreadySet;
    }
    
    /** Adds a byte array to the filter.
     * 
     * @param a a byte array.
     * @return true if this filter was modified (i.e., neither <code>a</code> nor any
     * other element with the same hash sequence as <code>a</code> was already in this filter).
     */

    public boolean add( final byte[] a ) {
        int i = d, l = a.length;
        long bits[] = this.bits;
        boolean alreadySet = true;
        while( i-- != 0 ) alreadySet &= set( bits, hash( a, l, i ) );
        if ( ! alreadySet ) size++;
        return ! alreadySet;
    }

    /** Adds a short array to the filter.
     * 
     * @param a a short array.
     * @return true if this filter was modified (i.e., neither <code>a</code> nor any
     * other element with the same hash sequence as <code>a</code> was already in this filter).
     */

    public boolean add( final short[] a ) {
        int i = d, l = a.length;
        long bits[] = this.bits;
        boolean alreadySet = true;
        while( i-- != 0 ) alreadySet &= set( bits, hash( a, l, i ) );
        if ( ! alreadySet ) size++;
        return ! alreadySet;
    }

    /** Adds a character array to the filter.
     * 
     * @param a a character array.
     * @return true if this filter was modified (i.e., neither <code>a</code> nor any
     * other element with the same hash sequence as <code>a</code> was already in this filter).
     */

    public boolean add( final char[] a ) {
        int i = d, l = a.length;
        long bits[] = this.bits;
        boolean alreadySet = true;
        while( i-- != 0 ) alreadySet &= set( bits, hash( a, l, i ) );
        if ( ! alreadySet ) size++;
        return ! alreadySet;
    }

    /** Adds an int array to the filter.
     * 
     * @param a an int array.
     * @return true if this filter was modified (i.e., neither <code>a</code> nor any
     * other element with the same hash sequence as <code>a</code> was already in this filter).
     */

    public boolean add( final int[] a ) {
        int i = d, l = a.length;
        long bits[] = this.bits;
        boolean alreadySet = true;
        while( i-- != 0 ) alreadySet &= set( bits, hash( a, l, i ) );
        if ( ! alreadySet ) size++;
        return ! alreadySet;
    }

    /** Adds a long array to the filter.
     * 
     * @param a a long array.
     * @return true if this filter was modified (i.e., neither <code>a</code> nor any
     * other element with the same hash sequence as <code>a</code> was already in this filter).
     */

    public boolean add( final long[] a ) {
        int i = d, l = a.length;
        long bits[] = this.bits;
        boolean alreadySet = true;
        while( i-- != 0 ) alreadySet &= set( bits, hash( a, l, i ) );
        if ( ! alreadySet ) size++;
        return ! alreadySet;
    }

    /** Adds a float array to the filter.
     * 
     * @param a a float array.
     * @return true if this filter was modified (i.e., neither <code>a</code> nor any
     * other element with the same hash sequence as <code>a</code> was already in this filter).
     */

    public boolean add( final float[] a ) {
        int i = d, l = a.length;
        long bits[] = this.bits;
        boolean alreadySet = true;
        while( i-- != 0 ) alreadySet &= set( bits, hash( a, l, i ) );
        if ( ! alreadySet ) size++;
        return ! alreadySet;
    }

    /** Adds a double array to the filter.
     * 
     * @param a a double array.
     * @return true if this filter was modified (i.e., neither <code>a</code> nor any
     * other element with the same hash sequence as <code>a</code> was already in this filter).
     */

    public boolean add( final double[] a ) {
        int i = d, l = a.length;
        long bits[] = this.bits;
        boolean alreadySet = true;
        while( i-- != 0 ) alreadySet &= set( bits, hash( a, l, i ) );
        if ( ! alreadySet ) size++;
        return ! alreadySet;
    }

    /** Clears this filter.
     */
    
    public void clear() {
        /*Long*/Arrays.fill( bits, 0 );
        size = 0;
    }

    /** Returns the size of this filter.
     *
     * <p>Note that the size of a Bloom filter is only a <em>lower bound</em>
     * for the number of distinct elements that have been added to the filter.
     * False positives might make the number returned by this method smaller
     * than it should be.
     * 
     * @return the size of this filter.
     */
    
    public long size() {
        return size;
    }

//    public static void main( final String[] arg ) throws IOException, JSAPException, NoSuchMethodException {
//        
//        final SimpleJSAP jsap = new SimpleJSAP( BloomFilter.class.getName(), "Creates a Bloom filter reading from standard input a newline-separated list of terms.",
//                new Parameter[] {
//                    new FlaggedOption( "bufferSize", IntSizeStringParser.getParser(), "64Ki", JSAP.NOT_REQUIRED, 'b',  "buffer-size", "The size of the I/O buffer used to read terms." ),
//                    new FlaggedOption( "encoding", ForNameStringParser.getParser( Charset.class ), "UTF-8", JSAP.NOT_REQUIRED, 'e', "encoding", "The term file encoding." ),
//                    new UnflaggedOption( "bloomFilter", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename for the serialised front-coded list." ),
//                    new UnflaggedOption( "size", JSAP.INTSIZE_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The size of the filter (i.e., the expected number of elements in the filter; usually, the number of terms)." ),
//                    new UnflaggedOption( "precision", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The precision of the filter." )
//        });
//        
//        JSAPResult jsapResult = jsap.parse( arg );
//        if ( jsap.messagePrinted() ) return;
//        
//        final int bufferSize = jsapResult.getInt( "bufferSize" );
//        final String filterName = jsapResult.getString( "bloomFilter" );
//        final Charset encoding = (Charset)jsapResult.getObject( "encoding" );
//
//        BloomFilter filter = new BloomFilter( jsapResult.getInt( "size" ), jsapResult.getInt( "precision" ) );
//        final ProgressLogger pl = new ProgressLogger();
//        pl.itemsName = "terms";
//        pl.start( "Reading terms..." );
//        MutableString s = new MutableString();
//        FastBufferedReader reader = new FastBufferedReader( new InputStreamReader( System.in, encoding ), bufferSize );
//        while( reader.readLine( s ) != null ) { 
//            filter.add( s );
//            pl.lightUpdate();
//        }
//        pl.done();
//
//        BinIO.storeObject( filter, filterName );
//    }  

}
