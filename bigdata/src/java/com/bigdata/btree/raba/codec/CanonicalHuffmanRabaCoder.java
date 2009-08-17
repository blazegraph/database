/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Aug 11, 2009
 */

package com.bigdata.btree.raba.codec;

import it.unimi.dsi.bits.BitVector;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.compression.CanonicalFast64CodeWordDecoder;
import it.unimi.dsi.compression.Decoder;
import it.unimi.dsi.compression.HuffmanCodec;
import it.unimi.dsi.compression.PrefixCoder;
import it.unimi.dsi.fastutil.bytes.Byte2IntOpenHashMap;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.io.ByteBufferInputStream;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.log4j.Logger;

import com.bigdata.btree.ILeafData;
import com.bigdata.btree.raba.IRaba;

/**
 * This class provides (de-)compression for logical byte[][]s based on canonical
 * Huffman codes. Canonical huffman codes preserve the alphabetic order of the
 * original values. However, they are often used because it is possible to
 * transmit the dictionary using fewer bits by sending only the bitLength[] for
 * the code words. The {@link CanonicalHuffmanRabaCoder}can be used for keys or
 * values. When used for keys it codes all byte values so we can search in the
 * coded space. When used for values, it codes only the byte values which
 * actually appear in the byte[][] values.
 * 
 * <h3>Record format</h3>
 * 
 * The encoded data are represented in the buffer as follows:
 * 
 * <pre>
 * version:uint6    The version identifier for this record format (6 bits,
 *                  which allows for 64 format revisions).
 * 
 * size:int31       The #of elements in the logical byte[][].
 * 
 * isKeys:1         Bit flag indicates whether the record is coding B+Tree keys
 *                  or B+Tree values.  When coding values only the non-zero
 *                  byte frequency counts are used to compute the dictionary
 *                  and nulls are permitted.  When coding keys, the dictionary
 *                  includes codes for all 256 possible byte values so we may
 *                  code search keys and nulls are not permitted.
 * 
 * bitCodedSymbols:1
 *                  A bit flag whose value is 1 iff the symbols are given as
 *                  a packed symbol[] and 0 if they are given as a 256 bit
 *                  vector (32 bytes).
 * 
 * nsymbols:uint9   There are at most 256 distinct symbols, which are the
 *                  distinct possible byte values (9 bits, which allows
 *                  for an empty leaf or node with no byte values used as
 *                  well as a leaf or node with all 256 byte values used).
 * 
 * -- note: at this point the record is byte aligned --
 * 
 * symbol:byte[]    The packed symbol array -or- an alternative coding which
 * symbol:bit[256]  is used if there are GT 32 distinct symbols to be coded
 *                  since it a more compact representation.
 * 
 *                  O_symbols := 16 bits.
 * 
 * -- note: at this point the record is byte aligned --
 * 
 * bitLengthBits:uint8
 *                  The width in bits of the integers used to code the 
 *                  bitLength[].
 * 
 *                  The bit offset to this field is either ((2+nsymbols)*8) (if
 *                  the symbols are coded as a packed byte[]) or ((2+32)*8) (if
 *                  the symbols are coded as a 256 bit vector).
 * 
 * codeOffsetBits:uint8
 *                  The width in bits of the integers used to code the
 *                  codeOffset[].  If this is ZERO (0), then the codeOffset[]
 *                  was not stored and the coded values must be decoded with
 *                  a sequential scan of the codeWord[].
 * 
 *                  O_codeOffsetBits := O_bitLengths + 8;
 * 
 * sumCodedValueBitLengths:uint32
 *                  The sum of the bit lengths of the coded values.  This is
 *                  a 32bit unsigned integer, which is sufficient to code up
 *                  bit lengths of up to 512MB.  This field IS NOT present 
 *                  if the codeOffsetBits is ZERO (0) since the field is only
 *                  used to compute the bit offset of the codeOffset[].
 * 
 *                  O_sumCodedValueBitLengths := O_bitLengths + 16;
 * 
 *  -- note: at this point the record is byte aligned --
 *    
 * nulls[]          A vector of [nvalues] bit flags.  A flag is a ONE (1) iff
 *                  the corresponding byte[] in the logical byte[][] was a 
 *                  null.  A null is coded as a sequence of ZERO (0) code 
 *                  words.  However, empty byte[]s are also permitted for
 *                  B+Tree values (but not for B+Tree keys).  Therefore you
 *                  MUST test the nulls[] to distinguish between a null byte[]
 *                  and an empty byte[].
 * 
 *                  O_nulls :=  O_bitLengthBits + 8 + 8 + (codeOffsetBits==0?0:32);
 *   
 * bitLength[]      The delta in the bit length of each code word from the
 *                  previous code word.  There is one code word pre symbol.  
 *                  The code words MUST be a canonical huffman code, which
 *                  implies that the code words are arranged in a
 *                  non-decreasing order.  This is used to reconstruct the
 *                  decoder when processing the record.  The combination of
 *                  a canonical huffman code and the bitLength[] allows us 
 *                  to generate the decoder without any additional information.
 * 
 *                  O_bitLength[] := O_nulls + nvalues
 * 
 * codedValue:bit[] The coded values given as a sequence of code words.  The
 *                  offsets of each coded value in this array are given
 *                  directly by the codeOffset[].
 * 
 *                  O_codedValue[] := O_bitLength[] + (nsymbols*bitLengthBits)
 * 
 * codeOffset[]     Offset to the start of each code from the start of the
 *                  codedValue[].  While the delta in the offsets could be
 *                  represented more efficiently, the offsets are represented
 *                  directly so that we may avoid reading the entire codeOffset[]
 *                  into memory.  This array is present iff codeOffsetBits is GT 
 *                  ZERO.
 * 
 *                  O_codeOffset[] := O_codedValue[] + sumCodedValueBitLengths
 * </pre>
 * 
 * <h3>Interesting links</h3>
 * <p>
 * Hu-Tucker links:
 * <ul>
 * <li></li>
 * <li>http://www.google.com/search?hl=en&q=Hu-Tucker - google search</li>
 * <li>http://www.cs.rit.edu/~std3246/thesis/node10.html - detailed online
 * description</li>
 * <li>T. C. Hu. Combinatorial Algorithms. Addison-Wesley Publishing Co., 1982.</li>
 * <li>Donald E. Knuth. The Art of Computer Programming, volume 3.
 * Addison-Wesley Publishing Co., 1973. Sorting and Searching.</li>
 * <li>http://portal.acm.org/citation.cfm?doid=355602.361319 - published
 * algorithm</li>
 * <li>http://www.cs.rit.edu/~std3246/thesis/thesis.html - thesis studying the
 * Hu-Tucker algorithm, implementation strategies, and their performance.</li>
 * <li>http://www.cse.ucsd.edu/classes/sp07/cse202/lec9.pdf - from a lecture.</li>
 * <li>http://www.cs.utexas.edu/users/plaxton/c/337/projects/1/desc.pdf - a
 * class project that describes phase I of the Hu-Tucker algorithm.</li>
 * <li>Towards a Dynamic Optimal Binary Alphabetic Tree -
 * ftp://dimacs.rutgers.edu
 * /pub/dimacs/TechnicalReports/TechReports/1999/99-22.ps.gz</li>
 * <li>Describes an approach to fast sorting (word RAM) using a linear time
 * algorithm as an alternative to Hu-Tucker.</li>
 * </ul>
 * 
 * @see http://en.wikipedia.org/wiki/Huffman_coding
 * 
 * @see http://www.zlib.net/ (BSD style license; the Java classes are JNI
 *      wrappers that expose part of this functionality. The JNI interface could
 *      doubtless be extended to gain greater access to the underlying library.
 *      madler@alumni.caltech.edu is the remaining active author.)
 * 
 * @see http://www.jcraft.com/jzlib/ (BSD style license; this is a pure Java
 *      port of the zlib library and might provide access to the dictionary).
 * 
 * @see http://www.oberhumer.com/opensource/lzo/ (GPL license; there is a java
 *      binding and it provides faster compression and extremely fast
 *      decompression. There is a pure Java decompression package, but the
 *      compression package is only available in C. The Java code is also GPL.
 *      The author offers the possiblity of alternative licensing on request.)
 * 
 * @see http://www.faqs.org/faqs/compression-faq/ (FAQs on the compression news
 *      group).
 * 
 * @see http://www.compressconsult.com/huffman/ (Practical Huffman coding by a
 *      data compression consultant : michael@compressconsult.com).
 * 
 * @see http://www.cs.helsinki.fi/u/jikorhon/ngp/compression.html
 * 
 * @see http 
 *      ://coding.derkeiler.com/Archive/Java/comp.lang.java.programmer/2003-10
 *      /1545.html
 * 
 * @see http 
 *      ://java.sun.com/j2se/1.4.2/docs/api/java/util/zip/package-summary.html
 *      #package_description
 * 
 * @see http://www.isi.edu/in-notes/rfc1951.txt (RFC)
 * 
 * @todo wrap an {@link ILeafData}, etc. within another implementation which
 *       caches the objects required to decode (or decode and encode for
 *       hu-tucker). associate those with the tuple used to access the node or
 *       leaf or with the invocation context, or place onto an LRU so we can
 *       avoid constant re-creates of these decoders and encoders without having
 *       them hang around 1:1 with the buffered nodes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CanonicalHuffmanRabaCoder implements IRabaCoder {

    /**
     * If there are GT this many symbols then we write a bit coded symbol table,
     * otherwise we write a packed symbol table. The bit coded symbol table is
     * 256 bits, where each bit indicates whether the corresponding byte value
     * is present in the uncoded byte[][]. The packed symbol table is one byte
     * per distinct byte occurring in the byte[][] to be coded. When this is set
     * to 256 we always write a packed symbol table. If it is set to 32, then we
     * never use more than 32 bytes for the symbol table and bit flag the
     * symbols which are present (which takes 32 bytes).
     * 
     * @todo This is parameterized so we can use bit flags if a lot of the byte
     *       values are in use. However, if MOST byte values are in use then we
     *       could assume that ALL byte values were in use and that the symbol
     *       index IS the byte value and NOT encode the symbol table into the
     *       record.
     * 
     * @todo make this a parameter to {@link #encode(IRaba)}
     */
    final static int PACKED_SYMBOL_TABLE_THRESHOLD = 256; // @todo 32.

    /**
     * The original serialization version for the record.
     */
    final static byte VERSION0 = 0x00;

    protected static final Logger log = Logger
            .getLogger(CanonicalHuffmanRabaCoder.class);

    /**
     * FIXME When the {@link IRaba#isKeys()} then code with the unpacked
     * frequency[], do not allow nulls, and support search. Otherwise code with
     * only the packed frequency[], allow nulls, and do not support search.
     */
    final public boolean isKeyCoder() {

        return false;

    }

    final public boolean isValueCoder() {

        return true;

    }

    public CanonicalHuffmanRabaCoder() {

    }

    /**
     * Write out the #of symbols and the symbol table (symbol2byte). The symbol
     * table is written either as a sequence of the in use byte values or as 256
     * bit flags indicating for each byte value whether or not it is part of the
     * symbol table. The values in the symbol table are appear in their lexical
     * ordering.
     * 
     * @param symbol2byte
     *            The symbol table.
     * @param obs
     *            The symbol table is written on this bit stream.
     * 
     * @return <code>true</code> iff the symbol table was bit coded.
     */
    protected boolean writeSymbolTable(final Symbol2Byte symbol2byte,
            final OutputBitStream obs) throws IOException {

        assert symbol2byte != null;
        // assert symbol2byte.length <= 256;
        assert obs != null;

        final int nsymbols = symbol2byte.getSymbolCount();

        final boolean bitCodedSymbols = nsymbols > PACKED_SYMBOL_TABLE_THRESHOLD;

        obs.writeBit(bitCodedSymbols);

        obs.writeInt(nsymbols, 9/* nbits */);

        if (bitCodedSymbols) {

            // @todo write bit flags for the used vs unused symbols.
            throw new UnsupportedOperationException();

        } else {

            // for each symbol, write the byte
            for (int i = 0; i < nsymbols; i++) {

                obs.writeInt(symbol2byte.symbol2byte(i), 8/* len */);

            }

        }

        return bitCodedSymbols;

    }

    /**
     * Return the bit length used to code the bit lengths of the code words. The
     * code words MUST be a canonical huffman code, which implies that the bit
     * lengths are non-decreasing. The bit lengths are coded using the delta of
     * the bit lengths between successive code words.
     * 
     * @param codeWord
     *            The code words.
     * 
     * @return The minimum #of bits required to represent the code word bit
     *         length deltas.
     */
    protected int getBitLengthBits(final BitVector[] codeWord) {
        if (codeWord == null)
            throw new IllegalArgumentException();
        int lastBitLength = 0;
        int maxDelta = 0;
        int i = 0;
        for (BitVector v : codeWord) {
            // delta between two bit lengths.
            final int bitLength = v.size();
            final int delta = bitLength - lastBitLength;
            assert delta >= 0 : "Not a canonical huffman code: index=" + i
                    + ", " + Arrays.toString(codeWord);
            if (delta > maxDelta) {
                maxDelta = delta;
            }
            lastBitLength = bitLength;
            i++;
        }
        return maxDelta;
    }

    /**
     * Write out the bitLength[] using the delta between the bit lengths of
     * successive code words. The code MUST be a canonical huffman code. This
     * implies that the bit lengths are non-decreasing.
     * 
     * @param codeWord
     *            The code words.
     * @param bitLengthBits
     *            The #of bits that will be used to represent the delta between
     *            successive code word lengths.
     * @param obs
     *            The output bit stream.
     * 
     * @throws IOException
     */
    protected void writeBitLengths(final BitVector[] codeWord,
            final int bitLengthBits, final OutputBitStream obs)
            throws IOException {
        int lastBitLength = 0;
        for (BitVector v : codeWord) {
            // delta between two bit lengths.
            final int bitLength = v.size();
            final int delta = bitLength - lastBitLength;
            assert delta >= 0;
            obs.writeInt(delta, bitLengthBits);
        }
    }

    /**
     * Return the cumulative bit length of the coded values.
     * 
     * @param coder
     *            The coder.
     * @param raba
     *            The logical byte[][].
     * @param byte2symbol
     *            The mapping from byte values to symbol indices.
     * 
     * @return The total bit length of the coded values.
     * 
     * @deprecated Leave this field and the #of bits per codedValueOffset[]
     *             element blank until we have written out the coded values and
     *             then rewind the OBS and fill in those fields. Otherwise we
     *             are encoding the same byte[][] data twice, which is wasted
     *             effort.
     */
    protected long getSumCodedValueBitLengths(final BitVector[] codeWords,
            final IRaba raba, final Byte2Symbol byte2symbol) {

        final int nvalues = raba.size();

        // cumulative code lengths.
        long sumCodedValueBitLengths = 0;

        for (int i = 0; i < nvalues; i++) {

            final byte[] a = raba.get(i);

            long codeLength = 0;

            if (a != null) {

                for (byte b : a) {

                    codeLength += codeWords[byte2symbol.byte2symbol(b)]
                            .length();

                }

            }

            // Cumulative bit length of the code words for all coded values.
            sumCodedValueBitLengths += codeLength;

        }

        return sumCodedValueBitLengths;

    }

    /**
     * Write out the coded values.
     * 
     * @param coder
     *            The coder.
     * @param raba
     *            The logical byte[][].
     * @param byte2symbol
     *            The mapping from byte values to symbol indices.
     * @param codedValueOffset
     *            An optional array dimensioned to <code>nvalues+1</code>, where
     *            <i>nvalues</i> is #of values in the logical byte[][]. When
     *            given, the array will be populated with the relative bit
     *            offset of the start of each coded value. The offsets are
     *            relative to the start of the first coded value.
     * 
     * @return The #of bits written (sum total of the bit lengths of the coded
     *         values).
     */
    // @todo verify Ok to pass coder in here rather than codeWord[].
    protected long writeCodedValues(final PrefixCoder coder,
            // final BitVector[] codeWords,
            final IRaba raba, final Byte2Symbol byte2symbol,
            final long[] codedValueOffset, final OutputBitStream obs)
            throws IOException {

        final int nvalues = raba.size();

        if (codedValueOffset != null) {

            if (codedValueOffset.length != nvalues + 1)

                throw new IllegalArgumentException();

        }

        long bitsWritten = 0L;

        for (int i = 0; i < nvalues; i++) {

            if (codedValueOffset != null)
                codedValueOffset[i] = bitsWritten;

            final byte[] a = raba.get(i);

            if (a != null) {

                for (byte b : a) {

                    final int symbol = byte2symbol.byte2symbol(b);

                    if (symbol == -1)
                        throw new UnsupportedOperationException(
                                "Can not code value: " + b);

                    bitsWritten += coder.encode(symbol, obs);
                    // final int nbits =
                    // obs.write(codeWords[symbol].iterator());
                    // if(nbits<0)throw new IOException();
                    // bitsWritten += nbits;

                }

            }

        }

        if (codedValueOffset != null)
            codedValueOffset[nvalues] = bitsWritten;

        System.err.println("codedValueOffset[]="
                + Arrays.toString(codedValueOffset));

        return bitsWritten;

    }

    /**
     * Interface maps a byte value to the symbol used to code that byte value.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    interface Byte2Symbol {

        /**
         * Mapping from byte values to symbol indices.
         * 
         * @param b
         *            The byte value.
         * 
         * @return The symbol used to code that byte value -or- <code>-1</code>
         *         if the byte value was not assigned to any symbol.
         */
        int byte2symbol(byte b);

    }

    /**
     * Interface maps a symbol to the byte value assigned to that symbol.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    interface Symbol2Byte {

        /**
         * Return the #of symbols.
         */
        int getSymbolCount();

        /**
         * Mapping from symbol indices to byte values.
         * 
         * @param symbol
         *            The symbol index.
         * 
         * @return The byte value.
         */
        byte symbol2byte(int symbol);

    }

    /**
     * Abstract base class for preparing a logical byte[][] for coding.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    abstract protected static class AbstractCodingSetup implements Byte2Symbol,
            Symbol2Byte {

        /**
         * Return the #of distinct symbols used to generate the code.
         */
        abstract public int getSymbolCount();

        // /**
        // * The codec used to encode and decode the logical byte[][].
        // */
        // abstract public HuffmanCodec codec();

        abstract public BitVector[] codeWords();

        abstract PrefixCoder coder();

        /**
         * Format the code book as a multi-line string.
         * 
         * @param codeWords
         *            The code words.
         * @param symbol2byte
         *            The mapping from symbol indices to byte value.
         * 
         * @return A representation of the code book.
         */
        static protected String printCodeBook(final BitVector[] codeWords,
                final Symbol2Byte symbol2byte) {

            final StringBuilder sb = new StringBuilder();

            {

                int symbol = 0;

                for (BitVector v : codeWords) {

                    final byte b = symbol2byte.symbol2byte(symbol);

                    sb.append("codeWord: " + v + ", symbol=" + symbol
                            + ", value=" + b + " (" + (char) b + ")\n");

                    symbol++;

                }

            }
            // {
            //
            // // indices will be reordered by indirection sort.
            // final int order[] = new int[codeWords.length];
            // for (int i = 0; i < codeWords.length; i++) {
            // order[i] = i;
            // }
            //
            // // sort indirection[] into non-decreasing code word length.
            // Sorting.quickSort( order, 0, codeWords.length, new
            // IntComparator() {
            // public int compare( int x, int y ) {
            // return codeWords[ x ].size() - codeWords[ y ].size();
            // }
            // });
            //
            // sb.append("\n-- sorted by code length--\n");
            //
            // for (int i = 0; i < codeWords.length; i++) {
            //
            // final BitVector v = codeWords[order[i]];
            //
            // sb.append("codeWord: " + v + ", order[" + i + "]="
            // + order[i]
            // + // indirection order.
            // ", value[order[" + i + "]=" + symbol2byte[order[i]]
            // + " (" + (char) symbol2byte[order[i]] + ")"
            // + // w/o indirection
            // ", value=" + symbol2byte[i] + " ("
            // + (char) symbol2byte[i] + ")" +
            // //
            // "\n");
            //
            // }
            //
            // }
            return sb.toString();

        }

        // /**
        // * Return a dense array of the non-zero frequency counts in byte value
        // * order. The length of the array is the #of distinct symbols
        // appearing in
        // * the input.
        // *
        // * @param raba
        // * The logical byte[][].
        // *
        // * @return The packed frequency counts.
        // */
        // protected int[] getPackedFrequencyCount(final IRandomAccessByteArray
        // raba) {
        //
        // final int[] frequency = new int[Byte.MAX_VALUE - Byte.MIN_VALUE + 1];
        //
        // final int size = raba.size();
        //
        // int nsymbols = 0;
        //        
        // for (int i = 0; i < size; i++) {
        //
        // final byte[] a = raba.get(i);
        //
        // if (a != null) {
        //
        // for (byte b : a) {
        //
        // if(frequency[b - Byte.MIN_VALUE]++==1) {
        //                        
        // nsymbols++;
        //                        
        // }
        //
        // }
        //
        // }
        //
        // }
        //
        // final int[] packedFreq = new int[nsymbols];
        //
        // for (int i = 0, j = 0; i < frequency.length; i++) {
        //
        // if (frequency[i] != 0) {
        //
        // packedFreq[j++] = frequency[i];
        //
        // }
        //
        // }
        //        
        // return frequency;
        //        
        // }

        /**
         * Create a frequency table reporting the #of occurrences of for every
         * possible byte value.
         * 
         * @param The
         *            data.
         * 
         * @return An 256 element array giving the frequency of each byte value.
         *         Values not observed will have a zero frequency count.
         */
        protected int[] getFrequencyCount(final IRaba raba) {

            final int[] frequency = new int[Byte.MAX_VALUE - Byte.MIN_VALUE + 1];

            final int size = raba.size();

            for (int i = 0; i < size; i++) {

                final byte[] a = raba.get(i);

                if (a != null) {

                    for (byte b : a) {

                        frequency[b - Byte.MIN_VALUE]++;

                    }

                }

            }

            return frequency;

        }

        /**
         * Compute the number of distinct bytes.
         * 
         * @param frequency
         *            An array of 256 elements giving the frequency of
         *            occurrence for each possible byte value.
         * 
         * @return The #of non-zero elements in that array.
         */
        protected int getSymbolCount(final int[] frequency) {

            int count = 0;
            for (int i = frequency.length; i-- != 0;) {

                if (frequency[i] != 0) {

                    count++;

                }

            }

            return count;

        }

        /**
         * Build the symbol table, populating the packedFrequency array, etc. as
         * a side effect.
         * 
         * @param frequency
         *            An array of 256 frequency counts. Each element of the
         *            array gives the frequency of occurrence of the
         *            corresponding byte value.
         * @param packedFrequency
         *            The non-zero symbol frequency counts. This array is
         *            correlated with the packed symbol table. The array must be
         *            pre-allocated by the caller with <i>nsymbol</i> elements.
         * @param symbol2byte
         *            The forward lookup symbol table. The array must be
         *            pre-allocated by the caller with <i>nsymbol</i> elements.
         * @return The reverse symbol table.
         */
        protected Byte2IntOpenHashMap buildSymbolTable(final int[] frequency,
                final int[] packedFrequency, final byte[] symbol2byte) {

            /*
             * Remap the used bytes, building at the same time maps from symbol
             * to bytes and from bytes to symbols.
             */

            assert frequency.length == 256;
            assert packedFrequency.length == symbol2byte.length;

            final int nsymbols = packedFrequency.length;

            // the reverse lookup for the packed symbol table.
            final Byte2IntOpenHashMap byte2symbol = new Byte2IntOpenHashMap(
                    nsymbols);

            byte2symbol.defaultReturnValue(-1);

            for (int i = frequency.length, k = nsymbols; i-- != 0;) {

                if (frequency[i] != 0) {

                    packedFrequency[--k] = frequency[i];

                    final byte b = (byte) (i + Byte.MIN_VALUE);

                    symbol2byte[k] = b;

                    byte2symbol.put(b, k);

                }

            }

            byte2symbol.trim();

            return byte2symbol;

        }

    }

    /**
     * Class performs set up for coding and is used when we require only direct
     * lookup of a coded value by its index into the logical byte[][]. In this
     * case we use a packed symbol table containing only the non-zero frequency
     * counts. This results in a more compact code.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * 
     * @todo The main issue here is making sure that the packedFreq[] is in
     *       decreasing symbol frequency order so the generated code words will
     *       have lengths which are correlated with the byte values of the
     *       symbols. The outputs are:
     * 
     *       x. packedFreq[] with decreasing symbol frequency. This is only used
     *       internally.
     * 
     *       x. codeWord[] with increasing code word bit length. Since we
     *       pre-sort the symbols into decreasing frequency, this is just
     *       {@link HuffmanCodec#codeWords()}.
     * 
     *       x. byte2symbol(byte b):int A function mapping byte values to
     *       indices in the packedFreq[] and the codeWord[].
     * 
     *       x. HuffmanCodec which may be used for encoding or decoding.
     */
    protected static class PackedCodingSetup extends AbstractCodingSetup {

        /** The #of distinct symbols (distinct byte values) actually used. */
        final int nsymbols;

        /**
         * The packed frequency counts in non-increasing frequency order.
         */
        private final int[] packedFrequency;

        // /** The canonical huffman codec. */
        // private final HuffmanCodec codec;

        /**
         * The code words of the canonical huffman code.
         */
        private final BitVector[] codeWords;

        final public BitVector[] codeWords() {

            return codeWords;

        }

        private final PrefixCoder coder;

        final PrefixCoder coder() {

            return coder;

        }

        private final Byte2IntOpenHashMap byte2symbol;

        /** The packed symbol table (symbol to byte). */
        private final byte[] symbol2byte;

        final public int getSymbolCount() {

            return nsymbols;

        }

        final public int byte2symbol(final byte b) {

            return byte2symbol.get(b);

        }

        final public byte symbol2byte(final int symbol) {

            return (byte) symbol2byte[symbol];

        }

        // @Override
        // final public HuffmanCodec codec() {
        //
        // return codec;
        //        
        // }

        public PackedCodingSetup(final IRaba raba) {

            // The #of byte[] values to be coded.
            final int size = raba.size();

            {

                /**
                 * Array of frequency counts for all possible byte values.
                 */
                final int[] frequency = new int[256];

                int nsymbols = 0;

                for (int i = 0; i < size; i++) {

                    final byte[] a = raba.get(i);

                    if (a != null) {

                        for (byte b : a) {

                            // @todo test with all byte values!!!
                            if (frequency[b - Byte.MIN_VALUE]++ == 0) {

                                nsymbols++;

                            }

                        }

                    }

                }

                this.nsymbols = nsymbols;

                /*
                 * Remap the used bytes, building at the same time maps from
                 * symbol to bytes and from bytes to symbols.
                 */

                this.packedFrequency = new int[nsymbols];

                this.symbol2byte = new byte[nsymbols];

                // the reverse lookup for the packed symbol table.
                byte2symbol = new Byte2IntOpenHashMap(nsymbols);

                byte2symbol.defaultReturnValue(-1);

                for (int i = frequency.length, k = nsymbols; i-- != 0;) {

                    if (frequency[i] != 0) {

                        packedFrequency[--k] = frequency[i];

                        final byte b = (byte) (i + Byte.MIN_VALUE);

                        symbol2byte[k] = b;

                        byte2symbol.put(b, k);

                    }

                }

                byte2symbol.trim();

            }

            // /*
            // * FIXME All of this pre-ordering is a hack working around the
            // lack
            // * of an API to expose the ordered codeWords[] and the correlated
            // * symbol[] in HuffmanCodec. If we can get that API modified to
            // * expose the appropriate data then we can avoid these sorting
            // steps
            // * which are already being performed internally by the
            // HuffmanCodec.
            // * By sorting first, we are simply guaranteeing that the codec
            // will
            // * not reorder the data and hence that codec.codeWords() will be
            // in
            // * code book order for a canonical huffman code.
            // */
            //        
            // /*
            // * Generate order[] : a permutation of the symbol indices into
            // * non-increasing frequency.
            // *
            // * Note: Since the frequency[] is decreasing, the assigned
            // * codeWord[] will be in non-decreasing order.
            // */
            // final int order[] = new int[nsymbols];
            // for (int i = 0; i < nsymbols; i++) {
            // order[i] = i;
            // }
            //
            // /*
            // * Sort order[] into non-increasing frequency order.
            // */
            // Sorting.quickSort(order, 0, nsymbols, new IntComparator() {
            // public int compare(final int x, final int y) {
            // return packedFrequency[y] - packedFrequency[x];
            // }
            // });
            //
            // /*
            // * Permutate the packedFrequency[] into non-increasing order as
            // * determined by the order[]. Note that this uses the exact
            // * permutation established by order[] rather than sorting by
            // * packedFrequency[] again in case there is something which is not
            // * stable about the sort.
            // */
            // Sorting.quickSort(packedFrequency, 0, nsymbols,
            // new IntComparator() {
            // public int compare(final int x, final int y) {
            // return order[y] - order[x];
            // }
            // });

            /*
             * Generate a canonical huffman code. The input is the packed
             * frequency data (only symbols which occur at least once). The
             * packed frequency counts appear in non-increasing frequency order.
             * Therefore the assigned canonical codeWord[] will already be in
             * non-decreasing code length order.
             */
            final HuffmanCodec codec = new HuffmanCodec(packedFrequency);

            this.coder = codec.coder();

            /*
             * Fix up the symbol2byte mapping to match the canonical code book.
             */
            {
                // the symbol permutation order.
                final int order[] = codec.getSymbols();

                // rearrange the symbol2byte mapping.
                final byte[] tmp = new byte[nsymbols];
                for (int i = 0; i < nsymbols; i++) {
                    tmp[i] = symbol2byte[order[i]];
                }

                System.arraycopy(tmp, 0, symbol2byte, 0, nsymbols);
            }

            // // the packed symbol table.
            // symbol2byte = new byte[nsymbols];
            //
            // // the reverse lookup for the packed symbol table.
            // byte2symbol = buildSymbolTable(frequency, packedFrequency,
            // symbol2byte);

            this.codeWords = codec.codeWords();

            // /*
            // * Generate order[] : a permutation of the symbol indices into
            // * increasing code length order.
            // */
            //
            // // indices will be reordered by indirection sort.
            // final int order[] = new int[codeWords.length];
            // for (int i = 0; i < codeWords.length; i++) {
            // order[i] = i;
            // }

            // /*
            // * Place the symbol[] into non-decreasing code word length order
            // so
            // * it will remain correlated with the codeWord[] once the latter
            // is
            // * sorted into the same order.
            // */
            // Sorting.quickSort(symbol2byte, 0, nsymbols, new ByteComparator()
            // {
            // public int compare(byte x, byte y) {
            // return order[x] - order[y];
            // }
            // });

            // sort order[] into non-decreasing code word length.
            // Sorting.quickSort(order, 0, codeWords.length, new IntComparator()
            // {
            // public int compare(int x, int y) {
            // return codeWords[x].size() - codeWords[y].size();
            // }
            // });

            // /*
            // * Place the symbol[] into non-decreasing code word length order
            // so
            // * it will remain correlated with the codeWord[] once the latter
            // is
            // * sorted into the same order.
            // */
            // Sorting.quickSort(symbol2byte, 0, codeWords.length,
            // new ByteComparator() {
            // public int compare(byte x, byte y) {
            // return codeWords[x].size() - codeWords[y].size();
            // }
            // });

            /*
             * Place codeWord[] into non-decreasing code word length order.
             * 
             * @todo I have absolutely no idea why this sort is required. I
             * would think that it should have been done by the HuffmanCodec
             * ctor.
             */
            Arrays.sort(codeWords, 0, codeWords.length,
                    new Comparator<BitVector>() {
                        public int compare(final BitVector x, final BitVector y) {
                            final int diff = x.size() - y.size();
                            if (diff < 0)
                                return -1;
                            if (diff > 0)
                                return 1;
                            return 0;
                        }
                    });

            if (log.isInfoEnabled()) {

                log
                        .info("\n"
                                + printCodeBook(codeWords, this/* Symbol2Byte */));

            }

        }

    }

    /**
     * Class performs setup for coding and is used wnen we need to perform
     * search on the coded logical byte[][]. In this case we use byte values
     * with non-zero frequency counts as well. This makes it possible to encode
     * any byte[], which is a capability we require in order to search the coded
     * byte[][].
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private static class SearchCodingSetup extends AbstractCodingSetup {

        public SearchCodingSetup(final IRaba raba) {
            
            throw new UnsupportedOperationException();
            
        }
        
        @Override
        public int getSymbolCount() {
            // TODO Auto-generated method stub
            return 0;
        }

        public int byte2symbol(byte b) {
            // TODO Auto-generated method stub
            return 0;
        }

        // @Override
        // public HuffmanCodec codec() {
        // // TODO Auto-generated method stub
        // return null;
        // }

        public byte symbol2byte(int symbol) {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public BitVector[] codeWords() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        PrefixCoder coder() {
            // TODO Auto-generated method stub
            return null;
        }

    }

    /**
     * Setup for an empty {@link IRaba}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private static class EmptyRabaSetup extends AbstractCodingSetup {

        private final BitVector[] codeWords = new BitVector[0];
        
        public EmptyRabaSetup() {
            
        }
        
        @Override
        public BitVector[] codeWords() {
            return codeWords;
        }

        @Override
        PrefixCoder coder() {
            return null;
        }

        @Override
        public int getSymbolCount() {
            return 0;
        }

        public int byte2symbol(byte b) {
            throw new UnsupportedOperationException();
        }

        public byte symbol2byte(int symbol) {
            throw new UnsupportedOperationException();
        }

    }

    public IRabaDecoder encode(final IRaba raba) {

        final AbstractCodingSetup setup;
        
        if (raba.isEmpty()) {
            setup = new EmptyRabaSetup();
        } else {
            if (raba.isKeys()) {
                setup = new SearchCodingSetup(raba);
            } else {
                setup = new PackedCodingSetup(raba);
            }
        }

        // The #of byte[] values to be coded.
        final int nvalues = raba.size();

        // The #of distinct symbols (distinct byte values) actually used.
        final int nsymbols = setup.getSymbolCount();

        // // the packed symbol table.
        // final byte[] symbol2byte;

        // // the reverse lookup for the packed symbol table.
        // final Byte2IntOpenHashMap byte2symbol;

        // The canonical huffman encoder.
        // final HuffmanCodec codec = setup.codec();

        final BitVector[] codeWords = setup.codeWords();

        // // prepare for coding.
        // {
        //        
        // // @todo option to use all byte values so we can encoded any input.
        // final int[] packedFrequency = getPackedFrequencyCount(raba);
        //
        // /*
        // * Generate a canonical huffman code. The input is the packed
        // * frequency data (only symbols which occur at least once). The
        // * frequency counts appear in byte value order.
        // *
        // * @todo if the input order is non-decreasing frequency then the
        // * codeWord[] will already be in code length order.
        // */
        // codec = new HuffmanCodec(packedFrequency);
        //
        // // // The frequency count for data.
        // // final int[] frequency = getFrequencyCount(raba);
        // //
        // // // The number of distinct byte values that were used in the data.
        // // nsymbols = getSymbolCount(frequency);
        // //
        // // /*
        // // * Build up the packedFrequency table, the forward symbol table
        // // * (symbols to bytes), and the reverse symbol table (bytes to
        // // * symbols). Note that "symbols" are abstract indices which can be
        // // * correlated with any alphabet. For our purposes, they are always
        // // * correlated with the (sub)set of the 256 byte which appears in
        // the
        // // * data.
        // // */
        // //
        // // // The non-zero symbol frequency counts. this array is correlated
        // with
        // // // the packed symbol table.
        // // final int[] packedFrequency = new int[nsymbols];
        //
        // nsymbols = packedFrequency.length;
        //
        // // the packed symbol table.
        // symbol2byte = new byte[nsymbols];
        //
        // // the reverse lookup for the packed symbol table.
        // byte2symbol = buildSymbolTable(frequency, packedFrequency,
        // symbol2byte);
        //
        // }
        //
        // final BitVector[] codeWords = codec.codeWords();
        // {
        //
        // // /*
        // // * Generate order[] : a permutation of the symbol indices into
        // // * increasing code length order.
        // // */
        // //
        // // // indices will be reordered by indirection sort.
        // // final int order[] = new int[codeWords.length];
        // // for (int i = 0; i < codeWords.length; i++) {
        // // order[i] = i;
        // // }
        // //
        // // // sort order[] into non-decreasing code word length.
        // // Sorting.quickSort(order, 0, codeWords.length, new IntComparator()
        // {
        // // public int compare(int x, int y) {
        // // return codeWords[x].size() - codeWords[y].size();
        // // }
        // // });
        // //
        // // /*
        // // * Place the symbol[] into non-decreasing code word length order so
        // // * it will remain correlated with the codeWord[] once the latter is
        // // * sorted into the same order.
        // // */
        // // for(int i=0; i<nsymbols; i++) {
        // // }
        // // Sorting.quickSort(symbol2byte, 0, codeWords.length,
        // // new ByteComparator() {
        // // public int compare(byte x, byte y) {
        // // return codeWords[x].size() - codeWords[y].size();
        // // }
        // // });
        // //
        // /*
        // * Place codeWord[] into non-decreasing code word length order.
        // *
        // * @todo I have absolutely no idea why this sort is required. I
        // * would think that it should have been done by the HuffmanCodec
        // * ctor.
        // */
        // Arrays.sort(codeWords, 0, codeWords.length,
        // new Comparator<BitVector>() {
        // public int compare(final BitVector x, final BitVector y) {
        // final int diff = x.size() - y.size();
        // if (diff < 0)
        // return -1;
        // if (diff > 0)
        // return 1;
        // return 0;
        // }
        // });
        //
        // if (log.isInfoEnabled()) {
        //
        // log.info("\n" + printCodeBook(codeWords, symbol2byte));
        //
        // }
        //
        // }

        /*
         * Write out the record on a bit stream backed by a byte[] buffer.
         */
        try {

            // The serialization version for the record.
            final int version = VERSION0;

            /*
             * @todo The fbaos default is a 16kb buffer! Plus the BOS allocates
             * ANOTHER 16kb buffer to buffer the buffer!
             */
            final FastByteArrayOutputStream baos = new FastByteArrayOutputStream();

            final OutputBitStream obs = new OutputBitStream(baos);

            // The record version identifier.
            obs.writeInt(version, 6/* nbits */);

            // The #of elements in the logical byte[][].
            obs.writeInt(nvalues, 31);
            
            // Indicates if we are coding keys vs values.
            obs.writeBit(raba.isKeys());

            if(nvalues>0) {
            
            // Write out the #of symbols and the symbol table.
            final boolean bitCodedSymbols = writeSymbolTable(
                    setup/* Symbol2Byte */, obs);

            // The #of bits used to code the bit lengths of the code words.
            final int bitLengthBits = getBitLengthBits(codeWords);

            // Total #of bits in the coded values.
            final long sumCodedValueBitLengths = getSumCodedValueBitLengths(
                    codeWords, raba, (Byte2Symbol) setup);

            // #of bits per element in the codedValueOffset[] : @todo iff
            // nvalues>16
            // and SEQUENTIAL flag was not specified by called.
            //
            // FIXME why plus 1?!?
            final int codedValueOffsetBits = Fast
                    .mostSignificantBit(sumCodedValueBitLengths) + 1;

            final long O_bitLengthBits = obs.writtenBits();
            assert O_bitLengthBits == (bitCodedSymbols ? (2 + 32L)
                    : (2 + nsymbols)) << 3;
            obs.writeInt(bitLengthBits, 8/* nbits */);
            obs.writeInt(codedValueOffsetBits, 8/* nbits */);
            if (codedValueOffsetBits != 0) {
                obs.writeLong(sumCodedValueBitLengths, 32/* nbits */);
            }

            // nulls[] : bit flags identifying null byte[]s.
            final long O_nulls = obs.writtenBits();
            assert O_nulls == (O_bitLengthBits + 8 + 8 + (codedValueOffsetBits == 0 ? 0
                    : 32));
            for (int i = 0; i < nvalues; i++) {
                obs.writeBit(raba.isNull(i));
            }

            // write out the bitLength[] (coded as delta in the bit lengths).
            final long O_bitLengths = obs.writtenBits();
            assert O_bitLengths == O_nulls + nvalues;
            writeBitLengths(codeWords, bitLengthBits, obs);

            // Write out the coded values.
            final long O_codedValues = obs.writtenBits();
            assert O_codedValues == O_bitLengths + (nsymbols * bitLengthBits) : "O_codedValues="
                    + O_codedValues
                    + " != (nsymbols="
                    + nsymbols
                    + " * bitLengthBits=" + bitLengthBits + ")";
            final long[] codedValueOffset = (codedValueOffsetBits == 0 ? null
                    : new long[nvalues + 1]);
            final long sumCodedValueBitLengths2 = writeCodedValues(setup
                    .coder(),
            // codeWords,
                    raba, (Byte2Symbol) setup, codedValueOffset, obs);
            assert sumCodedValueBitLengths == sumCodedValueBitLengths2 : "sumCodedValueBitLengths="
                    + sumCodedValueBitLengths
                    + " != sumCodedValueBitLengths2="
                    + sumCodedValueBitLengths2;

            final long O_codedValueOffsets;
            if (codedValueOffsetBits != 0) {
                /*
                 * Write out the codedValueOffset[].
                 */
                O_codedValueOffsets = obs.writtenBits();
                assert O_codedValueOffsets == O_codedValues
                        + sumCodedValueBitLengths;
                for (long offset : codedValueOffset) {
                    obs.writeLong(offset, codedValueOffsetBits/* nbits */);
                }
            } else {
                // Not included in the record.
                O_codedValueOffsets = 0L;
            }

            }
            
            // done writing (will byte align the OBS).
            obs.flush();

            // trim backing array to an exact fit.
            baos.trim();

            // done.
            return new HuffmanRabaDecoder(ByteBuffer.wrap(baos.array)
            // ,
            // codec.decoder()
            );

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    public IRabaDecoder decode(final ByteBuffer data) {

        return new HuffmanRabaDecoder(data);

    }

    /**
     * Decoder.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * 
     *          FIXME The methods which interact with the buffer must not change
     *          its position or limit. If they do then ALL methods which touch
     *          the buffer need to be synchronized so NONE of them can have a
     *          concurrent read during which the position/limit has been
     *          transiently modified.
     */
    public static class HuffmanRabaDecoder extends AbstractRabaDecoder {

        protected static final Logger log = Logger
                .getLogger(HuffmanRabaDecoder.class);

        /**
         * The entries in the logical byte[][].
         */
        private final int size;

        /**
         * If the logical byte[][] contains B+Tree keys vs B+Tree values.
         */
        private final boolean isKeys;
        
        private final ByteBuffer data;

        private final Decoder decoder;

        private final int nsymbols;

        /**
         * The bit offset to the start of the nulls[]. These are bit flags
         * indicating whether the corresponding entry in the logical byte[][] is
         * a <code>null</code>. You must consult these bit flags in order to
         * distinguish a <code>null</code> from a zero length byte[].
         */
        private final long O_nulls;

        /**
         * The bit offset to the start of the codedValue[] in the record.
         */
        private final long O_codedValues;

        /**
         * The #of bits used to represent the elements of the codedValueOffset[]
         * -or- ZERO (0) iff that array was not included in the record.
         */
        private final int codedValueOffsetBits;

        /**
         * The bit offset to the start of the codedValueOffset[] in the record
         * -or- ZERO (0L) iff that array was not included in the record.
         */
        private final long O_codedValueOffsets;

        /**
         * 
         * @param data
         *            The record containing the coded data.
         */
        public HuffmanRabaDecoder(final ByteBuffer data) {

            this(data, null/* decoder */);

        }

        /**
         * 
         * @param data
         *            The record containing the coded data.
         * @param decoder
         *            The decoder (optional). When not given the decoder is
         *            reconstructed from the record.
         * 
         * @todo the decoder parameter is not proving to be terribly useful
         *       right now due to API problems with the underlying
         *       {@link HuffmanCodec}.
         */
        public HuffmanRabaDecoder(final ByteBuffer data,
                final Decoder decoder) {
            
            final boolean debug = log.isDebugEnabled() || true;

            if (data == null)
                throw new IllegalArgumentException();

            this.data = data;

            final StringBuilder sb = debug ? new StringBuilder() : null;
            final InputBitStream ibs = new InputBitStream(
                    new ByteBufferInputStream(data));
            try {
                final int version = ibs.readInt(6/* nbits */);
                if (version != VERSION0) {
                    throw new IOException("Unknown version: " + version);
                }
                if (debug)
                    sb.append("version=" + version + "\n");
                
                this.size = ibs.readInt(31/*nbits*/);
                
                if (size < 0)
                    throw new RuntimeException();

                isKeys = ibs.readBit() != 0;
            
                final boolean bitCodedSymbols = ibs.readBit() != 0;
                if (debug)
                    sb.append("bitCodedSymbols=" + bitCodedSymbols + "\n");
                nsymbols = ibs.readInt(9/* nbits */);
                if (debug)
                    sb.append("nsymbols=" + nsymbols + "\n");
                final long O_bitLengthBits = (bitCodedSymbols ? ((2 + 32) << 3)
                        : ((2 + nsymbols) << 3));
                if (debug)
                    sb.append("O_bitLengthBits=" + O_bitLengthBits + "\n");
                ibs.position(O_bitLengthBits);
                final int bitLengthBits = ibs.readInt(8/* nbits */);
                if (debug)
                    sb.append("bitLengthBits=" + bitLengthBits + "\n");
                codedValueOffsetBits = ibs.readInt(8/* nbits */);
                if (debug)
                    sb.append("codedValueOffsetBits=" + codedValueOffsetBits
                            + "\n");
                final long sumCodedValueBitLengths;
                if (codedValueOffsetBits != 0) {
                    sumCodedValueBitLengths = ibs.readInt(32/* nbits */);
                } else {
                    // note: not reported.
                    sumCodedValueBitLengths = 0L;
                }
                if (debug)
                    sb.append("sumCodedValueBitLengths="
                            + sumCodedValueBitLengths + "\n");
                // offset to the nulls[] (bit flags).
                O_nulls = O_bitLengthBits + 8 + 8
                        + (codedValueOffsetBits == 0 ? 0 : 32);
                if (debug) {
                    sb.append("O_nulls=" + O_nulls + "\n");
                    for (int i = 0; i < size; i++) {
                        sb.append("null[" + i + "]=" + ibs.readBit() + "\n");
                    }
                }
                final long O_bitLengths = O_nulls + size;
                if (debug)
                    sb.append("O_bitLengths=" + O_bitLengths + "\n");
                O_codedValues = O_bitLengths + (nsymbols * bitLengthBits);
                if (debug)
                    sb.append("O_codedValues=" + O_codedValues + "\n");
                O_codedValueOffsets = (codedValueOffsetBits == 0 ? 0L
                        : O_codedValues + sumCodedValueBitLengths);
                if (debug) {
                    sb.append("O_codedValueOffsets=" + O_codedValueOffsets
                            + "\n");
                    if (O_codedValueOffsets != 0L) {
                        ibs.position(O_codedValueOffsets);
                        for (int i = 0; i <= size; i++) {
                            sb.append("codedValueOffsets[" + i + "]="
                                    + ibs.readInt(codedValueOffsetBits) + "\n");
                        }
                    }
                }
                if (decoder == null) {
                    /*
                     * The decoder is reconstructed based on the assumption that
                     * the code is a canonical huffman code. For this case we
                     * only need the bit lengths of the codes in a
                     * non-decreasing order to recreate the decoder.
                     */
                    final int[] codeWordLengths = new int[nsymbols];
                    final int[] symbols = new int[nsymbols];
                    {
                        final long O_symbols = 16;
                        ibs.position(O_symbols);
                        if (bitCodedSymbols) {
                            throw new UnsupportedOperationException();
                        } else {
                            for (int i = 0; i < nsymbols; i++) {
                                symbols[i] = ibs.readInt(8/* nbits */);
                            }
                        }
                    }
                    {
                        int lastBitLength = 0;
                        ibs.position(O_bitLengths);
                        for (int i = 0; i < nsymbols; i++) {
                            final int deltaBitLength = ibs
                                    .readInt(bitLengthBits/* nbits */);
                            final int bitLength = lastBitLength
                                    + deltaBitLength;
                            codeWordLengths[i] = bitLength;
                            if (debug)
                                sb.append("codeWordLengths[" + i + "]="
                                        + bitLength + " (deltaBitLength="
                                        + deltaBitLength + ")" + "\n");
                            // symbols[i] = i;
                        }
                    }
                    this.decoder = new CanonicalFast64CodeWordDecoder(
                            codeWordLengths, symbols);
                    if (debug) {
                        // @todo decode the coded values!
                    }
                } else {
                    this.decoder = decoder;
                }
                if (debug)
                    System.err.println(sb.toString());
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            } finally {
                try {
                    ibs.close();
                } catch (IOException ex) {
                    log.error(ex);
                }
            }

        }

        final public int size() {

            return size;

        }

        /**
         * The capacity is equal to the size (the data are immutable).
         */
        final public int capacity() {

            return size;

        }

        final public boolean isEmpty() {

            return size == 0;

        }

        /**
         * Always returns <code>true</code> since {@link #size()} ==
         * {@link #capacity()} by definition for this class.
         */
        final public boolean isFull() {

            return true;

        }

        /**
         * @todo Canonical huffman codes support efficient search IFF (a) all
         *       byte values can be coded (including those with a zero frequency
         *       in the coded data) and (b) the codedValueOffset[] was included
         *       in the record. If we can not code each byte value, then we need
         *       to search against the decoded values. If the offset[] was not
         *       stored, then we need to do a linear scan.
         * 
         * @todo search must be disabled if <code>null</code>s are allowed. That
         *       should probably be a constraint realized by subclassing.
         */
        final public boolean isKeys() {

            return isKeys;

        }

        public ByteBuffer data() {

            return data;

        }

        public Decoder decoder() {

            return decoder;

        }

        // private int[] getSymbol2byte() {
        //
        // byte[] a = new byte[nsymbols];
        // // index into the buffer start after [nsymbols].
        // return data.get(symbol + 2);
        //
        // }

        // /**
        // * Lookup a symbol, returning the byte value for that symbol.
        // *
        // * @param symbol
        // * The symbol.
        // *
        // * @return The byte value.
        // */
        // private byte symbol2byte(final int symbol) {
        //
        // if (symbol < 0 || symbol >= nsymbols)
        // throw new IndexOutOfBoundsException();
        //
        // if (nsymbols <= PACKED_SYMBOL_TABLE_THRESHOLD) {
        //
        // // index into the buffer start after [nsymbols].
        // return data.get(symbol + 2);
        //
        // }
        //
        // /*
        // * FIXME We need to lookup the byte for that symbol. This
        // * information is stored as a series of 256 bit flags. Decode it
        // * into a packed symbol2byte[] and cache a reference to that array.
        // * Set the trigger threshold to a reasonable value (or explictly set
        // * it via a ctor) and test with byte[][]s which would be coded both
        // * ways.
        // */
        //
        // throw new UnsupportedOperationException();
        //
        // }

        public boolean isNull(final int index) {

            if (index < 0 || index >= size)
                throw new IllegalArgumentException();

            final InputBitStream ibs = new InputBitStream(
                    new ByteBufferInputStream(data));

            try {

                ibs.position(O_nulls + index);

                return ibs.readBit() != 0;

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            } finally {

                try {
                    ibs.close();
                } catch (IOException ex) {
                    log.error(ex);
                }

            }

        }

        /**
         * This computes the length of the decoded byte[] by counting the code
         * words for the coded value.
         */
        public int length(final int index) {

            if (index < 0 || index >= size)
                throw new IllegalArgumentException();

            final InputBitStream ibs = new InputBitStream(
                    new ByteBufferInputStream(data));

            try {

                /*
                 * Figure out whether or not this index is a null.
                 */
                ibs.position(O_nulls + index);

                final boolean isNull = ibs.readBit() != 0;

                if (isNull) {

                    // per the API.
                    throw new NullPointerException();

                }

                /*
                 * Figure out the bit length of the coded byte[].
                 * 
                 * Note: When we are allocating the byte[] for the caller we
                 * will scan the coded byte[] once to get the length of the
                 * decoded byte[] and then scan it again to decode the byte[].
                 */

                // get the bit offset of the start of this coded value.
                ibs.position(O_codedValueOffsets + ((long) index)
                        * codedValueOffsetBits);

                final long O_from = ibs.readLong(codedValueOffsetBits);

                // get the bit offset of the start of the next coded value.
                ibs.position(O_codedValueOffsets + (index + 1L)
                        * codedValueOffsetBits);

                final long O_to = ibs.readLong(codedValueOffsetBits);

                // the total length of this code in bits.
                final long codeLength = O_to - O_from;

                /*
                 * Figure out how many symbols are in the coded byte[].
                 */
                // position at the start of the coded byte[].
                ibs.position(O_from + O_codedValues);
                // reset the read bits counter (@todo necessary?)
                ibs.readBits(0L);
                // #of symbols in this coded byte[].
                int nsymbols = 0;
                while (ibs.readBits() < codeLength) {

                    decoder.decode(ibs);

                    nsymbols++;

                }

                return nsymbols;

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            } finally {

                try {
                    ibs.close();
                } catch (IOException ex) {
                    log.error(ex);
                }

            }

        }

        /**
         * This uses two passes over the code words for the given index. The
         * first pass figures out the #of bytes in the decoded byte[] and
         * allocates the byte[]. The second pass decodes into the allocated
         * byte[].
         */
        public byte[] get(final int index) {

            if (index < 0 || index >= size)
                throw new IllegalArgumentException();

            final InputBitStream ibs = new InputBitStream(
                    new ByteBufferInputStream(data));

            try {

                /*
                 * Figure out whether or not this index is a null.
                 */
                ibs.position(O_nulls + index);

                final boolean isNull = ibs.readBit() != 0;

                if (isNull) {

                    // The value is a null.
                    return null;

                }

                /*
                 * Figure out the bit length of the coded byte[].
                 * 
                 * Note: When we are allocating the byte[] for the caller we
                 * will scan the coded byte[] once to get the length of the
                 * decoded byte[] and then scan it again to decode the byte[].
                 */

                // get the bit offset of the start of this coded value.
                ibs.position(O_codedValueOffsets + ((long) index)
                        * codedValueOffsetBits);

                final long O_from = ibs.readLong(codedValueOffsetBits);

                // get the bit offset of the start of the next coded value.
                ibs.position(O_codedValueOffsets + (index + 1L)
                        * codedValueOffsetBits);

                final long O_to = ibs.readLong(codedValueOffsetBits);

                // the total length of this code in bits.
                final long codeLength = O_to - O_from;

                /*
                 * Figure out how many symbols are in the coded byte[].
                 */
                // position at the start of the coded byte[].
                ibs.position(O_from + O_codedValues);
                // reset the read bits counter (@todo necessary?)
                ibs.readBits(0L);
                // #of symbols in this coded byte[].
                int nsymbols = 0;
                while (ibs.readBits() < codeLength) {

                    decoder.decode(ibs);

                    nsymbols++;

                }

                /*
                 * Allocate an exact fit byte[] and decode into that byte[].
                 */

                // position at the start of the coded byte[].
                ibs.position(O_from + O_codedValues);
                {

                    // the decoded byte[].
                    final byte[] a = new byte[nsymbols];

                    // decode to symbol indices and convert symbol indices to
                    // bytes.
                    for (int i = 0; i < nsymbols; i++) {

                        // final int symbol = decoder.decode(ibs);
                        //
                        // a[i] = (byte) symbol2byte(symbol);

                        a[i] = (byte) decoder.decode(ibs);

                    }

                    // done.
                    return a;

                }

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            } finally {

                try {
                    ibs.close();
                } catch (IOException ex) {
                    log.error(ex);
                }

            }

        }

        /**
         * This decodes the value at the specified index in a single pass onto
         * the caller's stream.
         */
        public int copy(final int index, final OutputStream os) {

            if (index < 0 || index >= size)
                throw new IllegalArgumentException();

            final InputBitStream ibs = new InputBitStream(
                    new ByteBufferInputStream(data));

            try {

                /*
                 * Figure out whether or not this index is a null.
                 */
                ibs.position(O_nulls + index);

                final boolean isNull = ibs.readBit() != 0;

                if (isNull) {

                    // per the API.
                    throw new NullPointerException();

                }

                /*
                 * Figure out the bit length of the coded byte[].
                 * 
                 * Note: When we are allocating the byte[] for the caller we
                 * will scan the coded byte[] once to get the length of the
                 * decoded byte[] and then scan it again to decode the byte[].
                 */

                // get the bit offset of the start of this coded value.
                ibs.position(O_codedValueOffsets + ((long) index)
                        * codedValueOffsetBits);

                final long O_from = ibs.readLong(codedValueOffsetBits);

                // get the bit offset of the start of the next coded value.
                ibs.position(O_codedValueOffsets + (index + 1L)
                        * codedValueOffsetBits);

                final long O_to = ibs.readLong(codedValueOffsetBits);

                // the total length of this code in bits.
                final long codeLength = O_to - O_from;

                /*
                 * Copy the decoded symbols into the buffer.
                 */

                // position at the start of the coded byte[].
                ibs.position(O_from + O_codedValues);

                // reset the read bits counter (@todo necessary?)
                ibs.readBits(0L);

                // #of symbols in this coded byte[].
                int nsymbols = 0;

                while (ibs.readBits() < codeLength) {

                    // final int symbol = decoder.decode(ibs);
                    //
                    // final byte b = (byte) symbol2byte(symbol);

                    final byte b = (byte) decoder.decode(ibs);

                    os.write(b);

                    nsymbols++;

                }

                // #of bytes written onto the buffer.
                return nsymbols;

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            } finally {

                try {
                    ibs.close();
                } catch (IOException ex) {
                    log.error(ex);
                }

            }

        }

        /**
         * FIXME write method based on mixture of linear scan and binary search
         * depending on the #of values in the logical array. Make sure that the
         * other methods (length(), getKey(), copyKey()) can also handle cases
         * when codedValueOffset[] is not defined). Implement the appropriate
         * interface for search().
         * 
         * <pre>
         *      There are some wrinkles for performing search in the
         *      compressed key space.  The main issue is if the key contains a
         *      byte value which is not in the code book, then we can not encode
         *      the key.
         * 
         *      1. We could still note a &quot;miss&quot; on a search simply because the
         *         byte value did not correspond to a code word.  When searching
         *         in a leaf this is enough.  If we are doing a lookup, contains
         *         or delete, then the key was not found and we are done.  If we
         *         are doing an insert, then the leaf needs to be converted into
         *         a mutable leaf.  At that point we could redo the search and
         *         locate the index where the new key would be inserted.
         * 
         *      2. We can always do a search in the decompressed key space.  For
         *         example, by de-serializing the keys one at a time.
         * 
         *      3. For a node (non-leaf index node) we always need to find the
         *         index of the first separator key LTE the probe key.  This will
         *         probably incur a lot of overhead if it requires us to
         *         decompress the keys in the node in order to perform the
         *         search.
         *     
         *      4. Another option would be to include zero (0) frequency counts
         *         for each of the non-appearing byte values.  This would give us
         *         a complete code and make it possible to do search directly in
         *         the compressed code space.
         * 
         *      5. We will have to build the Coder to perform search.
         * 
         *     Given these wrinkles, the prefix coding may be better for the
         *     B+Tree keys even though we can search in the compress canonical
         *     huffman coding.
         * </pre>
         * 
         * @param probe
         * @return
         * 
         *         FIXME Change the coding of the bitLength[] per the
         *         recommendations of the article on efficient decoding of
         *         prefix codes: min, max, #of code words of each length.
         *         This will be significantly fewer bits when we are coding
         *         all bytes for search.
         */
        public int search(final byte[] probe) {

            if(!isKeys())
                throw new UnsupportedOperationException();

            // Search is not implemented yet.
            throw new UnsupportedOperationException();

        }

        // /**
        // */
        // protected PrefixCoder getCoder() {
        //
        // // double-checked locking.
        // if (coder == null) {
        // synchronized (this) {
        // if (coder == null) {
        //
        // // coder = new Fast64CodeWordCoder( codeWord,
        // // longCodeWord );
        //
        // throw new UnsupportedOperationException();
        //
        // }
        // }
        // }
        //
        // return coder;
        //
        // }
        //
        // private volatile PrefixCoder coder;
        
    }

}
