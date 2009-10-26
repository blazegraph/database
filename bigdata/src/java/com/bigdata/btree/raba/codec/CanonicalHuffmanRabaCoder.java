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
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.compression.CanonicalFast64CodeWordDecoder;
import it.unimi.dsi.compression.Decoder;
import it.unimi.dsi.compression.HuffmanCodec;
import it.unimi.dsi.compression.PrefixCoder;
import it.unimi.dsi.compression.HuffmanCodec.DecoderInputs;
import it.unimi.dsi.fastutil.bytes.Byte2IntOpenHashMap;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;

/**
 * This class provides (de-)compression for logical byte[][]s based on canonical
 * Huffman codes. Canonical huffman codes preserve the alphabetic order of the
 * original values. However, they are often used because it is possible to
 * transmit the dictionary using fewer bits by sending only the bitLength[] for
 * the code words. The {@link CanonicalHuffmanRabaCoder} can be used for keys or
 * values and supports efficient search of the coded keys.
 * 
 * <h3>Record format</h3>
 * 
 * The encoded data are represented in the buffer as follows:
 * 
 * <pre>
 * version:byte     The version identifier for this record format (8 bits,
 *                  which allows for 256 format revisions).
 * 
 * isKeys:1         Bit flag indicates whether the record is coding B+Tree keys
 *                  or B+Tree values.  When coding values only the non-zero
 *                  byte frequency counts are used to compute the dictionary
 *                  and nulls are permitted.  When coding keys, the dictionary
 *                  includes codes for all 256 possible byte values so we may
 *                  code search keys and nulls are not permitted.
 * 
 * isSymbolTable:1
 *                  A bit flag whose value is 1 iff the symbols are given as
 *                  a packed symbol[] and 0 if all possible byte values were
 *                  coded.  Note that the implementation MAY choose to code
 *                  byte values with a zero frequency if MOST byte values are
 *                  used since that does not degrade the code by much and it
 *                  allows us to eliminate a &tilde;256 bytes from the data record.
 * 
 * isOffsetArray:1  A bit flag whose value is 1 iff the codedValueOffset[] is
 *                  stored.  At this time, this array is always stored.  Either
 *                  the offset[] or the #of symbols in each coded value MUST be
 *                  stored in order for us to compute the end of each sequence
 *                  of codeWord[]s for a given coded value. 
 * 
 * isByteAlignedOffset:1
 *                  A bit flag whose value is 1 iff the codedValueOffset[] is
 *                  byte aligned.  When true, the start of the array will fall
 *                  on a byte boundary and each array element will be a multiple
 *                  of 8 bits.
 *                  
 * unused:4         unused bit flags.
 * 
 * size:int31       The #of elements in the logical byte[][].
 * 
 * nsymbols:uint9   There are at most 256 distinct symbols, which are the
 *                  distinct possible byte values (9 bits, which allows
 *                  for an empty leaf or node with no byte values used as
 *                  well as a leaf or node with all 256 byte values used).
 * 
 * -- note: at this point the record is still byte aligned --
 * 
 * symbol2byte:byte[]
 *                  The symbol2byte array.  The index into the array is the
 *                  symbol, which is correlated with the frequency[] used to
 *                  build the code.  The value is the byte corresponding to that
 *                  symbol.  There are nsymbols entries in the array.  The array
 *                  is present IFF isSymbolTable is true.
 *                  
 *                  O_symbols := 48 bits.
 * 
 * -- note: at this point the record is still byte aligned --
 * 
 * codedValueOffsetBits:uint8
 *                  The width in bits of the integers used to code the
 *                  codedValueOffset[].
 * 
 *                  O_codedValueOffsetBits := [ibs.readBits];
 * 
 * sumCodedValueBitLengths:uint32
 *                  The sum of the bit lengths of the coded values.  This is
 *                  a 32bit unsigned integer, which is sufficient to code up
 *                  bit lengths of up to 512MB.  This field IS NOT present 
 *                  if the codeOffsetBits is ZERO (0) since the field is only
 *                  used to compute the bit offset of the codeOffset[].
 * 
 *                  O_sumCodedValueBitLengths := O_codedValueOffsetBits + 8;
 * 
 * nulls[]          A vector of [nvalues] bit flags IFF isKeys==false.  A flag
 *                  is a ONE (1) iff the corresponding byte[] in the logical
 *                  byte[][] was a null.  A null is coded as a sequence of ZERO
 *                  (0) code words.  However, empty byte[]s are also permitted 
 *                  for B+Tree values (but not for B+Tree keys).  Therefore you
 *                  MUST test the nulls[] to distinguish between a null byte[]
 *                  and an empty byte[].  This field IS NOT present when keys
 *                  are coded.
 * 
 *                  O_nulls :=  O_codedValueOffsetBits + 8 + (codedValueOffsetBits==0?0:32);
 * 
 * -- note: if nsymbols==0 then this is the end of the record --
 * 
 * decoderInputs:=
 * length[],        The bit length of each code word in the assigned order.
 * symbol[],        The correlated symbol indices.
 * codeWord[0].     The shortest code word.
 * 
 *                  These length[] and the symbol[] data are written out together
 *                  using a compact representation.  The length of the shortest
 *                  code word length is given by length[0].  Since we are using
 *                  a canonical huffman code, all we need is the shortest code
 *                  and the length[] to regenerate the entire code.
 *                                     
 *                  O_decoderInputs := O_symbols + (isSymbolTable?0:nsymbols*8)
 *                  
 * codedValue:bit[] The coded values given as a sequence of code words. The bit
 *                  length of this field is given by sumCodedValueBitLengths.
 * 
 *                  O_codedValue[] := O_nulls + size IFF values are coded
 *                  
 *                  -or-
 *                  
 *                  O_codedValue[] := O_nulls IFF keys are coded.
 * 
 * codeValueOffset[]
 *                  The bit offset to the start of each coded value in the
 *                  codedValue[].  The offsets are relative to the start of
 *                  the first coded value.
 *                  
 *                  While the delta in the offsets could be represented more
 *                  efficiently, the offsets are represented directly so that
 *                  we may avoid reading the entire codeOffset[] into memory.
 *                  This array is present iff isOffsetArray is true.
 * 
 *                  O_codedValueOffset[] := O_codedValue[] + sumCodedValueBitLengths
 * </pre>
 * 
 * @see HuffmanCodec
 * 
 * @see http://en.wikipedia.org/wiki/Huffman_coding
 * @see http://en.wikipedia.org/wiki/Canonical_Huffman_code
 * 
 * @todo it could be useful to byte align the codedValueOffset[] IF we also
 *       coded the offsets using nbits, where nbits was a multiple of 8.
 * 
 *       FIXME Generalize to allow alphabets of size GT 256 so we can reuse this
 *       to directly encode the long[] keys of the leaves in the RDF DB. This
 *       will still be an {@link IRaba} (a random access byte[]). The difference
 *       is that each symbol will correspond to a sequence of 8 bytes. So what
 *       we need is a means to decouple the generation of the frequency[], the
 *       assumption that alphabets of 256 are "complete", and the application of
 *       the codec and the decoder, which must process 8 bytes at a time.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CanonicalHuffmanRabaCoder implements IRabaCoder, Externalizable {

    protected static final Logger log = Logger
            .getLogger(CanonicalHuffmanRabaCoder.class);

    private static final transient boolean debug = log.isDebugEnabled();

    /**
     * The original serialization version for the coded data record.
     */
    final protected static transient byte VERSION0 = 0x00;

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        // No state.
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        // No state.
    }

    final public boolean isKeyCoder() {

        return true;

    }

    final public boolean isValueCoder() {

        return true;

    }
    
    public static transient final CanonicalHuffmanRabaCoder INSTANCE = new CanonicalHuffmanRabaCoder();

    /**
     * 
     */
    public CanonicalHuffmanRabaCoder() {

    }

    /**
     * Write out the optional packed symbol table (symbol2byte). When present,
     * the symbol table is written as a sequence of the in use byte values in
     * the unsigned byte order (this is the order in which the frequency[] was
     * specified).
     * 
     * @param symbol2byte
     *            The symbol table.
     * @param obs
     *            The symbol table is written on this bit stream.
     */
    protected void writeSymbolTable(final Symbol2Byte symbol2byte,
            final OutputBitStream obs) throws IOException {

        assert symbol2byte != null;

        assert obs != null;

        final int nsymbols = symbol2byte.getSymbolCount();

        assert nsymbols <= 256;

        // for each symbol, write the byte
        for (int i = 0; i < nsymbols; i++) {

            obs.writeInt(symbol2byte.symbol2byte(i), 8/* len */);

        }

    }

    /**
     * Write a compact minimum representation of the data required to
     * reconstruct the decoder (bit lengths and correlated symbols). The data
     * are written out as follows:
     * 
     * <pre>
     * min
     * max
     * count, symbol(s)
     * </pre>
     * 
     * where <code>min</code> is the bit length of the shortest code word;
     * <code>max</code> is the bit length of the longest code word;
     * <code>count</code> is the #of code words of a given length; and
     * <code>symbol(s)</code> are the symbols associated with each code word.
     * All values are nibble coded and will generally fit in 1-2 bytes each.
     * 
     * @param decoderInputs
     *            This contains both the bit lengths of the canonical huffman
     *            code and the symbols assigned to each code word.
     * @param obs
     *            The output bit stream.
     * @param sb
     *            Debugging information is added to this buffer (optional).
     * 
     * @throws IOException
     * 
     * @see {@link DecoderInputs}
     */
    static protected void writeDecoderInputs(final DecoderInputs decoderInputs,
            final OutputBitStream obs, final StringBuilder sb) throws IOException {

        if (decoderInputs == null)
            throw new IllegalArgumentException();
        
        if (obs == null)
            throw new IllegalArgumentException();

        final int[] length = decoderInputs.getLengths();
        
        final int[] symbol = decoderInputs.getSymbols();
        
        if (length == null)
            throw new IllegalArgumentException();
        
        if (symbol == null)
            throw new IllegalArgumentException();
        
        if (length.length != symbol.length)
            throw new IllegalArgumentException();

        final int nsymbols = length.length;

        final int min = length[0];

        final int max = length[nsymbols - 1];

        final int nsizes = max - min + 1;

        assert nsizes >= 0;

       if (sb != null)
            sb.append("min=" + min + ", max=" + max+"\n");

        // the minimum code word bit length.
        obs.writeNibble(min);

        // the maximum code word bit length.
        obs.writeNibble(max);

        // the bit length of the current code word.
        int lastSize = min;

        // #of code words having the same bit length as the current code word.
        int sizeCount = 0;

        // the next symbol (index) to be written.
        int nextSymbol = 0;

        /*
         * Write an array consisting of the #of code words for each bit length
         * between [min,max], inclusive.
         */
        for (int i = 0; i < nsymbols; i++) {
            final int thisSize = length[i];
            assert thisSize >= lastSize;
            if (thisSize == lastSize) {
                sizeCount++;
            } else {
                // loop handles bit lengths w/o assigned code words.
                while (thisSize >= lastSize + 1) {
                    assert thisSize >= lastSize + 1;
                    assert sizeCount >= 0;
                    if (sb != null)
                        sb.append("codeSize=" + lastSize + ", sizeCount="
                                + sizeCount + ", symbols=[");
                    obs.writeNibble(sizeCount);
                    for (int j = nextSymbol; j < i; j++) {
                        if (sb != null)
                            sb.append(" " + symbol[j]);
                        obs.writeNibble(symbol[j]);
                    }
                    if (sb != null)
                        sb.append("]\n");
                    sizeCount = (thisSize == lastSize + 1 ? 1 : 0);
                    nextSymbol = i;
                    lastSize++;
                }
            }
        }
        if (sizeCount > 0) {
            // make sure that we write out the last count.
            if (sb != null)
                sb.append("codeSize=" + lastSize + ", sizeCount=" + sizeCount
                        + ", symbols=[");
            obs.writeNibble(sizeCount);
            for (int j = nextSymbol; j < nsymbols; j++) {
                if (sb != null)
                    sb.append(" " + symbol[j]);
                obs.writeNibble(symbol[j]);
            }
            if (sb != null)
                sb.append(" ]\n");
        }
        assert lastSize == max;

        // write out the shortest code word.
        obs.write(decoderInputs.getShortestCodeWord().iterator());

        if (sb != null) {
            sb.append("shortestCodeWord="
                    + decoderInputs.getShortestCodeWord()+"\n");
        }
        
    }

    /**
     * Reconstruct the {@link DecoderInputs} from the data written by
     * {@link #writeDecoderInputs(BitVector[], OutputBitStream)}.
     * 
     * @param nsymbols
     *            The #of symbols.
     * @param ibs
     *            The input bit stream.
     * @param sb
     *            Debugging information is added to this buffer (optional).
     * 
     * @return The decoded bit lengths and the corresponding symbol indices for
     *         the canonical huffman code.
     * 
     * @throws IOException
     */
    static protected DecoderInputs readDecoderInputs(final int nsymbols,
            final InputBitStream ibs, final StringBuilder sb)
            throws IOException {

        final int min = ibs.readNibble();

        final int max = ibs.readNibble();

        if (sb != null)
            sb.append("min=" + min + ", max=" + max+"\n");

        final int[] length = new int[nsymbols];
        final int[] symbol = new int[nsymbols];

        // the current code length
        int codeSize = min;
        int lastSymbol = 0;
        while (codeSize <= max) {
            final int sizeCount = ibs.readNibble();
            if (sb != null)
                sb.append("codeSize="+codeSize+", sizeCount="+sizeCount+", symbols=[");
            for (int i = 0; i < sizeCount; i++, lastSymbol++) {
                final int tmp = ibs.readNibble();
                if (sb != null)
                    sb.append(" "+tmp);
                length[lastSymbol] = codeSize;
                symbol[lastSymbol] = tmp;
            }
            if (sb != null)
                sb.append(" ]\n");
            codeSize++;
        }

        final int shortestCodeWordLength = length[0];

        final BitVector shortestCodeWord = LongArrayBitVector.getInstance()
                .length(shortestCodeWordLength);

        for (int i = shortestCodeWordLength-1; i >= 0; i--) {

            shortestCodeWord.set(i, ibs.readBit());
            
        }

        if (sb != null) {
            sb.append("shortestCodeWord=" + shortestCodeWord+"\n");
        }

        return new DecoderInputs(shortestCodeWord, length, symbol);

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
    protected long writeCodedValues(final PrefixCoder coder,
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

                }

            }

        }

        if (codedValueOffset != null)
            codedValueOffset[nvalues] = bitsWritten;

        if (log.isDebugEnabled())
            log
                    .debug("codedValueOffset[]="
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

         /**
         * The codec used to encode and decode the logical byte[][].
         */
        abstract public HuffmanCodec codec();

        /**
         * The data required to reconstruct the decoder.
         */
        abstract public DecoderInputs decoderInputs();

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

                    sb
                            .append("codeWord: "
                                    + v
                                    + ", symbol="
                                    + symbol
                                    + ", value="
                                    + b
                                    + (b >= 32 && b < 127 ? " (" + (char) b
                                            + ")" : "") + "\n");

                    symbol++;

                }

            }

            return sb.toString();

        }

        /**
         * Return a dense array of the non-zero frequency counts in byte value
         * order. The length of the array is the #of distinct symbols appearing
         * in the input.
         * 
         * @param raba
         *            The logical byte[][].
         * 
         * @return The packed frequency counts.
         */
        protected int[] getPackedFrequencyCount(final IRaba raba) {

            final int[] frequency = new int[Byte.MAX_VALUE - Byte.MIN_VALUE + 1];

            final int size = raba.size();

            int nsymbols = 0;

            for (int i = 0; i < size; i++) {

                final byte[] a = raba.get(i);

                if (a != null) {

                    for (byte b : a) {

                        if (frequency[b - Byte.MIN_VALUE]++ == 1) {

                            nsymbols++;

                        }

                    }

                }

            }

            final int[] packedFreq = new int[nsymbols];

            for (int i = 0, j = 0; i < frequency.length; i++) {

                if (frequency[i] != 0) {

                    packedFreq[j++] = frequency[i];

                }

            }

            return frequency;

        }

        /**
         * Create a frequency table reporting the #of occurrences of for every
         * possible byte value.
         * 
         * @param raba
         *            The data.
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

            if (frequency == null)
                throw new IllegalArgumentException();

            if (frequency.length != 256)
                throw new IllegalArgumentException();

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
     * Sets up for coding an {@link IRaba} representing B+Tree values. For this
     * case the code book is generated from only the non-zero frequency counts.
     * This results in a more compact code, but the code can not be searched
     * since it only the byte values with non-zero frequencies have assigned
     * code words.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * 
     *          FIXME If most byte values are used, then simply code them all.
     *          This will be more efficient (smaller data record, no indirection
     *          through the symbol table).
     */
    protected static class RabaCodingSetup extends AbstractCodingSetup {

        /** The #of distinct symbols (distinct byte values) actually used. */
        final int nsymbols;

        /**
         * The frequency counts for only those byte values whose frequency count
         * is non-zero. The frequency counts appear in increasing byte value
         * order. This allows us to decode using a {@link #symbol2byte} map in
         * the same order.
         */
        private final int[] packedFrequency;

        private final DecoderInputs decoderInputs;
        
        public DecoderInputs decoderInputs() {

            return decoderInputs;
            
        }

        /** The canonical huffman codec. */
        private final HuffmanCodec codec;

        @Override
        public HuffmanCodec codec() {
            return codec;
        }

        private final Byte2IntOpenHashMap byte2symbol;

        /** The packed symbol table (symbol to byte). */
        private final byte[] symbol2byte;

        @Override
        final public int getSymbolCount() {

            return nsymbols;

        }

        final public int byte2symbol(final byte b) {

            return byte2symbol.get(b);

        }

        final public byte symbol2byte(final int symbol) {

            return (byte) symbol2byte[symbol];

        }

        public RabaCodingSetup(final IRaba raba) {

            if (raba == null)
                throw new IllegalArgumentException();

//            if (raba.isKeys())
//                throw new IllegalArgumentException();
            
            // The #of byte[] values to be coded.
            final int size = raba.size();

            {

                /*
                 * Array of frequency counts for all possible byte values.
                 */
                final int[] frequency = new int[256];

                int nsymbols = 0;
                byte lastByte = 0;

                for (int i = 0; i < size; i++) {

                    final byte[] a = raba.get(i);

                    if (a != null) {

                        for (byte b : a) {

                            if (frequency[b - Byte.MIN_VALUE]++ == 0) {

                                lastByte = b;
                                
                                nsymbols++;

                            }

                        }

                    }

                }

                if (nsymbols == 1) {
                    /*
                     * FIXME This is a hack to workaround a ctor bug in the
                     * HuffmanCodec when nsymbols==1 (a bug in the
                     * CanonicalFast64CodeWordDecoder ctor has since been fixed,
                     * but one remains in the HuffmanCodec ctor).
                     * 
                     * The workaround pretends that a 2nd symbol is present and
                     * assigns it a non-zero frequency count to ensure that it
                     * is preserved by the packedFrequency table. If that bug is
                     * fixed, then this code block can simply be commented out.
                     * 
                     * The choice of that 2nd symbol is arbitrary. Since there
                     * is only one symbol present, we use index #0 if the symbol
                     * is non-zero and use index #1 if the symbol is zero.
                     */
                    final int fakeByteIndex = (lastByte == 0 ? 1 : 0);
                    frequency[fakeByteIndex] = 1;
                    nsymbols++;
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

            if (nsymbols > 0) {
                
                /*
                 * Generate a canonical huffman code. The input is the packed
                 * frequency data (only symbols which occur at least once). The
                 * packed frequency counts appear in non-increasing frequency order.
                 * Therefore the assigned canonical codeWord[] will already be in
                 * non-decreasing code length order.
                 */

                decoderInputs = new DecoderInputs();

                codec = new HuffmanCodec(packedFrequency, decoderInputs);

                if (log.isDebugEnabled()) {
                    log
                            .debug("\n"
                                    + printCodeBook(codec.codeWords(), this/* Symbol2Byte */));
                }

            } else {
                
                /*
                 * Don't bother to generate the codec.
                 */
                
                decoderInputs = null;
                
                codec = null;
                
            }

        }

    }

    public AbstractFixedByteArrayBuffer encode(final IRaba raba,
            final DataOutputBuffer buf) {

        return encodeLive(raba, buf).data();
        
    }
    
    public ICodedRaba encodeLive(final IRaba raba, final DataOutputBuffer buf) {

        final AbstractCodingSetup setup = new RabaCodingSetup(raba);
        
        final StringBuilder sb = debug ? new StringBuilder("\n") : null;

        // The #of byte[] values to be coded.
        final int size = raba.size();

        // The #of distinct symbols (distinct byte values) actually used.
        final int nsymbols = setup.getSymbolCount();

        // Total #of bits in the coded values.
        // FIXME this codes the data twice.
        final long sumCodedValueBitLengths = nsymbols == 0 ? 0
                : getSumCodedValueBitLengths(setup.codec().codeWords(),
                        raba, (Byte2Symbol) setup);

        /*
         * The #of bits per element in the codedValueOffset[]
         * 
         * @todo iff nvalues>16 and SEQUENTIAL flag was not specified by
         * called.
         * 
         * @todo support isByteAlignedOffsets
         */
        final int codedValueOffsetBits = nsymbols == 0 ? 0 : Fast
                .mostSignificantBit(sumCodedValueBitLengths) + 1;

        /*
         * Note: this initialCapacity estimate appears to be more than we need,
         * however if the caller is reusing their buffer then there should be
         * little waste associated with it over time.
         */
        final int initialCapacity = (int) (512 + sumCodedValueBitLengths + (size + 1)
                * codedValueOffsetBits);

        buf.ensureCapacity(initialCapacity);
        
        /*
         * Write out the record on a bit stream backed by a byte[] buffer.
         */
        final int O_origin = buf.pos(); // byte offset of the origin.
        if (debug)
            sb.append("O_origin=" + O_origin + "\n");
        try {

            // The serialization version for the record.
            final int version = VERSION0;

            final OutputBitStream obs = buf.getOutputBitStream();
//                new OutputBitStream(buf, 0/* bufSize */, false/* reflectionTest */);

            // The record version identifier.
            obs.writeInt(version, 8/* nbits */);
            if (debug)
                sb.append("version=" + version + "\n");

            // Indicates if we are coding keys vs values.
            obs.writeBit(raba.isKeys());

            // Indicates if we write a packed symbol table into the record. 
            final boolean isSymbolTable = setup.getSymbolCount() != 256;
            obs.writeBit(isSymbolTable);

            // Indicates whether the codedValueOffset[] is stored.
            final boolean isOffsetArray = true;
            obs.writeBit(isOffsetArray);
            
            // Indicates if we will write the codedValueOffset[] with byte alignment.
            final boolean isByteAlignedOffsets = false;
            obs.writeBit(isByteAlignedOffsets);
            
            // unused bit flags.
            obs.writeInt(0, 4);
            if (debug) {
                sb.append("isKeys=" + raba.isKeys() + "\n");
                sb.append("isSymbolTable=" + isSymbolTable + "\n");
                sb.append("isOffsetArray=" + isOffsetArray + "\n");
                sb.append("isByteAlignedOffsets=" + isByteAlignedOffsets + "\n");
            }
            
            // The #of elements in the logical byte[][].
            obs.writeInt(size, 31);
            if (debug)
                sb.append("size=" + size+ "\n");
            
            // The #of symbols in the code.
            obs.writeInt(nsymbols, 9/* nbits */);
            assert obs.writtenBits() == CodedRabaImpl.O_symbols;
            if (debug)
                sb.append("nsymbols=" + nsymbols + "\n");
            
            if(isSymbolTable) {
                // Write out the optional symbol2byte[].
                writeSymbolTable(setup/* Symbol2Byte */, obs);
            }
            
            // the record should still be byte aligned.
            assert obs.writtenBits() % 8 == 0;
            assert obs.writtenBits() == CodedRabaImpl.O_symbols
                    + (isSymbolTable ? nsymbols * 8 : 0);

            final long O_codedValueOffsetBits = obs.writtenBits();
            obs.writeInt(codedValueOffsetBits, 8/* nbits */);
            if (debug) {
                sb.append("O_codedValueOffsetBits="
                        + O_codedValueOffsetBits + "\n");
                sb.append("codedValueOffsetBits=" + codedValueOffsetBits
                        + "\n");
            }

            if (codedValueOffsetBits != 0) {
                obs.writeLong(sumCodedValueBitLengths, 32/* nbits */);
                if (debug)
                    sb.append("sumCodedValueBitLengths="
                            + sumCodedValueBitLengths + "\n");
            }

            // nulls[] : bit flags identifying null byte[]s.
            final long O_nulls = obs.writtenBits();
            assert O_nulls == O_codedValueOffsetBits + 8
                    + (codedValueOffsetBits == 0 ? 0 : 32);
            if (!raba.isKeys()) {
                if (debug)
                    sb.append("O_nulls=" + O_nulls + "\n");
                for (int i = 0; i < size; i++) {
                    final boolean isNull = raba.isNull(i);
                    obs.writeBit(isNull);
                    if (debug)
                        sb.append("null[" + i + "]=" + isNull + "\n");
                }
            }

            // The bit length of the decoder inputs.
            final long decoderInputsBitLength;
            if (nsymbols == 0) {
    
                // not present in the coded data record.
                decoderInputsBitLength = 0;
                
            } else {

                // write the code word length[], the correlated symbol[], and the
                // shortest code word. 
                final long O_decoderInputs = obs.writtenBits();
                if (debug)
                    sb.append("O_decoderInputs=" + O_decoderInputs+"\n");
                writeDecoderInputs(setup.decoderInputs(), obs, sb);

                // Write out the coded values.
                final long O_codedValues = obs.writtenBits();
                decoderInputsBitLength = O_codedValues - O_decoderInputs;
                if (debug)
                    sb.append("O_codedValues=" + O_codedValues + "\n");
                //            assert O_codedValues == O_nulls + (raba.isKeys() ? 0 : size);
                final long[] codedValueOffset = (codedValueOffsetBits == 0 ? null
                        : new long[size + 1]);
                final long sumCodedValueBitLengths2 = writeCodedValues(setup
                        .codec().coder(), raba, (Byte2Symbol) setup,
                        codedValueOffset, obs);
                assert sumCodedValueBitLengths == sumCodedValueBitLengths2 : "sumCodedValueBitLengths="
                        + sumCodedValueBitLengths
                        + " != sumCodedValueBitLengths2="
                        + sumCodedValueBitLengths2;
                if (codedValueOffset != null)
                    assert codedValueOffset[size] == sumCodedValueBitLengths;
    
                // Write out the codedValueOffset[].
                final long O_codedValueOffsets;
                if (codedValueOffsetBits != 0) {
    //                // force byte alignment.
    //                final long tmp = obs.writtenBits();
    //                obs.align();
                    O_codedValueOffsets = obs.writtenBits();
                    assert O_codedValueOffsets == O_codedValues
                            + sumCodedValueBitLengths;
                    if (debug)
                        sb.append("O_codedValueOffsets=" + O_codedValueOffsets
                                + "\n");
                    for (int i = 0; i < codedValueOffset.length; i++) {
                        final long offset = codedValueOffset[i];
                        obs.writeLong(offset, codedValueOffsetBits/* nbits */);
                        if (debug)
                            sb.append("codedValueOffsets[" + i + "]=" + offset
                                    + "\n");
                    }
                } else {
                    // Not included in the record.
                    O_codedValueOffsets = 0L;
                }
                
            }// if(nsymbols>0)
            
            // done writing (will byte align the OBS).
            obs.flush();

            if (debug) {
                sb.append("bytesWritten=" + (buf.pos() - O_origin) + "\n");
                log.debug(sb.toString());
            }
            
            // a slice with just the coded data record.
            final AbstractFixedByteArrayBuffer slice = buf.slice(//
                    O_origin, buf.pos() - O_origin);

            if (nsymbols == 0) {

                return new CodedRabaImpl(slice, null/* decoder */, 0/* decoderInputsBitLength */);
            
            }
            
            return new CodedRabaImpl(slice, setup.codec().decoder(),
                    decoderInputsBitLength);

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    public ICodedRaba decode(final AbstractFixedByteArrayBuffer data) {

        return new CodedRabaImpl(data);

    }

    /**
     * Decoder.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    // @todo private
    public static class CodedRabaImpl extends AbstractCodedRaba {

        /**
         * The <em>byte</em> offset to the packed symbol2byte table relative to
         * the start of the slice.
         */
        private static final int BYTE_O_symbols = 7;

        /**
         * The bit offset to the packed symbol2byte table (relative to the start
         * of the data record).
         */
        private static final long O_symbols = BYTE_O_symbols * 8;
        
        /**
         * The entries in the logical byte[][].
         */
        private final int size;

        /**
         * If the logical byte[][] contains B+Tree keys vs B+Tree values.
         */
        private final boolean isKeys;

        /**
         * <code>true</code> iff the packed symbol2byte[] is present in the
         * record. When <code>false</code>, all byte values were coded and the
         * symbol index is directly transformed into the corresponding byte
         * value by {@link #symbol2byte(int)}.
         */
        private final boolean isSymbolTable;
        
        private final AbstractFixedByteArrayBuffer data;

//        /**
//         * A reference to the backing byte array. Offsets into this array MUST
//         * be adjusted for the starting offset of the slice. Direct access to
//         * this byte[] is used to reduce the cost of operations such lookup in
//         * the packed symbol2byte table.
//         * 
//         * @todo final
//         */
//        private byte[] array;
//
        /**
         * The offset into the coded data record of the first byte of the slice
         * for the data record.
         * <p>
         * Note: The reference to the backing <code>byte[]</code> can be changed
         * by {@link ByteArrayBuffer#trim()}. In order to protect against such
         * changes, we only store the offset into the data record (which is
         * immutable across trim()).
         * 
         * @todo if we implement a compacting store for the coded data records
         *       then the offset could change and we would also have to protect
         *       the API level operations of all {@link ICodedRaba}s against
         *       relocation of the backing data record due to asynchronous
         *       compacting operations.
         */
        private final int aoff;

//        /** @todo hack! resets these fields if the slice gets trimmed. */
//        public void trimmedSlice() {
//
//            this.array = data.array();
//            
//        }

        /**
         * The decoder, which is always available.
         */
        private final Decoder decoder;

        /**
         * The #of symbols in the coded alphabet. For B+Tree keys this is always
         * 256. Otherwise it is the #of distinct byte values appearing in the
         * B+Tree values.
         */
        private final int nsymbols;

        /**
         * The bit offset to the start of the nulls[], which is coded IFF the
         * logical byte[][] was representing B+Tree values rather than B+Tree
         * keys (no nulls). These are bit flags indicating whether the
         * corresponding entry in the logical byte[][] is a <code>null</code>.
         * You must consult these bit flags in order to distinguish a
         * <code>null</code> from a zero length byte[].
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
         * Constructor used to decode a data record.
         * 
         * @param data
         *            The record containing the coded data.
         */
        public CodedRabaImpl(final AbstractFixedByteArrayBuffer data) {

            this(data, null/* decoder */, 0L/* decoderInputsBitLength */);

        }

        /**
         * Constructor used when encoding a data record (more information is
         * available from the caller's context).
         * 
         * @param data
         *            The record containing the coded data.
         * @param decoder
         *            The decoder (optional). When not given the decoder is
         *            reconstructed from the record.
         * @param decoderInputsBitLength
         *            The bit length of the {@link DecoderInputs} in the coded
         *            data record. This information is used to skip beyond the
         *            {@link DecoderInputs} without having to read them from the
         *            {@link InputBitStream}.
         */
        public CodedRabaImpl(final AbstractFixedByteArrayBuffer data,
                final Decoder decoder, final long decoderInputsBitLength) {

            if (data == null)
                throw new IllegalArgumentException();

            if (decoder != null && decoderInputsBitLength == 0)
                throw new IllegalArgumentException();
            
            this.data = data;
//            if (decoder == null) {
//                // context is NOT encodeLive() so trim() is NOT used.
//                this.array = data.array();
                this.aoff = data.off();
//            } else {
//                this.array = null;
//                this.aoff = Integer.MIN_VALUE;
//            }

            final StringBuilder sb = debug ? new StringBuilder("\n") : null;
            final InputBitStream ibs = data.getInputBitStream();
            try {
                final int version = ibs.readInt(8/* nbits */);
                if (version != VERSION0) {
                    throw new IOException("Unknown version: " + version);
                }
                if (debug)
                    sb.append("version=" + version + "\n");
                
                isKeys = ibs.readBit() != 0;
            
                isSymbolTable = ibs.readBit() != 0;

                final boolean isOffsetArray = ibs.readBit() != 0;

                final boolean isByteAlignedOffsets = ibs.readBit() != 0;

                // skip unused bit flags.
                ibs.readInt(4/*nbits*/);
                
                if (debug) {
                    sb.append("isKeys=" + isKeys + "\n");
                    sb.append("isSymbolTable=" + isSymbolTable + "\n");
                    sb.append("isOffsetArray=" + isOffsetArray + "\n");
                    sb.append("isByteAlignedOffsets=" + isByteAlignedOffsets + "\n");
                }

                this.size = ibs.readInt(31/*nbits*/);
                if (debug)
                    sb.append("size=" + size + "\n");
                if (size < 0)
                    throw new IOException();

                nsymbols = ibs.readInt(9/* nbits */);
                if (debug)
                    sb.append("nsymbols=" + nsymbols + "\n");
                assert ibs.readBits() == O_symbols;

                // skip over the packed symbol table.
                if (isSymbolTable) {

                    ibs.skip(nsymbols * 8L);

                    assert ibs.readBits() == O_symbols + (nsymbols * 8L);

                }
            
                final long O_codedValueOffsetBits = ibs.readBits();
                codedValueOffsetBits = ibs.readInt(8/* nbits */);
                if (debug) {
                    sb.append("O_codedValueOffsetBits="
                            + O_codedValueOffsetBits + "\n");
                    sb.append("codedValueOffsetBits=" + codedValueOffsetBits
                            + "\n");
                }

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
                O_nulls = ibs.readBits();
                assert O_nulls == O_codedValueOffsetBits + 8
                        + (codedValueOffsetBits == 0 ? 0 : 32);
                if (!isKeys) {
                    if (debug) {
                        sb.append("O_nulls=" + O_nulls + "\n");
                        for (int i = 0; i < size; i++) {
                            final boolean isNull = ibs.readBit() != 0;
                            sb.append("null[" + i + "]=" + isNull + "\n");
                            assert isNull == data.getBit(O_nulls + i) : "index="
                                    + i;
                        }
                    } else {
                        // skip over the null bit flags.
                        ibs.skip((long) size);
                    }
                }

                if (nsymbols == 0) {

                    /*
                     * The rest of the record is empty.
                     */

                    this.decoder = null;
                    this.O_codedValues = -1;
                    this.O_codedValueOffsets = -1;

                    return;
                    
                }
                
                /*
                 * Setup the decoder.
                 */
                final long O_decoderInputs = ibs.readBits();
                if (debug)
                    sb.append("O_decoderInputs=" + O_decoderInputs + "\n");
                assert O_decoderInputs == O_nulls + (isKeys ? 0 : size);
                
                // read bit length[], symbol[], and shortest code word. 
                final DecoderInputs decoderInputs = readDecoderInputs(nsymbols,
                        ibs, sb);

                if (decoder == null) {
                    /*
                     * Reconstruct the decoder.
                     */
                    this.decoder = new CanonicalFast64CodeWordDecoder(
                            decoderInputs.getLengths(), decoderInputs
                                    .getSymbols());
                } else {
                    /*
                     * There is a minor cost savings if given, but we still need
                     * to read the 'decoderInputs' to know where we are in the
                     * bit stream.
                     */
                    this.decoder = decoder;
                }

                // skip over the coded byte[][] values.
                O_codedValues = ibs.readBits();
                if (debug)
                    sb.append("O_codedValues=" + O_codedValues + "\n");

                // skip over the coded byte[][] values.
                ibs.skip(sumCodedValueBitLengths);

                // note bit offset of the codedValueOffset[].
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
                
                if (debug)
                    log.debug(sb.toString());

            } catch (IOException ex) {
                throw new RuntimeException(ex);
// close not required for IBS backed by byte[] and has high overhead.
//            } finally {
//                try {
//                    ibs.close();
//                } catch (IOException ex) {
//                    log.error(ex);
//                }
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

        final public boolean isKeys() {

            return isKeys;

        }

        public AbstractFixedByteArrayBuffer data() {

            return data;

        }

//        /**
//         * Lookup a symbol, returning the byte value for that symbol.
//         * 
//         * @param symbol
//         *            The symbol.
//         * 
//         * @return The byte value.
//         * 
//         * @deprecated This has been inlined for better performance.
//         */
//        final private byte symbol2byte(final byte[] array, final int symbol) {
//        
//            assert symbol >= 0 && symbol < nsymbols : "nsymbol=" + symbol
//                    + " not in [0:" + (nsymbols - 1) + "]";
//
//            if (!isSymbolTable) {
//
//                return (byte) KeyBuilder.encodeByte(symbol);
//                
//            } else {
//
//                /*
//                 * Index into the buffer start after [nsymbols].
//                 * 
//                 * Note: This uses direct indexing into the backing byte[] to
//                 * avoid method call and parameter check overhead associated
//                 * with data.getByte(). [I had to disable the direct addressing
//                 * because ByteArrayBuffer#trim() was replacing the backing
//                 * byte[] reference after the data were coded by encodeLive()].
//                 */
////                if (array != null)
//                return array[aoff + BYTE_O_symbols + symbol];
//                
////                return data.getByte(BYTE_O_symbols + symbol);
//            
//            }
//
//        }

        public boolean isNull(final int index) {

            if (index < 0 || index >= size)
                throw new IllegalArgumentException();

            if(isKeys) {

                /*
                 * nulls are not allowed for keys and we do not code the
                 * presence of nulls when we are coding keys.
                 */
                
                return false;
                
            }

            return data.getBit(O_nulls + index);

        }

        /**
         * This computes the length of the decoded byte[] by counting the code
         * words for the coded value.
         */
        public int length(final int index) {

            if (index < 0 || index >= size)
                throw new IllegalArgumentException();

            if (!isKeys) {

                /*
                 * Figure out whether or not this index is a null.
                 */
                final boolean isNull = data.getBit(O_nulls + index);

                if (isNull) {

                    // per the API.
                    throw new NullPointerException();

                }

            }

            if (nsymbols == 0) {

                /*
                 * The offset[] is not stored if there are no symbols. Since we
                 * know that the value is not a null, this implies that the
                 * coded value was an empty byte[]. We have to handle this case
                 * explicitly.
                 */

                return 0;

            }

            /*
             * Figure out the bit length of the coded byte[].
             * 
             * Note: When we are allocating the byte[] for the caller we will
             * scan the coded byte[] once to get the length of the decoded
             * byte[] and then scan it again to decode the byte[].
             */

            final InputBitStream ibs = data.getInputBitStream();
            try {

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
                // reset the read bits counter.
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

// close not required for IBS backed by byte[] and has high overhead.
//            } finally {
//
//                try {
//                    ibs.close();
//                } catch (IOException ex) {
//                    log.error(ex);
//                }

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

            if (!isKeys) {

                /*
                 * Figure out whether or not this index is a null.
                 */
                final boolean isNull = data.getBit(O_nulls + index);

                if (isNull) {

                    // The value is a null.
                    return null;

                }
                
            }

            if (nsymbols == 0) {

                /*
                 * The offset[] is not stored if there are no symbols. Since
                 * we know that the value is not a null, this implies that
                 * the coded value was an empty byte[]. We have to handle
                 * this case explicitly.
                 */
                
                return BytesUtil.EMPTY;
                
            }
                
            final InputBitStream ibs = data.getInputBitStream();

            try {

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
                
                assert codeLength >= 0 : "index=" + index + ", codeLength="
                        + codeLength;

                /*
                 * Figure out how many symbols are in the coded byte[].
                 */
                // position at the start of the coded byte[].
                ibs.position(O_from + O_codedValues);
                // reset the read bits counter (required).
                ibs.readBits(0L);
                // #of symbols in this coded byte[].
                int nsymbols = 0;
                while (ibs.readBits() < codeLength) {

                    decoder.decode(ibs);

                    nsymbols++;

                }

                /*
                 * Allocate an exact fit byte[] and decode into that byte[]. We
                 * decode to symbol indices and then convert symbol indices to
                 * bytes, which are stored in that exact fit byte[].
                 */

                // position at the start of the coded byte[].
                ibs.position(O_from + O_codedValues);
                
                return getFrom(ibs, nsymbols);

            } catch (IOException ex) {

                throw new RuntimeException(ex);

// close not required for IBS backed by byte[] and has high overhead.
//            } finally {
//
//                try {
//                    ibs.close();
//                } catch (IOException ex) {
//                    log.error(ex);
//                }

            }

        }

        /**
         * Decodes the specified number of symbols into an exact fit byte[]. The
         * {@link InputBitStream} must be pre-positioned to the start of the
         * coded symbols.
         * 
         * @param ibs
         *            The bit stream.
         * @param nsymbols
         *            The #of symbols to decode.
         * 
         * @return The exact fit byte[] containing the decoded symbols.
         * 
         * @throws IOException
         */
        private byte[] getFrom(final InputBitStream ibs, final int nsymbols)
                throws IOException {

            // Allocate the decoded byte[].
            final byte[] a = new byte[nsymbols];

            if (!isSymbolTable) {

                for (int i = 0; i < nsymbols; i++) {

                    final int symbol = decoder.decode(ibs);

                    a[i] = (byte) KeyBuilder.encodeByte(symbol);

                }

                return a;
                
            }

            /*
             * Note: This uses direct indexing into the backing byte[] to
             * avoid method call and parameter check overhead associated
             * with data.getByte().
             */

            // the coded data record.
            final byte[] array = data.array();

            final int aoff = this.aoff + BYTE_O_symbols;

            for (int i = 0; i < nsymbols; i++) {

                final int symbol = decoder.decode(ibs);

                a[i] = array[aoff + symbol];

                // a[i] = symbol2byte(array, symbol);

            }

            return a;

        }

        /**
         * This decodes the value at the specified index in a single pass onto
         * the caller's stream.
         */
        public int copy(final int index, final OutputStream os) {

            if (index < 0 || index >= size)
                throw new IllegalArgumentException();

            if (!isKeys) {

                /*
                 * Figure out whether or not this index is a null.
                 */
                final boolean isNull = data.getBit(O_nulls + index);

                if (isNull) {

                    // per the API.
                    throw new NullPointerException();

                }

            }

            if (nsymbols == 0) {

                /*
                 * The offset[] is not stored if there are no symbols. Since we
                 * know that the value is not a null, this implies that the
                 * coded value was an empty byte[]. We have to handle this case
                 * explicitly.
                 */

                return 0;

            }

            final InputBitStream ibs = data.getInputBitStream();

            try {

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

                assert codeLength >= 0;
                
                /*
                 * Copy the decoded symbols into the buffer.
                 */

                // position at the start of the coded byte[].
                ibs.position(O_from + O_codedValues);

                // transfer codeLength decoded bytes to caller's stream.
                return copyFrom(ibs, codeLength, os);

            } catch (IOException ex) {

                throw new RuntimeException(ex);
// close() not required for IBS backed by byte[], and has high overhead.
//            } finally {
//
//                try {
//                    ibs.close();
//                } catch (IOException ex) {
//                    log.error(ex);
//                }

            }

        }

        /**
         * Copy a sequence of decoded code words onto the caller's stream.
         * 
         * @param ibs
         *            The input stream, pre-positioned at the start of the code
         *            words to be decoded.
         * @param codeLength
         *            The bit length of the sequence of code words to be
         *            decoded.
         * @param os
         *            Where to put the data.
         * 
         * @return The #of decoded symbols that were copied.
         * 
         * @throws IOException
         */
        private int copyFrom(final InputBitStream ibs, final long codeLength,
                final OutputStream os) throws IOException {

            // The #of symbols decoded. 
            int nsymbols = 0;

            // reset the read bits counter.
            ibs.readBits(0L);

            if (!isSymbolTable) {

                while (ibs.readBits() < codeLength) {

                    final int symbol = decoder.decode(ibs);

                    final byte b = (byte) KeyBuilder.encodeByte(symbol);

                    os.write(b);

                    nsymbols++;

                }

                return nsymbols;

            }

            /*
             * Note: This uses direct indexing into the backing byte[] to avoid
             * method call and parameter check overhead associated with
             * data.getByte().
             */

            // the coded data record.
            final byte[] array = data.array();

            // compute once.
            final int aoff = this.aoff + BYTE_O_symbols;

            while (ibs.readBits() < codeLength) {

                final int symbol = decoder.decode(ibs);

                // final byte b = symbol2byte(array, symbol);
                final byte b = array[aoff + symbol];

                os.write(b);

                nsymbols++;

            }

            return nsymbols;

        }

        /**
         * Basic implementation may be overridden if a faster implementation is
         * available.
         */
        public Iterator<byte[]> iterator() {

            /**
             * This per-iterator buffer is used to copy the decoded byte[] out
             * of the coded raba. get(index) requires two passes over the coded
             * value - one of which is just to compute the #of symbols. By using
             * copy(int,os) we can extract the data in a single pass onto this
             * buffer. The data in the buffer is then cloned to obtain an exact
             * fit byte[].
             * 
             * @todo if we knew the max byte[] length for the instance then we
             *       could use that here and avoid over/under allocating.
             */
            final ByteArrayBuffer tmp = new ByteArrayBuffer(128/*initialCapacity*/);
            
            return new Iterator<byte[]>() {

                int i = 0;

                public boolean hasNext() {

                    return i < size();

                }

                public byte[] next() {

                    if (!hasNext())
                        throw new NoSuchElementException();

                    try {

                        if (!isKeys) {

                            /*
                             * Figure out whether or not this index is a null.
                             */
                            final boolean isNull = data.getBit(O_nulls + i);

                            if (isNull) {

                                // per the API.
                                return null;

                            }

                        }

                        // reset the buffer.
                        tmp.reset();

                        // copy out decoded byte[] (more efficient than get()).
                        copy(i, tmp);

                        // return an exact fit copy of the decoded byte[].
                        return tmp.toByteArray();

                    } finally {

                        i++;

                    }

//                    return get(i++);

                }

                public void remove() {

                    throw new UnsupportedOperationException();

                }

            };

        }
        
        /**
         * This is an efficient binary search performed without materializing the
         * coded byte[][].
         */
        public int search(final byte[] probe) {

            if (probe == null)
                throw new IllegalArgumentException();
            
            if(!isKeys())
                throw new UnsupportedOperationException();
            
            final InputBitStream ibs = data.getInputBitStream();

            try {

                return binarySearch(ibs, probe);
                
            } catch (IOException ex) {

                throw new RuntimeException(ex);

// close not required for IBS backed by byte[] and has high overhead.
//            } finally {
//
//                try {
//                    ibs.close();
//                } catch (IOException ex) {
//                    log.error(ex);
//                }

            }

        }

        /**
         * Binary search in the coded key space.
         * 
         * @param ibs
         *            The bit stream.
         * @param key
         *            The key for the search.
         * 
         * @return index of the search key, if it is contained in <i>keys</i>;
         *         otherwise, <code>(-(insertion point) - 1)</code>. The
         *         insertion point is defined as the point at which the key
         *         would be inserted into the array of keys. Note that this
         *         guarantees that the return value will be >= 0 if and only if
         *         the key is found.
         */
        final private int binarySearch(final InputBitStream ibs,
                final byte[] key) throws IOException {

            /*
             * The implementation codes the symbols in the key space one at a
             * time, searching the coded values as it goes. This avoids the
             * overhead of allocating a byte[] for the coded key. Instead, we
             * directly access the codeWord for the symbol in the coder impl.
             */

            // the coded data record.
            final byte[] array = data.array();
            
            final int nmem = this.size;
            
            final int base = 0;
            
            int low = 0;

            int high = nmem - 1;

            while (low <= high) {

                final int mid = (low + high) >> 1;

                final int offset = base + mid;

                // compare actual vs probe
                final int tmp = compare(ibs, offset/* index */, key, array);

                if (tmp > 0) {

                    // Actual LT probe, restrict lower bound and try again.
                    low = mid + 1;

                } else if (tmp < 0) {

                    // Actual GT probe, restrict upper bound and try again.
                    high = mid - 1;

                } else {

                    // Actual EQ probe. Found : return offset.

                    return offset;

                }

            }

            // Not found: return insertion point.

            final int offset = (base + low);

            return -(offset + 1);

        }

        /**
         * Compares the given byte[] to the coded value at the specified index.
         * The comparison is done in the code space.
         * 
         * @param index
         *            The index of the coded value.
         * @param key
         *            The search probe key.
         * @param array
         *            The reference to the coded data record.
         * 
         * @return a negative integer, zero, or a positive integer if the coded
         *         value identified by the <i>index</i> is less than, equal to,
         *         or greater than the <i>key</i> when they are compared in the
         *         code space.
         * 
         * @throws IOException
         */
        private int compare(final InputBitStream ibs, final int index,
                final byte[] key, final byte[] array) throws IOException {

            if (nsymbols == 0) {

                /*
                 * The offset[] is not stored if there are no symbols. Since we
                 * know that the value is not a null (keys do not permit nulls),
                 * this implies that the coded value was an empty byte[]. We
                 * have to handle this case explicitly.
                 */

                return BytesUtil.compareBytes(key, BytesUtil.EMPTY);

            }

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

            assert codeLength >= 0;

            // position at the start of the coded byte[].
            ibs.position(O_from + O_codedValues);

            // reset the read bits counter.
            ibs.readBits(0L);

            // #of symbols decoded.
            int nsymbols = 0;

            // pre-compute once.
            final int aoff = this.aoff + BYTE_O_symbols;
            
            // loop compares decoded symbols one-by-one.
            while (ibs.readBits() < codeLength && nsymbols < key.length) {

                assert decoder != null;
                
                final int symbol = decoder.decode(ibs);

//              final byte b = symbol2byte(array, symbol);
                final byte b;
                if (!isSymbolTable) {

                    b = (byte) KeyBuilder.encodeByte(symbol);
                    
                } else {

                    /*
                     * Note: This uses direct indexing into the backing byte[] to
                     * avoid method call and parameter check overhead associated
                     * with data.getByte().
                     */

                    b = array[aoff + symbol];
                
                }
                
                final byte a = key[nsymbols];

                // promotes to signed integers in [0:255] for comparison.
                final int ret = (a & 0xff) - (b & 0xff);

                if (ret != 0)
                    return ret;

                nsymbols++;

            }

            if (nsymbols == key.length) {

                /*
                 * Note: When the search key is [] we but the coded symbol is
                 * non-empty, we need to indicate that the coded symbol is
                 * longer.
                 */
                
                if (ibs.readBits() < codeLength) {
                
                    return -1;
                    
                }
                
            }
            
            return key.length - nsymbols;

        }

    }

}
