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
 * Created on Aug 6, 2009
 */

package com.bigdata.btree.raba.codec;

import it.unimi.dsi.bits.BitVector;
import it.unimi.dsi.compression.CanonicalFast64CodeWordDecoder;
import it.unimi.dsi.compression.Fast64CodeWordCoder;
import it.unimi.dsi.compression.HuffmanCodec;
import it.unimi.dsi.fastutil.booleans.BooleanIterator;

import java.util.Arrays;
import java.util.Random;


/**
 * Test suite for the {@link CanonicalHuffmanRabaCoder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCanonicalHuffmanRabaCoder extends AbstractRabaCoderTestCase {

    /**
     * 
     */
    public TestCanonicalHuffmanRabaCoder() {
    }

    /**
     * @param name
     */
    public TestCanonicalHuffmanRabaCoder(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        
        rabaCoder = new CanonicalHuffmanRabaCoder();
        
    }

    /**
     * Format the code book as a multi-line string.
     * 
     * @param codeWords
     *            The code words.
     * 
     * @return A representation of the code book.
     */
    static protected String printCodeBook(final BitVector[] codeWords) {

        final StringBuilder sb = new StringBuilder();

        for (BitVector v : codeWords) {

            sb.append("codeWord: " + v + ", bitLength=" + v.size()
                    + ", longValue=" + v.getLong(0, v.size()) + "\n");

        }

        return sb.toString();

    }

    /*
     * Bootstrapping unit tests for various assumptions about the
     * {@link HuffmanCodec} implementation class.
     */
    
    /**
     * Test with a simple fixed frequency[].
     */
    public void test_huffmanCodec01() {

        final int[] frequency = new int[] { 1, 2, 3, 3, 4, 5 };

        doRoundTripTest(frequency);
        
    }

    /**
     * Fails.
     * 
     * <pre>
     * java.lang.ArrayIndexOutOfBoundsException: -2
     *     at it.unimi.dsi.compression.CanonicalFast64CodeWordDecoder.&lt;init&gt;(CanonicalFast64CodeWordDecoder.java:62)
     *     at it.unimi.dsi.compression.HuffmanCodec.&lt;init&gt;(HuffmanCodec.java:107)
     *     at com.bigdata.btree.raba.codec.TestCanonicalHuffmanRabaCoder.doRoundTripTest(TestCanonicalHuffmanRabaCoder.java:166)
     *     at com.bigdata.btree.raba.codec.TestCanonicalHuffmanRabaCoder.test_huffmanCodec_noSymbols(TestCanonicalHuffmanRabaCoder.java:121)
     * </pre>
     */
    public void test_huffmanCodec_noSymbols() {

        final int[] frequency = new int[] {};

        doRoundTripTest(frequency);
        
    }

    /**
     * Fails.
     * 
     * <pre>
     * java.lang.ArrayIndexOutOfBoundsException: -1
     *     at it.unimi.dsi.compression.CanonicalFast64CodeWordDecoder.&lt;init&gt;(CanonicalFast64CodeWordDecoder.java:89)
     *     at it.unimi.dsi.compression.HuffmanCodec.&lt;init&gt;(HuffmanCodec.java:107)
     *     at com.bigdata.btree.raba.codec.TestCanonicalHuffmanRabaCoder.doRoundTripTest(TestCanonicalHuffmanRabaCoder.java:166)
     *     at com.bigdata.btree.raba.codec.TestCanonicalHuffmanRabaCoder.test_huffmanCodec_oneSymbols(TestCanonicalHuffmanRabaCoder.java:132)
     * </pre>
     */
    public void test_huffmanCodec_oneSymbols() {

        final int[] frequency = new int[] {1};

        doRoundTripTest(frequency);
        
    }

    /**
     * Stress test with random frequency distributions of between 2 and 256
     * distinct symbols. Frequencies MAY be zero for some symbols. Tests with
     * zero and one symbols are done separately since both cases have errors.
     */
    public void test_huffmanCodecStress() {

        final int ntrials = 10000;

        final Random r = new Random();

        for (int trial = 0; trial < ntrials; trial++) {

            // #of distinct symbols in [2:256].
            final int[] frequency = new int[r.nextInt(255) + 2];

            for (int i = 0; i < frequency.length; i++) {

                if (r.nextFloat() < 0.001) {
                    // zero freq allowed but rare.
                    frequency[i] = 0;
                } else {
                    frequency[i] = r.nextInt(4000);
                }

            }

            doRoundTripTest(frequency);

        }

    }

    /**
     * This verifies that a code book constructed from a given set of
     * frequencies may be reconstructed from the cord word bit lengths, given in
     * a non-decreasing order, together with the symbols in a correlated array.
     * 
     * @param frequency
     */
    public void doRoundTripTest(final int[] frequency) {
        
        final HuffmanCodec codec = new HuffmanCodec(frequency);

        final int[] length = codec.getLengths();

        final int[] symbol = codec.getSymbols().clone();

        if (log.isDebugEnabled()) {
            log.debug(printCodeBook(codec.codeWords()) + "\nlength[]="
                    + Arrays.toString(length) + "\nsymbol[]="
                    + Arrays.toString(symbol));
        }

//        final Decoder expectedDecoder = codec.decoder();
        
        final CanonicalFast64CodeWordDecoder actualDecoder = new CanonicalFast64CodeWordDecoder(
                length, symbol);

        for (int i = 0; i < symbol.length; i++) {

            final BooleanIterator coded = codec.coder().encode(i/*symbol*/);
            
            assertEquals(i, actualDecoder.decode(coded));
            
        }

    }

    /**
     * Stress test with 256 distinct symbols (corresponding to byte values in
     * the application). A large percentage of all symbols have a zero frequency
     * code, which models the expected patterns of B+Tree keys.
     */
    public void test_huffmanRecoderStress() {

        final int ntrials = 10000;
        
        final int percentZero = 40;

        final Random r = new Random();

        for (int trial = 0; trial < ntrials; trial++) {

            final int[] frequency = new int[256];

            for (int i = 0; i < frequency.length; i++) {

                if (r.nextInt() < percentZero) {
                    frequency[i] = 0;
                } else {
                    frequency[i] = r.nextInt(4000);
                }

            }

            doRecoderRoundTripTest(frequency);

        }

    }

    public void test_huffmanRecoder01() {
        
        final int[] frequency = new int[]{1,0,3,5,0,0,9};

        doRecoderRoundTripTest(frequency);

    }
    
    /**
     * Verify we can regenerate the {@link Fast64CodeWordCoder} from the code
     * word[]. This is tested by coding and decoding random symbol sequences.
     * 
     * @param frequency
     *            The frequency[] should include a reasonable proportion of
     *            symbols with a zero frequency in order to replicate the
     *            expected conditions when coding non-random data such as are
     *            found in the keys of a B+Tree.
     * 
     * @todo For this we need to reconstruct the {@link Fast64CodeWordCoder}. To
     *       do that, we need to persist the codeWord[] and create a long[]
     *       having the same values as the codeWords, but expressed as 64-bit
     *       integers.
     */
    public void doRecoderRoundTripTest(final int frequency[]) {
        
        final HuffmanCodec codec = new HuffmanCodec(frequency);

        System.err.println(printCodeBook(codec.codeWords()));

        throw new UnsupportedOperationException();
//        final int[] length = codec.getLengths();
//
//        final int[] symbol = codec.getSymbols().clone();
//
//        if (false && log.isInfoEnabled()) {
//            log.info(printCodeBook(codec.codeWords()) + "\nlength[]="
//                    + Arrays.toString(length) + "\nsymbol[]="
//                    + Arrays.toString(symbol));
//        }
//
////        final Decoder expectedDecoder = codec.decoder();
//        
//        final CanonicalFast64CodeWordDecoder actualDecoder = new CanonicalFast64CodeWordDecoder(
//                length, symbol);
//
//        for (int i = 0; i < symbol.length; i++) {
//
//            final BooleanIterator coded = codec.coder().encode(i/*symbol*/);
//            
//            assertEquals(i, actualDecoder.decode(coded));
//            
//        }

    }
    
}
