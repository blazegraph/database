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
import it.unimi.dsi.compression.Coder;
import it.unimi.dsi.compression.Fast64CodeWordCoder;
import it.unimi.dsi.compression.HuffmanCodec;
import it.unimi.dsi.compression.PrefixCoder;
import it.unimi.dsi.compression.HuffmanCodec.DecoderInputs;
import it.unimi.dsi.fastutil.booleans.BooleanIterator;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.ReadOnlyKeysRaba;
import com.bigdata.btree.raba.ReadOnlyValuesRaba;
import com.bigdata.btree.raba.codec.CanonicalHuffmanRabaCoder.AbstractCodingSetup;
import com.bigdata.btree.raba.codec.CanonicalHuffmanRabaCoder.RabaCodingSetup;

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
        
        rabaCoder = CanonicalHuffmanRabaCoder.INSTANCE;
        
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

            final long long1 = v.getLong(0, v.size());
            
            final long long2 = Long.reverse(long1 << (64-v.size()));

//            System.err.println("codeWord=" + v + ", v.size=" + v.size()
//                    + " : long2=" + long2);

            sb.append("codeWord: " + v + ", bitLength=" + v.size()
                    + ", longValue=" + long2 + "\n");

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
     * Fails due to a known bug in {@link HuffmanCodec}. This is just here as
     * place holder for this bug. We work around this bug in the
     * {@link CanonicalHuffmanRabaCoder}.
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
     * Fails due to a known bug in {@link HuffmanCodec}. This is just here as
     * place holder for this bug. We work around this bug in the
     * {@link CanonicalHuffmanRabaCoder}.
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
        
        final DecoderInputs decoderInputs = new DecoderInputs();
        
        final HuffmanCodec codec = new HuffmanCodec(frequency, decoderInputs);

        if (log.isDebugEnabled()) {
            log.debug(printCodeBook(codec.codeWords()) + "\nlength[]="
                    + Arrays.toString(decoderInputs.getLengths()) + "\nsymbol[]="
                    + Arrays.toString(decoderInputs.getSymbols()));
        }
        
        final CanonicalFast64CodeWordDecoder actualDecoder = new CanonicalFast64CodeWordDecoder(
                decoderInputs.getLengths(), decoderInputs.getSymbols());

        for (int i = 0; i < frequency.length; i++) {

            final BooleanIterator coded = codec.coder().encode(i/*symbol*/);
            
            assertEquals(i, actualDecoder.decode(coded));
            
        }

    }

    /**
     * Stress test with 256 distinct symbols (corresponding to byte values in
     * the application). A large percentage of all symbols have a zero frequency
     * code, which models the expected patterns of B+Tree keys.
     * 
     * @throws IOException
     */
    public void test_huffmanRecoderStress() throws IOException {

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

    /**
     * Simple test with a known symbol frequency distribution.
     * 
     * @throws IOException
     */
    public void test_huffmanRecoder01() throws IOException {
        
        final int[] frequency = new int[]{1,0,3,5,0,0,9};

        doRecoderRoundTripTest(frequency);

    }

    /**
     * Verify we can regenerate the {@link Fast64CodeWordCoder} from the code
     * word[]. This is tested by coding and decoding random symbol sequences.
     * For this test we need to reconstruct the {@link Fast64CodeWordCoder}. To
     * do that, we need to use the codeWord[] and create a long[] having the
     * same values as the codeWords, but expressed as 64-bit integers.
     * 
     * @param frequency
     *            The frequency[] should include a reasonable proportion of
     *            symbols with a zero frequency in order to replicate the
     *            expected conditions when coding non-random data such as are
     *            found in the keys of a B+Tree.
     * 
     * @throws IOException
     */
    public void doRecoderRoundTripTest(final int frequency[]) throws IOException {
        
        final DecoderInputs decoderInputs = new DecoderInputs();
        
        final HuffmanCodec codec = new HuffmanCodec(frequency, decoderInputs);

        final PrefixCoder expected = codec.coder();
        
        final PrefixCoder actual = new Fast64CodeWordCoder(codec.codeWords());
        
        if (log.isDebugEnabled())
            log.debug(printCodeBook(codec.codeWords()));

        /*
         * First verify that both coders produce the same coded values for a
         * symbol sequence of random length drawn from the full set of symbols
         * of random length [1:nsymbols].
         */
        final int[] value = new int[r.nextInt(frequency.length) + 1];
        for(int i=0; i<value.length; i++) {
            // any of the symbols in [0:nsymbols-1].
            value[i] = r.nextInt(frequency.length);
        }

        /*
         * Now code the symbol sequence using both coders and then compare the
         * coded values. They should be the same.
         */
        final byte[] codedValue;
        {
            final FastByteArrayOutputStream ebaos = new FastByteArrayOutputStream();
            final FastByteArrayOutputStream abaos = new FastByteArrayOutputStream();
            final OutputBitStream eobs = new OutputBitStream(ebaos);
            final OutputBitStream aobs = new OutputBitStream(abaos);
            for (int i = 0; i < value.length; i++) {
                final int symbol = value[i];
                expected.encode(symbol, eobs);
                actual.encode(symbol, aobs);
            }
            eobs.flush();
            aobs.flush();
            assertEquals(0, BytesUtil.compareBytesWithLenAndOffset(0/* aoff */,
                    ebaos.length, ebaos.array, 0/* boff */, abaos.length,
                    abaos.array));
            codedValue = new byte[abaos.length];
            System.arraycopy(abaos.array/*src*/, 0/*srcPos*/, codedValue/*dest*/, 0/*destPos*/, abaos.length/*len*/);
        }

        /*
         * Now verify that the coded sequence decodes to the original symbol
         * sequence using a Decoder which is reconstructed from the bit length
         * and symbol arrays of the codec.
         */
        final CanonicalFast64CodeWordDecoder actualDecoder = new CanonicalFast64CodeWordDecoder(
                decoderInputs.getLengths(), decoderInputs.getSymbols());

        {

            final InputBitStream ibs = new InputBitStream(codedValue);
            
            for (int i = 0; i < value.length; i++) {

                assertEquals(value[i]/* symbol */, actualDecoder.decode(ibs));

            }
            
        }

    }

    /**
     * Unit test for processing an empty {@link IRaba} representing B+Tree keys.
     * <p>
     * For an empty {@link IRaba}, {@link RabaCodingSetup} actually assigns
     * <code>null</code> for the {@link DecoderInputs} due to a bug in the
     * {@link HuffmanCodec} when nsymbols == 0. Therefore, this verifies that
     * the {@link Coder} and {@link DecoderInputs} are <code>null</code> and
     * that the symbol count is zero.
     * 
     * @throws IOException
     */
    public void test_emptyKeyRabaSetup() throws IOException {
        
        final int n = 0;
        final byte[][] a = new byte[n][];
        
        final IRaba raba = new ReadOnlyKeysRaba(a);

        final AbstractCodingSetup setup = new RabaCodingSetup(raba);
        
        assertEquals(0,setup.getSymbolCount());
        assertNull(setup.codec());
        assertNull(setup.decoderInputs());

//        doDecoderInputRoundTripTest(setup.getSymbolCount(), setup
//                .decoderInputs());
//
//        // verify that we can re-create the coder.
//        doCoderRoundTripTest(setup.codec().codeWords(), setup.decoderInputs()
//                .getShortestCodeWord(), setup.decoderInputs().getLengths(),
//                setup.decoderInputs().getSymbols());

    }

    /**
     * Unit test for processing an {@link IRaba} representing B+Tree keys
     * suitable to setup the data for compression.
     * 
     * @throws IOException 
     */
    public void test_keyRabaSetup() throws IOException {

        final int n = 8;
        final byte[][] a = new byte[n][];
        a[0] = new byte[]{1,2};
        a[1] = new byte[]{1,2,3};
        a[2] = new byte[]{1,3};
        a[3] = new byte[]{1,3,1};
        a[4] = new byte[]{1,3,3};
        a[5] = new byte[]{1,3,7};
        a[6] = new byte[]{1,5};
        a[7] = new byte[]{1,6,0};
        
        final IRaba raba = new ReadOnlyKeysRaba(a);

        final AbstractCodingSetup setup = new RabaCodingSetup(raba);

        doDecoderInputRoundTripTest(setup.getSymbolCount(), setup
                .decoderInputs());

        // verify that we can re-create the coder.
        doCoderRoundTripTest(setup.codec().codeWords(), setup.decoderInputs()
                .getShortestCodeWord(), setup.decoderInputs().getLengths(),
                setup.decoderInputs().getSymbols());

    }

    /**
     * Unit test for processing an {@link IRaba} representing B+Tree values
     * suitable to setup the data for compression.
     * 
     * @throws IOException 
     * 
     * @todo test w/ nulls.
     */
    public void test_valueRabaSetup() throws IOException {

        final int n = 3;
        final byte[][] a = new byte[n][];
        a[0] = new byte[]{2,3};
        a[1] = new byte[]{3,5};
        a[2] = new byte[]{'m','i','k','e'};
        
        final IRaba raba = new ReadOnlyValuesRaba(a);

        final RabaCodingSetup setup = new RabaCodingSetup(raba);
        
        // verify that we can re-create the decoder.
        doDecoderInputRoundTripTest(setup.getSymbolCount(), setup
                .decoderInputs());

        // verify that we can re-create the coder.
        doCoderRoundTripTest(setup.codec().codeWords(), setup.decoderInputs()
                .getShortestCodeWord(), setup.decoderInputs().getLengths(),
                setup.decoderInputs().getSymbols());

    }

    /**
     * Unit test for processing an empty {@link IRaba} representing B+Tree
     * values.
     * <p>
     * For an empty {@link IRaba}, {@link RabaCodingSetup} actually assigns
     * <code>null</code> for the {@link DecoderInputs} due to a bug in the
     * {@link HuffmanCodec} when nsymbols == 0. Therefore, this verifies that
     * the {@link Coder} and {@link DecoderInputs} are <code>null</code> and
     * that the symbol count is zero.
     * 
     * @throws IOException
     */
    public void test_emptyValueRabaSetup() throws IOException {

        final int n = 0;
        final byte[][] a = new byte[n][];

        final IRaba raba = new ReadOnlyValuesRaba(a);

        final RabaCodingSetup setup = new RabaCodingSetup(raba);

        assertEquals(0,setup.getSymbolCount());
        assertNull(setup.codec());
        assertNull(setup.decoderInputs());
        
//        // verify that we can re-create the decoder.
//        doDecoderInputRoundTripTest(setup.getSymbolCount(), setup
//                .decoderInputs());
//
//        // verify that we can re-create the coder.
//        doCoderRoundTripTest(setup.codec().codeWords(), setup.decoderInputs()
//                .getShortestCodeWord(), setup.decoderInputs().getLengths(),
//                setup.decoderInputs().getSymbols());

    }

    /**
     * Verify that we can round-trip the data required to reconstruct the
     * decoder.
     * 
     * @param decoderInputs
     * 
     * @throws IOException
     */
    private void doDecoderInputRoundTripTest(final int nsymbols,
            final DecoderInputs decoderInputs) throws IOException {

        final byte[] in;
        {
            final FastByteArrayOutputStream baos = new FastByteArrayOutputStream();
            final OutputBitStream obs = new OutputBitStream(baos);

            final StringBuilder sb = CanonicalHuffmanRabaCoder.log.isDebugEnabled()?new StringBuilder():null;
            
            CanonicalHuffmanRabaCoder.writeDecoderInputs(decoderInputs, obs, sb);

            if (sb != null) {
            
                CanonicalHuffmanRabaCoder.log.debug(sb.toString());
                
            }

            obs.flush();
            obs.close();

            // just the bytes written.
            in = new byte[baos.length];
            System.arraycopy(baos.array, 0, in, 0, baos.length);

        }

        {

            final InputBitStream ibs = new InputBitStream(in);

            final StringBuilder sb = CanonicalHuffmanRabaCoder.log
                    .isDebugEnabled() ? new StringBuilder() : null;

            final DecoderInputs actualInputs = CanonicalHuffmanRabaCoder
                    .readDecoderInputs(nsymbols, ibs, sb);

            if (sb != null) {

                CanonicalHuffmanRabaCoder.log.debug(sb.toString());

            }

            assertEquals("shortestCodeWord", decoderInputs
                    .getShortestCodeWord(), actualInputs.getShortestCodeWord());

            assertEquals("length[]", decoderInputs.getLengths(), actualInputs
                    .getLengths());

            assertEquals("symbol[]", decoderInputs.getSymbols(), actualInputs
                    .getSymbols());

        }

    }

    /**
     * @param shortestCodeWord
     * @param lengths
     * @param
     */
    private void doCoderRoundTripTest(final BitVector[] expected,
            final BitVector shortestCodeWord, final int[] length,
            final int[] symbol) {

        final PrefixCoder newCoder = HuffmanCodec.newCoder(shortestCodeWord,
                length, symbol);

        final BitVector[] actual = newCoder.codeWords();

        assertEquals("codeWord[]", expected, actual);

        if (log.isDebugEnabled()) {
         
            log.debug("\nexpected: " + Arrays.toString(expected)
                    + "\nactual  : " + Arrays.toString(actual));
            
        }
        
    }

}
