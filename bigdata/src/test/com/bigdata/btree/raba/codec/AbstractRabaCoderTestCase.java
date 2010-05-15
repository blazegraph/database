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

import it.unimi.dsi.compression.CanonicalFast64CodeWordDecoder;
import it.unimi.dsi.compression.HuffmanCodec;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import junit.framework.TestCase2;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.ReadOnlyKeysRaba;
import com.bigdata.btree.raba.ReadOnlyValuesRaba;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.io.FixedByteArrayBuffer;
import com.bigdata.io.SerializerUtil;

/**
 * Abstract test suite for {@link IRabaCoder} implementations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRabaCoderTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractRabaCoderTestCase() {
    }

    /**
     * @param name
     */
    public AbstractRabaCoderTestCase(String name) {
        super(name);
    }

    /**
     * Return <code>true</code> if the {@link IRabaCoder} only handles fixed
     * length values (the default always returns <code>false</code>). This is
     * used to skip tests which have variable length byte[]s when testing the
     * {@link FixedLengthValueRabaCoder}.
     */
    protected boolean isFixedLength() {
        
        return false;
        
    }

    /**
     * The fixture under test. This will be <code>null</code> unless you
     * explicitly set it in {@link #setUp()}.
     */
    protected IRabaCoder rabaCoder = null;

    /**
     * A simple unit test.
     */
    public void test_mike_personick() throws UnsupportedEncodingException {

        if(isFixedLength()) return;
        
        final byte[][] a = new byte[2][];
        a[0] = "mike".getBytes("US-ASCII");
        a[1] = "personick".getBytes("US-ASCII");

        if (rabaCoder.isValueCoder()) {

            doRoundTripTest(rabaCoder, new ReadOnlyValuesRaba(a));

        }

        if (rabaCoder.isKeyCoder()) {

            final IRaba expected = new ReadOnlyKeysRaba(a);

            doRoundTripTest(rabaCoder, expected);

            {
                /*
                 * Spot check the correct computation of the insertion point for
                 * a variety of search keys.
                 */

                final AbstractFixedByteArrayBuffer data = rabaCoder.encode(
                        expected, new DataOutputBuffer());

                final ICodedRaba actual = rabaCoder.decode(data);

                // verify correct insertion point for an empty byte[].
                assertEquals(-1, actual.search(new byte[] {}));

                assertEquals(-1, actual.search(new byte[] { 'm', 'i', 'k' }));

                assertEquals(-2, actual.search(new byte[] { 'm', 'i', 'k', 'e',
                        's' }));

                assertEquals(-2, actual.search("personic".getBytes("US-ASCII")));

                assertEquals(-3, actual.search("personicks"
                        .getBytes("US-ASCII")));
            
            }
            
        }

    }

    /**
     * Test with byte values which are negative values when interpreted as as
     * signed 8 bit integers.
     */
    public void test_negativeByteValues() throws UnsupportedEncodingException {

        if(isFixedLength()) return;

        final byte[][] a = new byte[1][];

        a[0] = new byte[] { 64, -64 };

        if (rabaCoder.isKeyCoder()) {

            doRoundTripTest(rabaCoder, new ReadOnlyKeysRaba(a));

        }

        if (rabaCoder.isValueCoder()) {

            doRoundTripTest(rabaCoder, new ReadOnlyValuesRaba(a));

        }

    }

    /**
     * Test with an empty byte[] element.
     * 
     * @throws UnsupportedEncodingException
     */
    public void test_emptyElement() throws UnsupportedEncodingException {

        if(isFixedLength()) return;

        final byte[][] a = new byte[3][];
        a[0] = new byte[0];
        a[1] = "mike".getBytes("US-ASCII");
        a[2] = "personick".getBytes("US-ASCII");
        
        if (rabaCoder.isKeyCoder()) {

            doRoundTripTest(rabaCoder, new ReadOnlyKeysRaba(a));

        }

        if (rabaCoder.isValueCoder()) {

            doRoundTripTest(rabaCoder, new ReadOnlyValuesRaba(a));

        }

    }

    /**
     * Test with a single byte value (nsymbols:=1). This test was written to a
     * known bug in {@link HuffmanCodec} and
     * {@link CanonicalFast64CodeWordDecoder}. A workaround for that bug has
     * been implemented in the {@link CanonicalHuffmanRabaCoder}.
     */
    public void test_nsymbolsOne() {

        if(isFixedLength()) return;

        final byte[][] a = new byte[1][];
        a[0] = new byte[]{1};
        
        if (rabaCoder.isKeyCoder()) {

            doRoundTripTest(rabaCoder, new ReadOnlyKeysRaba(a));

        }

        if (rabaCoder.isValueCoder()) {

            doRoundTripTest(rabaCoder, new ReadOnlyValuesRaba(a));

        }

    }

    /**
     * Test with a single byte value (nsymbols=1) and some nulls. This test was
     * written to a known bug in {@link HuffmanCodec} and
     * {@link CanonicalFast64CodeWordDecoder}. A workaround for that bug has
     * been implemented in the {@link CanonicalHuffmanRabaCoder}.
     */
    public void test_nsymbolsOne_nulls() {

        if(isFixedLength()) return;

        final byte[][] a = new byte[3][];
        a[0] = new byte[]{1};
        a[1] = null;
        a[2] = null;
        
        if (rabaCoder.isValueCoder()) {

            doRoundTripTest(rabaCoder, new ReadOnlyValuesRaba(a));

        }

    }

    /**
     * Test with a null value.
     * 
     * @throws UnsupportedEncodingException
     */
    public void test_withNulls() throws UnsupportedEncodingException {

        if (!rabaCoder.isValueCoder()) {

            // coded does not allow nulls.
            return;
            
        }
        
        final byte[][] a = new byte[3][];
        a[0] = getRandomValue(rabaCoder);
        a[1] = getRandomValue(rabaCoder);
        a[2] = null;
        
        final IRaba expected = new ReadOnlyValuesRaba(a);

        doRoundTripTest(rabaCoder, expected);

    }

    /**
     * Test with a single value, which is null.
     * 
     * FIXME Due to a bug in the {@link CanonicalFast64CodeWordDecoder} ctor
     * there is a problem handling a logical byte[][] consisting solely of
     * <code>null</code>s. We handle this for the case of an empty logical
     * byte[][] using an {@link EmptyRabaValueDecoder}. I suppose that could be
     * parameterized to identify the <code>null</code>s or else just fix the
     * ctor.
     */
    public void test_withNulls2() {
        
        if (!rabaCoder.isValueCoder()) {

            // coded does not allow nulls.
            return;
            
        }
        
        final byte[][] a = new byte[1][];

        a[0] = null;
        
        final IRaba expected = new ReadOnlyValuesRaba(a);

        doRoundTripTest(rabaCoder, expected);

    }

    final Random r = new Random();

    public void test_empty() throws IOException {

        doRandomRoundTripTest(rabaCoder, 0/* size */, 0/*capacity*/);

        doRandomRoundTripTest(rabaCoder, 0/* size */, 1/*capacity*/);
        
        doRandomRoundTripTest(rabaCoder, 0/* size */, 2/*capacity*/);
        
        doRandomRoundTripTest(rabaCoder, 0/* size */, 10/*capacity*/);
        
    }

    /**
     * Test with {@link IRaba} having a size of ONE (1) and a variety of
     * capacities.
     */
    public void test_entryCount1() throws IOException {

        doRandomRoundTripTest(rabaCoder, 1/* n */, 1/* capacity */);

        doRandomRoundTripTest(rabaCoder, 1/* n */, 2/* capacity */);

        doRandomRoundTripTest(rabaCoder, 1/* n */, 10/* capacity */);

    }

    /**
     * Test with {@link IRaba} having a size of TWO (2) and a variety of
     * capacities.
     */
    public void test_entryCount2() throws IOException {

        doRandomRoundTripTest(rabaCoder, 2/* n */, 2/* capacity */);

        doRandomRoundTripTest(rabaCoder, 2/* n */, 3/* capacity */);

        doRandomRoundTripTest(rabaCoder, 2/* n */, 10/* capacity */);

    }
    
    /**
     * This test case was developed for the {@link FrontCodedRabaCoder}.
     */
    public void test_error1() throws IOException {

        if(isFixedLength()) return;
        
        final byte b187 = KeyBuilder.encodeByte(187);
        final byte b146 = KeyBuilder.encodeByte(146);
        final byte b207 = KeyBuilder.encodeByte(207);
        
        final byte[][] a = new byte[][] {//
        new byte[]{121, b187, b146, b207, 99, 112, 24, 116},//
        new byte[]{121, b187, b146, b207, 99, 112, 43, 68},//
        new byte[]{121, b187, b146, b207, 99, 112, 46, 78},//
        new byte[]{121, b187, b146, b207, 99, 112, 54, KeyBuilder.encodeByte(176)},//
        new byte[]{121, b187, b146, b207, 99, 112, 54, KeyBuilder.encodeByte(236)},//
        new byte[]{121, b187, b146, b207, 99, 112, 55, KeyBuilder.encodeByte(209)},//
        new byte[]{121, b187, b146, b207, 99, 112, 62, 85},//
        new byte[]{121, b187, b146, b207, 99, 112, 63, KeyBuilder.encodeByte(238)},//
        new byte[]{121, b187, b146, b207, 99, 112, 71, 124},//
        new byte[]{121, b187, b146, b207, 99, 112, 73, 49}//
        };
        
        if(rabaCoder.isKeyCoder()) {
            final IRaba expected = new ReadOnlyKeysRaba(a);
//            final IRaba actual = rabaCoder.encode(expected);
//            System.err.println(actual.toString());
//            // this is the one whose rlen/clen appear to be incorrect.
//            assertEquals(expected.get(4),actual.get(4));
//            assertEquals(0, actual.search(expected.get(0)));
//            assertEquals(1, actual.search(expected.get(1)));
//            assertEquals(2, actual.search(expected.get(2)));
//            assertEquals(3, actual.search(expected.get(3)));
//            assertEquals(8, actual.search(expected.get(8)));
//            assertEquals(4, actual.search(expected.get(4)));
//            assertEquals(5, actual.search(expected.get(5)));//broken
            doRoundTripTest(rabaCoder, expected);
        }

        if(rabaCoder.isValueCoder()) {
            doRoundTripTest(rabaCoder, new ReadOnlyValuesRaba(a));
        }

    }

    /**
     * This test case was developed for the {@link CanonicalHuffmanRabaCoder}.
     * 
     * <pre>
     * junit.framework.AssertionFailedError: search([44, 186, 169, 175, 191, 31, 36, 227]): expectedIndex=1, actualIndex=-1,
     * expected=com.bigdata.btree.raba.ReadOnlyKeysRaba{ capacity=3, size=2, isKeys=true, isReadOnly=true, [
     * [44, 186, 169, 175, 191, 31, 36, 12],
     * [44, 186, 169, 175, 191, 31, 36, 227]]},
     * actual=com.bigdata.btree.raba.codec.CanonicalHuffmanRabaCoder$RabaDecoder{ capacity=2, size=2, isKeys=true, isReadOnly=true, [
     * [44, 186, 169, 175, 191, 31, 36, 12],
     * [44, 186, 169, 175, 191, 31, 36, 227]]}
     *     at junit.framework.Assert.fail(Assert.java:47)
     *     at com.bigdata.btree.AbstractBTreeTestCase.assertSameRaba(AbstractBTreeTestCase.java:583)
     *     at com.bigdata.btree.raba.codec.AbstractRabaCoderTestCase.doRoundTripTest(AbstractRabaCoderTestCase.java:499)
     *     at com.bigdata.btree.raba.codec.AbstractRabaCoderTestCase.doRandomRoundTripTest(AbstractRabaCoderTestCase.java:487)
     *     at com.bigdata.btree.raba.codec.AbstractRabaCoderTestCase.test_entryCount2(AbstractRabaCoderTestCase.java:247)
     * </pre>
     * 
     * <pre>
     * junit.framework.AssertionFailedError: search([44, 197, 214, 208, 192, 31, 36, 156]): expectedIndex=1, actualIndex=-1,
     * expected=com.bigdata.btree.raba.ReadOnlyKeysRaba{ capacity=2, size=2, isKeys=true, isReadOnly=true, [
     * [44, 197, 214, 208, 192, 31, 36, 12],
     * [44, 197, 214, 208, 192, 31, 36, 156]]},
     * actual=com.bigdata.btree.raba.codec.CanonicalHuffmanRabaCoder$RabaDecoder{ capacity=2, size=2, isKeys=true, isReadOnly=true, [
     * [44, 197, 214, 208, 192, 31, 36, 12],
     * [44, 197, 214, 208, 192, 31, 36, 156]]}
     *     at junit.framework.Assert.fail(Assert.java:47)
     *     at com.bigdata.btree.AbstractBTreeTestCase.assertSameRaba(AbstractBTreeTestCase.java:583)
     *     at com.bigdata.btree.raba.codec.AbstractRabaCoderTestCase.doRoundTripTest(AbstractRabaCoderTestCase.java:538)
     *     at com.bigdata.btree.raba.codec.AbstractRabaCoderTestCase.test_error2(AbstractRabaCoderTestCase.java:325)
     * </pre>
     */
    public void test_error2() {

        if(isFixedLength()) return;

        final int n = 2;
        final byte[][] a = new byte[n][];
        a[0] = new byte[]{44, 127-186, 127-169, 127-175, 127-191, 31, 36, 12};
        a[1] = new byte[]{44, 127-186, 127-169, 127-175, 127-191, 31, 36, 127-227};
//        a[0] = new byte[]{44, 186-127, 169-127, 175-127, 191-127, 31, 36, 12};
//        a[1] = new byte[]{44, 186-127, 169-127, 175-127, 191-127, 31, 36, 227-127};

        if(rabaCoder.isKeyCoder()) {
            final IRaba expected = new ReadOnlyKeysRaba(a);
            doRoundTripTest(rabaCoder, expected);
        }

        if(rabaCoder.isValueCoder()) {
            doRoundTripTest(rabaCoder, new ReadOnlyValuesRaba(a));
        }
    }
    
    public void test_randomOnce() throws IOException {
        
        // #of elements.
        final int n = r.nextInt(100);

        // capacity of the array.
        final int capacity = n + r.nextInt(n + 1);

        doRandomRoundTripTest(rabaCoder, n, capacity);
        
    }

    public void testStress() throws IOException {
        
        for (int i = 0; i < 1000; i++) {

            // #of elements.
            final int n = r.nextInt(100);

            // capacity of the array.
            final int capacity = n + r.nextInt(n + 1);

            doRandomRoundTripTest(rabaCoder, n, capacity);

        }

    }

    /**
     * Test using a sequence of random URIs (formed with successive prefixes).
     * 
     * @throws Exception
     */
    public void test_randomURIs() throws Exception {

        if(isFixedLength()) return;
        
        // random, distinct, unordered w/o nulls.
        final byte[][] data = new RandomURIGenerator(r).generateValues(100);

        if (rabaCoder.isValueCoder()) {

            // layer on interface.
            final IRaba raba = new ReadOnlyValuesRaba(0/* fromIndex */,
                    data.length/* toIndex */, data.length/* capacity */, data);

            doRoundTripTest(rabaCoder, raba);

        }

        if (rabaCoder.isKeyCoder()) {

            // put into sorted order.
            Arrays.sort(data, 0, data.length, UnsignedByteArrayComparator.INSTANCE);

            // layer on interface.
            final IRaba raba = new ReadOnlyKeysRaba(0/* fromIndex */,
                    data.length/* toIndex */, data.length/* capacity */, data);

            doRoundTripTest(rabaCoder, raba);

        }
        
    }

    /**
     * Return a random byte array. The byte array will also have a random length
     * in [0:512] unless the {@link IRabaCoder} is a
     * {@link FixedLengthValueRabaCoder}, in which case a byte[] having the
     * appropriate length will be returned.
     * 
     * @param dataCoder
     *            The coder.
     *            
     * @return The random byte[].
     */
    protected byte[] getRandomValue(final IRabaCoder dataCoder) {

        final int len;
        if (dataCoder instanceof FixedLengthValueRabaCoder) {
            
            len = ((FixedLengthValueRabaCoder) dataCoder).getLength();

        } else {
        
            len = r.nextInt(512);
            
        }

        final byte[] a = new byte[len];

        r.nextBytes(a);

        return a;

    }
    
    /**
     * Generates a random byte[][] and verifies round-trip encoding and
     * decoding.
     * <p>
     * Note: This uses a uniform random distribution. Therefore the huffman
     * codes tend to occupy MORE space than the original byte[][] since the
     * canonical huffman code can not be shorter than the original byte values
     * on average when all values are equally likely. When you want to measure
     * the expected compression ratio you need to test with a gaussian
     * distribution (bell curve) over the values, a USASCII distribution, etc.
     * 
     * @param size
     *            The #of entries in the byte[][].
     * @param capacity
     *            The capacity of the byte[][].
     * @throws IOException
     */
    protected void doRandomRoundTripTest(final IRabaCoder dataCoder,
            final int size, final int capacity) throws IOException {

        assert capacity >= size;

        if (dataCoder.isValueCoder()) {

            /*
             * Note: random values are not ordered and may contain nulls.
             */
 
            final byte[][] data = new byte[capacity][];

            for (int i = 0; i < size; i++) {

                final boolean isNull = r.nextFloat() < .03;

                if(isNull) {
                    
                    data[i] = null;

                } else {

                    data[i] = getRandomValue(dataCoder);

                }

            }

            // layer on interface.
            final IRaba raba = new ReadOnlyValuesRaba(0/* fromIndex */,
                    size/* toIndex */, capacity, data);

            doRoundTripTest(dataCoder, raba);
            
        }

        if (dataCoder.isKeyCoder()) {

            /*
             * Note: B+Tree keys based on random values. The keys must be
             * ordered, may not contain duplicates, and may not contain nulls.
             */
 
            final byte[][] data = new byte[capacity][];

            /*
             * The nominal maximum possible increment between successive keys.
             * The actual increment will be a random number in [1:nominal].
             */
            final int nominalIncRange = 5000;
            
            // any integer value.
            long lastKey = r.nextLong();

            // The #of keys that we actually generated.
            int nactual = 0;

            for (int i = 0; i < size; i++, nactual++) {

                data[i] = KeyBuilder.asSortKey(lastKey);

                final long remainder = Long.MAX_VALUE - lastKey;

                if (remainder == 1) {

                    // out of room in the long value space.
                    break;

                }

                final int incRange = (int) Math.max(nominalIncRange, Math
                        .min(nominalIncRange, remainder));
                
                // increment is always at least by one to avoid duplicate keys.
                final int inc = r.nextInt(incRange) + 1;

                lastKey += inc;

            }

            // layer on interface.
            final IRaba raba = new ReadOnlyKeysRaba(0/* fromIndex */,
                    nactual/* toIndex */, capacity, data);

            doRoundTripTest(dataCoder, raba);

        }

    }

    static public void doRoundTripTest(final IRabaCoder rabaCoder,
            final IRaba expected) {

        try {

            /*
             * Verify that we can (de-)serialize the coder itself.
             */
            {
                final byte[] a = SerializerUtil.serialize(rabaCoder);
                
                final IRabaCoder b = (IRabaCoder)SerializerUtil.deserialize(a);
                
            }
            
            // Test the live coded path (returns coded raba instance for immediate use).
            final ICodedRaba liveCodedRaba = rabaCoder.encodeLive(expected,
                    new DataOutputBuffer());

            final AbstractFixedByteArrayBuffer liveCodedData = liveCodedRaba.data();
            
            AbstractBTreeTestCase.assertSameRaba(expected, liveCodedRaba);
            
            final AbstractFixedByteArrayBuffer originalData = rabaCoder.encode(
                    expected, new DataOutputBuffer());
            
            {

                /*
                 * Verify that we can read the byte[] out of [data]. This is
                 * really a test of the data.getDataInput() and the returned
                 * DataInputBuffer.
                 */

                assertEquals(0, originalData.off());
                
                final byte[] tmp = new byte[originalData.len()];

                originalData.getDataInput().readFully(tmp);

                // compare against result from encode()
                assertTrue(BytesUtil.compareBytesWithLenAndOffset(originalData.off(),
                        originalData.len(), originalData.array(), 0, tmp.length, tmp) == 0);

                // compare against result from encodeLive.
                assertTrue(BytesUtil.compareBytesWithLenAndOffset(originalData
                        .off(), originalData.len(), originalData.array(),
                        liveCodedData.off(), liveCodedData.len(), liveCodedData
                                .array()) == 0);

            }

            // verify we can decode the encoded data.
            {
             
                // decode.
                final ICodedRaba actual0 = rabaCoder.decode(originalData);

                // Verify encode() results in object which can decode the
                // byte[]s.
                AbstractBTreeTestCase.assertSameRaba(expected, actual0);

                // Verify decode when we build the decoder from the serialized
                // format.
                AbstractBTreeTestCase.assertSameRaba(expected, rabaCoder
                        .decode(actual0.data()));
            }

            // Verify encode with a non-zero offset for the DataOutputBuffer
            // returns a slice which has the same data.
            {

                // buffer w/ non-zero offset.
                final int off = 10;
                final DataOutputBuffer out = new DataOutputBuffer(off,
                        new byte[100 + off]);

                // encode onto that buffer.
                final AbstractFixedByteArrayBuffer slice = rabaCoder.encode(
                        expected, out);

                // verify same encoded data for the slice.
                assertEquals(originalData.toByteArray(), slice.toByteArray());
                
            }

            // Verify decode when we build the decoder from a slice with a
            // non-zero offset
            {

                final int off = 10;
                final byte[] tmp = new byte[off + originalData.len()];
                System.arraycopy(originalData.array(), originalData.off(), tmp,
                        off, originalData.len());

                // create slice
                final FixedByteArrayBuffer slice = new FixedByteArrayBuffer(
                        tmp, off, originalData.len());

                // verify same slice.
                assertEquals(originalData.toByteArray(), slice.toByteArray());

                // decode the slice.
                final IRaba actual = rabaCoder.decode(slice);
                
                // verify same raba.
                AbstractBTreeTestCase.assertSameRaba(expected, actual);
                
            }
            
        } catch (Throwable t) {

            fail("Cause=" + t + ", expectedRaba=" + expected, t);

        }

    }

    /**
     * Performance stress test for keys. Performance tuning should give more
     * weight to coded raba access times, including search and key retrieval or
     * copy, than coding times since most use will be access on the coded data.
     * Those costs are not factored apart in the stress test times. They are
     * parameterized here by a normalized vector of the rates of the different
     * operations (search(), get(), length(), etc).
     * 
     * <dl>
     * <dt>nops</dt>
     * <dd>
     * The #of random operations to be performed. Large values for <i>nops</i>
     * need to be used to get beyond the initial JVM performance tuning so you
     * can more accurately compare the performance of the different coders. For
     * example, a value of 1M (1000000) will run for ~ 30-40s for the
     * front-coded coders. For shorter run times, the order in which we test the
     * coders will dominate their performance.</dd>
     * <dt>size</dt>
     * <dd>The #of entries in the raba to be tested (must be LTE the capacity)</dd>
     * </dl>
     * 
     * @param args
     *            [nops [generator [size]]]
     * 
     *            FIXME parameterize the generator choice.
     */
    static public void main(final String[] args) {

        final Random r = new Random();

        // default nops.
        int nops = 200000;
//        int nops = 1000000; // ~30-40s per coder @ 1M.
        if (args.length > 0)
            nops = Integer.valueOf(args[0]);
        if (nops <= 0)
            throw new IllegalArgumentException();
        
//        // default capacity (branching factor).
//        int capacity = 256;
//        if (args.length > 1)
//            capacity = Integer.valueOf(args[1]);
//        if (capacity <= 0)
//            throw new IllegalArgumentException();

        // default size (#of keys).
        int size = 256;
        if (args.length > 2)
            nops = Integer.valueOf(args[2]);
        if (size <= 0)
            throw new IllegalArgumentException();
        
        // The coders to be tested.
        final IRabaCoder[] coders = new IRabaCoder[] {
                new MutableRabaCoder(), // provides performance baseline.
                SimpleRabaCoder.INSTANCE, // simplest coding.
////                new FrontCodedRabaCoder(2/* ratio */),
                new FrontCodedRabaCoder(8/* ratio */), // front-coding.
////                new FrontCodedRabaCoder(32/* ratio */),
                CanonicalHuffmanRabaCoder.INSTANCE // huffman coding.
                };

        System.out.println("nops=" + nops + ", size=" + size + ", ncoders="
                + coders.length);

        /*
         * Generate a raba.  The same data is used for each coder. 
         */

        // The raw data.
        final byte[][] a;

        // Random keys based on random variable length byte[]s.
//        a = new RandomKeysGenerator(r, size + r.nextInt(size)/* maxKeys */, 20/* maxKeyLength */)
//                .generateKeys(size);

        // Random URIs in sorted order.
//        a = new RandomURIGenerator(r).generateKeys(size);

        // based on a tokenized source code file.
        a = new TokenizeKeysGenerator(
                "bigdata/src/test/com/bigdata/btree/raba/codec/AbstractRabaCoderTestCase.java")
                .generateKeys(size);
        
        /*
         * isNull, length, get, copy, search, iterator, recode.
         * 
         * Note: isNull is not used for keys!
         */
        final Op op = new Op(0.0f, .01f, .4f, .2f, .6f, .2f, .04f);

        /*
         * Test each IRabaCoder.
         * 
         * @todo should also test on coded B+Tree values, which would be a
         * different [expected] instance.
         */
        for(IRabaCoder rabaCoder : coders) {

            // the read-only raba.
            final ReadOnlyKeysRaba expected = new ReadOnlyKeysRaba(size, a);

            final long begin = System.nanoTime();

            int recordLength = -1;
            try {

                recordLength = doRabaCoderPerformanceTest(expected, rabaCoder,
                        size, nops, r, op);
                
            } catch (Throwable t) {

                System.err.println("coder failed: " + rabaCoder);
                
                t.printStackTrace(System.err);
                
            }

            final long elapsed = System.nanoTime() - begin;

            System.out.println(rabaCoder.toString() + " : elapsed="
                    + TimeUnit.NANOSECONDS.toMillis(elapsed)
                    + ", recordLength="
                    + (recordLength == -1 ? "N/A" : recordLength));

        }
        
    }
    
    /**
     * A test designed to measure the performance of an {@link IRabaCoder} for
     * operations on B+Tree keys, including search.
     */
    public void test_keyCoderPerformance() {
        
        // test is only for coders which can code keys.
        if(!rabaCoder.isKeyCoder()) return;

        /*
         * Some branching factors to choose from.
         */
        final int[] branchingFactors = new int[] { 3, 4, 8, 16, 27, 32, 48,
                64, 96, 99, 112, 128, 256, 512, 1024, 4096 };

        final int capacity = branchingFactors[r
                .nextInt(branchingFactors.length)];

        final int size = r.nextInt(capacity) + 1;

        // Generate a read-only raba.
        final ReadOnlyKeysRaba expected = new ReadOnlyKeysRaba(size,
                AbstractBTreeTestCase.getRandomKeys(capacity, size));

        final int nops = 50000;
        
        /*
         * isNull, length, get, copy, search, iterator, recode.
         * 
         * Note: isNull is not used for keys!
         */
        final Op op = new Op(0.0f, .01f, .4f, .2f, .6f, .2f, .04f);

        doRabaCoderPerformanceTest(expected, rabaCoder, size, nops, r, op);
        
    }

    /**
     * Do a performance stress test consisting of random operations on a
     * randomly generated B+Tree keys {@link IRaba}. The operations will be
     * checked against ground truth.
     * 
     * @param rabaCoder
     *            The coder to be tested.
     * @param size
     *            The #of keys.
     * @param nops
     *            The #of operations to perform.
     * @param r
     *            The random number generator.
     * @param op
     *            The distribution of the operations to be performed.
     * 
     * @return The size of the coded record.
     */
    static public int doRabaCoderPerformanceTest(final IRaba expected,
            final IRabaCoder rabaCoder, final int size, final int nops,
            final Random r, final Op op) {

        // The raba under test.  This can be recoded by one of the ops.
        ICodedRaba actual;
        final byte[] originalData;
        {

            // encode the record.
            final AbstractFixedByteArrayBuffer data = rabaCoder.encode(
                    expected, new DataOutputBuffer());

            // save off a copy of the original coded record.
            originalData = data.toByteArray();
            
            // decode the record.
            actual = rabaCoder.decode(data);

            // verify correct initial coding.
            AbstractBTreeTestCase.assertSameRaba(expected, actual);

        }

        // reused buffer.
        final DataOutputBuffer os = new DataOutputBuffer();

        // #of operations per operation type.
        final long[] count = new long[op._dist.length];

        // elapsed ns per operation type.
        final long[] elapsed = new long[op._dist.length];
        
        for (int i = 0; i < nops; i++) {

            final long begin = System.nanoTime();
            final int code;
            switch (code = op.nextOp(r)) {
            case Op.ISNULL: {
                if (expected.isKeys()) {
                    // method not defined for keys.
                    continue;
                }
                final int index = r.nextInt(size);
                if (log.isDebugEnabled())
                    log.debug(op.getName(code) + "(" + index + ") : expected="
                            + expected.isNull(index));
                assertEquals(op.getName(code), expected.isNull(index), actual
                        .isNull(index));
                break;
            }
            case Op.LENGTH: {
                final int index = r.nextInt(size);
                if (log.isDebugEnabled())
                    log.debug(op.getName(code) + "(" + index + ") : expected="
                            + expected.length(index));
                assertEquals(op.getName(code), expected.length(index), actual
                        .length(index));
                break;
            }
            case Op.GET: {
                final int index = r.nextInt(size);
                if (log.isDebugEnabled())
                    log.debug(op.getName(code) + "(" + index + ") : expected="
                            + BytesUtil.toString(expected.get(index)));
                assertEquals(op.getName(code), expected.get(index), actual
                        .get(index));
                break;
            }
            case Op.COPY: {
                /*
                 * Note: We reuse the same output buffer all the time for this.
                 * This is not a problem since we are not overwriting the data
                 * backing the raba.
                 */
                final int index = r.nextInt(size);
                final int len = expected.length(index);
                if (log.isDebugEnabled())
                    log.debug(op.getName(code) + "(" + index + ") : expected="
                            + BytesUtil.toString(expected.get(index)));
                // reset the buffer.
                os.reset();
                assertEquals(op.getName(code), len, actual.copy(index, os));
                assertTrue(0 == BytesUtil.compareBytesWithLenAndOffset(0, len,
                        os.array(), 0, len, expected.get(index)));
                break;
            }
            case Op.SEARCH: {
                /*
                 * Search with a key chosen randomly from the original data.
                 */
                if (!expected.isKeys()) {
                    // method not defined for values.
                    continue;
                }
                final int index = r.nextInt(size);
                final byte[] key = expected.get(index);
                if (log.isDebugEnabled())
                    log.debug(op.getName(code) + "(" + index + ") : key="
                            + BytesUtil.toString(key));
                { // search at the key.

                    assertEquals(op.getName(code), index, actual.search(key));
                    
                }
                { // search at key plus a random byte[] suffix.
                    
                    // random suffix length.
                    final int suffixLength = r.nextInt(1 + (key.length / 2)) + 1;
                    
                    // random fill of entire key.
                    final byte[] key2 = new byte[key.length + suffixLength];
                    r.nextBytes(key2);
                    
                    // copy shared prefix (all of the original key).
                    System.arraycopy(key, 0, key2, 0, key.length);
                    
                    // expected insert position (or index iff found).
                    final int epos = expected.search(key2);
                    
                    // actual result from search on the coded raba.
                    final int apos = actual.search(key2);
                    
                    assertEquals(op.getName(code), epos, apos);
                    
                }
                { // search at random length prefix of the key.
                    
                    // random prefix length (may be zero).
                    final int prefixLength = Math.max(0, r.nextInt(Math.max(1,
                            key.length)) - 1);
                    
                    // copy shared prefix.
                    final byte[] key2 = new byte[prefixLength];
                    System.arraycopy(key, 0, key2, 0, prefixLength);

                    // expected insert position (or index iff found).
                    final int epos = expected.search(key2);
                    
                    // actual result from search on the coded raba.
                    final int apos = actual.search(key2);
                    
                    assertEquals(op.getName(code), epos, apos);
                    
                }
                break;
            }
            case Op.ITERATOR: {
                if (log.isDebugEnabled())
                    log.debug(op.getName(code) + "()");
                assertSameIterator(expected.iterator(), actual.iterator());
                break;
            }
            case Op.RECODE: {

                /*
                 * Note: this uses a new buffer instance so we do not stomp on
                 * the existing coded representation backing the raba. The
                 * backing array for the buffer is preallocated to a modest size
                 * and filled with random data. When we setup the buffer, we
                 * then advance it a random #of bytes into the buffer so the
                 * raba will frequently be coded at a non-zero offset in the
                 * buffer.
                 */

                // backing byte[]. sometimes empty. will be extended on demand.
                final byte[] tmp = (r.nextFloat() < .1 ? new byte[0]
                        : new byte[r.nextInt(100) * size]);

                // fill it with random data.
                r.nextBytes(tmp);

                /*
                 * Start at random (but small) offset into the buffer with a
                 * bias to start at zero.
                 * 
                 * Note: A lot of re-coding errors are linked to a non-zero
                 * starting offset. If you set [start] to zero explicitly and
                 * the re-coding problem goes away, then the problem is a
                 * non-zero offset. Likewise, you can explicitly choose a
                 * non-zero start to debug a problem.
                 */

                // random start offset.
                final int start = Math.min(tmp.length, (r.nextFloat() < .2 ? 0
                        : r.nextInt(20)));
//                final int start = 0;

                if (log.isDebugEnabled())
                    log.debug(op.getName(code) + "(): start=" + start
                            + ", buf.len=" + tmp.length);

                // output buffer wrapping that byte[].
                final DataOutputBuffer buf = new DataOutputBuffer(start, tmp);

                try {
                    
                    // recode onto the buffer.
                    final AbstractFixedByteArrayBuffer data = rabaCoder.encode(
                            actual, buf);

                    // verify the same coding was produced.
                    assertEquals(originalData, data.toByteArray());
                    
                    // new instance wrapping the buffer.
                    actual = rabaCoder.decode(data);

                    // verify recoded raba.
                    AbstractBTreeTestCase.assertSameRaba(expected, actual);
                    
                } catch (AssertionFailedError ex) {
                    fail(op.getName(code) + "(): start=" + start + ", buf.len="
                            + tmp.length, ex);
                }

                break;
            }
            default:
                throw new AssertionError();
            }
            elapsed[code] = System.nanoTime() - begin;
            count[code]++;

        }

        double totalNS = 0;
        for (long ns : elapsed)
            totalNS += ns;
        
        final NumberFormat percentF = NumberFormat.getPercentInstance();
        percentF.setMinimumFractionDigits(2);
        final NumberFormat rateF = NumberFormat.getInstance();
        rateF.setMinimumFractionDigits(0);
        rateF.setMaximumFractionDigits(0);
        System.out.println("op\tcount\tnanos\t%time\tops/ms");
        for (int i = 0; i < count.length; i++) {

            if (count[i] == 0)
                continue;
            
            System.out.println(//
                    op.getName(i) + "\t"
                    + count[i]
                    + "\t"
                    + elapsed[i]
                    + "\t"
                    + percentF.format(elapsed[i] / totalNS)//
                    + "\t"
                    + (elapsed[i] == 0 ? "N/A" : rateF.format(count[i]
                                    / (elapsed[i] * scalingFactor))) //
                    );

        }
        
        // The size of the coded record.
        return originalData.length;
        
    }

    /**
     * Verify same byte[] iterators.
     * 
     * @param eitr
     *            The expected iterator.
     * @param aitr
     *            The actual iterator.
     */
    static protected void assertSameIterator(final Iterator<byte[]> eitr,
            final Iterator<byte[]> aitr) {

        int i = 0;
        while (eitr.hasNext()) {

            assertTrue("hasNext", aitr.hasNext());
            
            // verify same byte[] (compare data, may both be null).
            assertEquals("byte[" + i + "]", eitr.next(), aitr.next());

            i++;
            
        }
        
        assertFalse("hasNext", aitr.hasNext());
        
    }
    
    /**
     * Scaling factor converts nanoseconds to milliseconds.
     */
    static protected final double scalingFactor = 1d / TimeUnit.NANOSECONDS
            .convert(1, TimeUnit.MILLISECONDS);

    /**
     * Helper class generates a random sequence of operation codes obeying the
     * probability distribution described in the constructor call.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class Op {
        
        static public final int ISNULL = 0;
        static public final int LENGTH = 1;
        static public final int GET = 2;
        static public final int COPY = 3;
        static public final int SEARCH = 4;
        static public final int ITERATOR = 5;
        static public final int RECODE = 6;
        
        /**
         * The last defined operator.
         */
        static final int lastOp = RECODE;
        
//        final private Random r = new Random();
        
        final private float[] _dist;

        /*
         * isNull, length, get, copy, search, iterator, recode.
         */
        public Op(float isNullRate, float lengthRate, float getRate,
                float copyRate, float searchRate, float iteratorRate,
                float recodeRate)
        {
            if (isNullRate < 0 || lengthRate < 0 || getRate < 0
                    || copyRate < 0 || searchRate < 0 || iteratorRate < 0
                    || recodeRate < 0) {
                throw new IllegalArgumentException("negative rate");
            }
            float total = isNullRate + lengthRate + getRate + copyRate
                    + searchRate + iteratorRate + recodeRate;
            if( total == 0.0 ) {
                throw new IllegalArgumentException("all rates are zero.");
            }
            /*
             * Convert to normalized distribution in [0:1].
             */
            isNullRate /= total;
            lengthRate /= total;
            getRate /= total;
            copyRate /= total;
            searchRate /= total;
            iteratorRate /= total;
            recodeRate /= total;
            /*
             * Save distribution.
             */
            int i = 0;
            _dist = new float[lastOp+1];
            _dist[ i++ ] = isNullRate;
            _dist[ i++ ] = lengthRate;
            _dist[ i++ ] = getRate;
            _dist[ i++ ] = copyRate;
            _dist[ i++ ] = searchRate;
            _dist[ i++ ] = iteratorRate;
            _dist[ i++ ] = recodeRate;

            /*
             * Checksum.
             */
            float sum = 0f;
            for( i = 0; i<_dist.length; i++ ) {
                sum += _dist[ i ];
            }
            if( Math.abs( sum - 1f) > 0.01 ) {
                throw new AssertionError("sum of distribution is: "+sum+", but expecting 1.0");
            }
            
        }
        
        /**
         * Return the name of the operator.
         * 
         * @param op
         * @return
         */
        public String getName( final int op ) {
            if( op < 0 || op > lastOp ) {
                throw new IllegalArgumentException();
            }
            /*
             * isNull, length, get, copy, search, iterator, recode.
             */
            switch( op ) {
            case ISNULL:  return "isNull";
            case LENGTH:  return "length";
            case GET:     return "get   ";
            case COPY:    return "copy  ";
            case SEARCH:  return "search";
            case ITERATOR:return "itr   ";
            case RECODE:  return "recode";
            default:
                throw new AssertionError();
            }
        }
        
        /**
         * An array of normalized probabilities assigned to each operator. The
         * array may be indexed by the operator, e.g., dist[{@link #fetch}]
         * would be the probability of a fetch operation.
         * 
         * @return The probability distribution over the defined operators.
         */
        public float[] getDistribution() {
            return _dist;
        }

        /**
         * Generate a random operator according to the distribution described to
         * to the constructor.
         * 
         * @return A declared operator selected according to a probability
         *         distribution.
         */
        public int nextOp(final Random r) {
            final float rand = r.nextFloat(); // [0:1)
            float cumprob = 0f;
            for( int i=0; i<_dist.length; i++ ) {
                cumprob += _dist[ i ];
                if( rand <= cumprob ) {
                    return i;
                }
            }
            throw new AssertionError();
        }
        
    }

    /**
     * Tests of the {@link Op} test helper class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TestOp extends TestCase {

        private final Random r = new Random();
        
        public void test_Op() {
            /*
             * isNull, length, get, copy, search, iterator, recode.
             */
            Op gen = new Op(.2f, .05f, .2f, 05f, .1f, .05f, .001f);
            doOpTest(gen);
        }

        public void test_Op2() {
            /*
             * isNull, length, get, copy, search, iterator, recode.
             */
            Op gen = new Op(0f,0f,0f,1f,0f,0f,0f);
            doOpTest(gen);
        }

        /**
         * Correct rejection test when all rates are zero.
         */
        public void test_correctRejectionAllZero() {
            /*
             * isNull, length, get, copy, search, iterator, recode.
             */
            try {
                new Op(0f,0f,0f,0f,0f,0f,0f);
                fail("Expecting: "+IllegalArgumentException.class);
            }
            catch(IllegalArgumentException ex) {
                log.info("Ignoring expected exception: "+ex);
            }
        }

        /**
         * Correct rejection test when one or more rates are negative.
         */
        public void test_correctRejectionNegativeRate() {
            /*
             * isNull, length, get, copy, search, iterator, recode.
             */
            try {
                new Op(0f,0f,0f,-1f,0f,1f,0f);
                fail("Expecting: "+IllegalArgumentException.class);
            }
            catch(IllegalArgumentException ex) {
                log.info("Ignoring expected exception: "+ex);
            }
        }

        /**
         * Verifies the {@link Op} class given an instance with some probability
         * distribution.
         */
        void doOpTest(final Op gen) {
            final int limit = 10000;
            int[] ops = new int[limit];
            int[] sums = new int[Op.lastOp + 1];
            for (int i = 0; i < limit; i++) {
                int op = gen.nextOp(r);
                assertTrue(op >= 0);
                assertTrue(op <= Op.lastOp);
                ops[i] = op;
                sums[op]++;
            }
            float[] expectedProbDistribution = gen.getDistribution();
            float[] actualProbDistribution = new float[Op.lastOp + 1];
            float sum = 0f;
            for (int i = 0; i <= Op.lastOp; i++) {
                sum += expectedProbDistribution[i];
                actualProbDistribution[i] = (float) ((double) sums[i] / (double) limit);
                float diff = Math.abs(actualProbDistribution[i]
                        - expectedProbDistribution[i]);
                System.err.println("expected[i=" + i + "]="
                        + expectedProbDistribution[i] + ", actual[i=" + i
                        + "]=" + actualProbDistribution[i] + ", diff="
                        + ((int) (diff * 1000)) / 10f + "%");
                assertTrue(diff < 0.02); // difference is less than 2%
                                            // percent.
            }
            assertTrue(Math.abs(sum - 1f) < 0.01); // essential 1.0
        }

    }
    
}
