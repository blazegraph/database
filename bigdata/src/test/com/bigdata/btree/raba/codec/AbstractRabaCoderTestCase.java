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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase2;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.ReadOnlyKeysRaba;
import com.bigdata.btree.raba.ReadOnlyValuesRaba;

/**
 * Abstract test suite for {@link IRabaCoder} implementations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo performance tuning on stress test and on real data. be sure to factor
 *       out the {@link ByteBuffer} first.
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
     * The fixture under test. This will be <code>null</code> unless you
     * explicitly set it in {@link #setUp()}.
     */
    protected IRabaCoder rabaCoder = null;

    /**
     * FIXME Test search using select keys which should be located before or
     * after the coded keys. Do this for more than just this one test case.
     */
    public void test_mike_personick() throws UnsupportedEncodingException {
        
        final byte[][] a = new byte[2][];
        a[0] = "mike".getBytes("US-ASCII");
        a[1] = "personick".getBytes("US-ASCII");

        if (rabaCoder.isValueCoder()) {

            doRoundTripTest(rabaCoder, new ReadOnlyValuesRaba(a));

        }

        if (rabaCoder.isKeyCoder()) {

            final IRaba expected = new ReadOnlyKeysRaba(a);

            doRoundTripTest(rabaCoder, expected);

            final IRabaDecoder actual = rabaCoder.encode(expected);

            /*
             * Spot check the correct computation of the insertion point for a
             * variety of search keys.
             */
            
            // verify correct insertion point for an empty byte[].
            assertEquals(-1, actual.search(new byte[] {}));

            assertEquals(-1, actual.search(new byte[] { 'm', 'i', 'k' }));

            assertEquals(-2, actual.search(new byte[] { 'm', 'i', 'k', 'e', 's'}));

            assertEquals(-2, actual.search("personic".getBytes("US-ASCII")));

            assertEquals(-3, actual.search("personicks".getBytes("US-ASCII")));
            
        }

    }

    /**
     * Test with byte values which are negative values when interpreted as as
     * signed 8 bit integers.
     */
    public void test_negativeByteValues() throws UnsupportedEncodingException {

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
     * Test with a single byte value (nsymbols=1).
     * 
     * @throws UnsupportedEncodingException
     */
    public void test_nsymbolsOne() throws UnsupportedEncodingException {

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
     * Test with a single byte value (nsymbols=1) and some nulls.
     * 
     * @throws UnsupportedEncodingException
     */
    public void test_nsymbolsOne_nulls() throws UnsupportedEncodingException {

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
        a[0] = "mike".getBytes("US-ASCII");
        a[1] = "personick".getBytes("US-ASCII");
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
     * Maybe the problem is a failure to maintain the code words and the symbols
     * in the correct two-level ordering? However we encode the data twice and
     * get a different answer the 2nd time, which is not very good!
     * 
     * <pre>
     * java.lang.AssertionError: sumCodedValueBitLengths=3670 != sumCodedValueBitLengths2=3453
     *     at com.bigdata.btree.data.codec.TestHuffmanCodedValues.encode(TestHuffmanCodedValues.java:2526)
     *     at com.bigdata.btree.data.codec.TestHuffmanCodedValues.doRoundTripTest(TestHuffmanCodedValues.java:1292)
     *     at com.bigdata.btree.data.codec.TestHuffmanCodedValues.doRandomRoundTripTest(TestHuffmanCodedValues.java:1285)
     *     at com.bigdata.btree.data.codec.TestHuffmanCodedValues.test_huffmanCompression_1(TestHuffmanCodedValues.java:1137)
     * </pre>
     */
    public void test_entryCount1() throws IOException {

        doRandomRoundTripTest(rabaCoder, 1/* n */, 1/* capacity */);

        doRandomRoundTripTest(rabaCoder, 1/* n */, 2/* capacity */);

        doRandomRoundTripTest(rabaCoder, 1/* n */, 10/* capacity */);

    }

    public void test_entryCount2() throws IOException {

        doRandomRoundTripTest(rabaCoder, 2/* n */, 2/* capacity */);

        doRandomRoundTripTest(rabaCoder, 2/* n */, 3/* capacity */);

        doRandomRoundTripTest(rabaCoder, 2/* n */, 10/* capacity */);

    }
    
    /**
     * This test case was developed for the {@link FrontCodedRabaCoder}.
     */
    public void test_error1() throws IOException {

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

        /*
         * Generate 100 random and distinct URIs.
         * 
         * Note: These data are distinct but ARE NOT fully ordered. We sort them
         * before we use them as keys.
         */

        final byte[][] data = new byte[100][];
        
        final String ns = "http://www.bigdata.com/rdf#";
        
        long lastCounter = r.nextInt();
        
        for (int i = 0; i < 100; i++) {

            data[i] = KeyBuilder.asSortKey(ns + String.valueOf(lastCounter));

            final int inc = r.nextInt(100) + 1;

            assert inc > 0 : "inc="+inc;

            lastCounter += inc;
            
        }
        
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

                final boolean isNull = r.nextFloat()<.03;
                
                if(isNull) {
                    
                    data[i] = null;

                } else {

                    final int len = r.nextInt(512);

                    final byte[] a = new byte[len];

                    r.nextBytes(a);

                    data[i] = a;

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

    public void doRoundTripTest(final IRabaCoder rabaCoder, final IRaba expected) {

        try {

            // encode the logical byte[][].
            final IRabaDecoder actual0 = rabaCoder.encode(expected);

            // Verify encode() results in object which can decode the byte[]s.
            AbstractBTreeTestCase.assertSameRaba(expected, actual0);

            // Verify decode when we build the decoder from the serialized
            // format.
            AbstractBTreeTestCase.assertSameRaba(expected, rabaCoder
                    .decode(actual0.data()));
            
        } catch (Throwable t) {

            fail("Cause=" + t + ", expectedRaba=" + expected, t);

        }

    }

}
