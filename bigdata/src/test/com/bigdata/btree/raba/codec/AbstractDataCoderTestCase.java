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
 * @todo performance tuning on stress test and on real data.
 * 
 * @todo also test for probe keys that are not found.
 */
abstract public class AbstractDataCoderTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractDataCoderTestCase() {
    }

    /**
     * @param name
     */
    public AbstractDataCoderTestCase(String name) {
        super(name);
    }

    /**
     * The fixture under test. This will be <code>null</code> unless you
     * explicitly set it in {@link #setUp()}.
     */
    protected IRabaCoder dataCoder = null;

    /**
     * @todo test decode w/ nulls and delete markers and verify this. deleted
     *       values should appear as it they have nulls. nulls should be
     *       represented as zero length coded values but also have the "null"
     *       bit set for that value. On decode, we need to check the "null" bit
     *       and return null if the value was null.
     * 
     * @todo test search.
     */
    public void test_mike_personick() throws UnsupportedEncodingException {
        
        // example: [ mike ], [ personick ]

        // nvalues=2 // not used!
        
        // keyLength[] = {4 9} // not used!
        
        // nsymbols=10
        
        // symbols (the distinct byte values observed, written in lexical order).
        // c e i k m n o p r s

        // packedFrequency[]
        // 1 2 2 2 1 1 1 1 1 1
        
        // <num compressed bytes> <compressed bytes> 
        final byte[][] a = new byte[2][];
        a[0] = "mike".getBytes("US-ASCII");
        a[1] = "personick".getBytes("US-ASCII");

        if(dataCoder.isKeyCoder()) {

            doRoundTripTest(dataCoder, new ReadOnlyKeysRaba(a));
            
        }

        if(dataCoder.isValueCoder()) {

            doRoundTripTest(dataCoder, new ReadOnlyValuesRaba(a));
            
        }

    }

    /**
     * Test with byte values which are negative values when interpreted as as
     * signed 8 bit integers.
     */
    public void test_negativeByteValues() throws UnsupportedEncodingException {

        final byte[][] a = new byte[1][];

        a[0] = new byte[] { 64, -64 };

        if (dataCoder.isKeyCoder()) {

            doRoundTripTest(dataCoder, new ReadOnlyKeysRaba(a));

        }

        if (dataCoder.isValueCoder()) {

            doRoundTripTest(dataCoder, new ReadOnlyValuesRaba(a));

        }

    }

    /**
     * Test with an empty byte[] element.
     * 
     * @throws UnsupportedEncodingException
     */
    public void test_withEmptyElement() throws UnsupportedEncodingException {

        final byte[][] a = new byte[3][];
        a[0] = new byte[0];
        a[1] = "mike".getBytes("US-ASCII");
        a[2] = "personick".getBytes("US-ASCII");
        
        if (dataCoder.isKeyCoder()) {

            doRoundTripTest(dataCoder, new ReadOnlyKeysRaba(a));

        }

        if (dataCoder.isValueCoder()) {

            doRoundTripTest(dataCoder, new ReadOnlyValuesRaba(a));

        }

    }


    /**
     * Test with a null value.
     * 
     * @throws UnsupportedEncodingException
     */
    public void test_withNulls() throws UnsupportedEncodingException {

        if (!dataCoder.isValueCoder()) {

            // coded does not allow nulls.
            return;
            
        }
        
        final byte[][] a = new byte[3][];
        a[0] = "mike".getBytes("US-ASCII");
        a[1] = "personick".getBytes("US-ASCII");
        a[2] = null;
        
        final IRaba expected = new ReadOnlyValuesRaba(a);

        doRoundTripTest(dataCoder, expected);

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
        
        if (!dataCoder.isValueCoder()) {

            // coded does not allow nulls.
            return;
            
        }
        
        final byte[][] a = new byte[1][];

        a[0] = null;
        
        final IRaba expected = new ReadOnlyValuesRaba(a);

        doRoundTripTest(dataCoder, expected);

    }

    final Random r = new Random();

    public void test_empty() throws IOException {

        doRandomRoundTripTest(dataCoder, 0/* size */, 0/*capacity*/);

        doRandomRoundTripTest(dataCoder, 0/* size */, 1/*capacity*/);
        
        doRandomRoundTripTest(dataCoder, 0/* size */, 2/*capacity*/);
        
        doRandomRoundTripTest(dataCoder, 0/* size */, 10/*capacity*/);
        
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

        doRandomRoundTripTest(dataCoder, 1/* n */, 1/* capacity */);

        doRandomRoundTripTest(dataCoder, 1/* n */, 2/* capacity */);

        doRandomRoundTripTest(dataCoder, 1/* n */, 10/* capacity */);

    }

    public void test_entryCount2() throws IOException {

        doRandomRoundTripTest(dataCoder, 2/* n */, 2/* capacity */);

        doRandomRoundTripTest(dataCoder, 2/* n */, 3/* capacity */);

        doRandomRoundTripTest(dataCoder, 2/* n */, 10/* capacity */);

    }
    
    public void test_randomOnce() throws IOException {
        
        // #of elements.
        final int n = r.nextInt(100);

        // capacity of the array.
        final int capacity = n + r.nextInt(n); 
        
        doRandomRoundTripTest(dataCoder, n, capacity);
        
    }

    public void testStress() throws IOException {
        
        for (int i = 0; i < 1000; i++) {

            // #of elements.
            final int n = r.nextInt(100) + 0;

            // capacity of the array.
            final int capacity = n + r.nextInt(n + 1);

            doRandomRoundTripTest(dataCoder, n, capacity);

        }

    }

    /**
     * Interpret the string as a sequence of ASCII characters and encode it as
     * an unsigned byte[].
     * 
     * @param s
     *            The string
     * 
     * @return The unsigned byte[].
     * 
     * @todo why not use the real {@link KeyBuilder}?
     */
    protected byte[] makeKey(final String s) {
    
        final int len = s.length();
        
        //ensureFree(len);
        
        final byte[] buf = new byte[len];

        for (int j = 0; j < len; j++) {

            final char ch = s.charAt(j);
            
    //        append((byte)(ch & 0xff));
    
            // lexiographic ordering as unsigned byte.
            
            int v = (byte)ch;
            
            if (v < 0) {
    
                v = v - 0x80;
    
            } else {
                
                v = v + 0x80;
                
            }
            
            buf[j] = (byte)(v & 0xff);
    
        }
        
        return buf;

    }
    
    /**
     * Test using a sequence of random URIs (formed with successive prefixes).
     * 
     * @throws Exception
     */
    public void test_randomURIs() throws Exception {
        
        // generate 100 random URIs

        final byte[][] data = new byte[100][];
        
        final String ns = "http://www.bigdata.com/rdf#";
        
        long lastCounter = r.nextInt();
        
        for (int i = 0; i < 100; i++) {
            
            data[i] = makeKey(ns + String.valueOf(lastCounter)); 
                
            lastCounter = lastCounter + r.nextInt(100) + 1;
            
        }
        
//        // put into sorted order.
//        Arrays.sort(data, 0, data.length, UnsignedByteArrayComparator.INSTANCE);

        if (dataCoder.isValueCoder()) {

            // layer on interface.
            final IRaba raba = new ReadOnlyValuesRaba(0/* fromIndex */,
                    data.length/* toIndex */, data.length/* capacity */, data);

            doRoundTripTest(dataCoder, raba);

        }

        if (dataCoder.isKeyCoder()) {

            // layer on interface.
            final IRaba raba = new ReadOnlyKeysRaba(0/* fromIndex */,
                    data.length/* toIndex */, data.length/* capacity */, data);

            doRoundTripTest(dataCoder, raba);

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

    public void doRoundTripTest(final IRabaCoder dataCoder, final IRaba expected) {

        // encode the logical byte[][].
        final IRabaDecoder actual0 = dataCoder.encode(expected);

        // Verify encode() results in object which can decode the byte[]s.
        AbstractBTreeTestCase.assertSameRaba(expected, actual0);

        // Verify decode when we build the decoder from the serialized format.
        AbstractBTreeTestCase.assertSameRaba(expected, dataCoder.decode(actual0
                .data()));

    }

}
