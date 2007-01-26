/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Jan 16, 2007
 */

package com.bigdata.objndx;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import junit.framework.TestCase2;

import com.bigdata.journal.Bytes;
import com.bigdata.objndx.ndx.NoSuccessorException;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;

/**
 * Test suite for high level operations that build variable length _unsigned_
 * byte[] keys from various data types and unicode strings.
 * 
 * @see http://docs.hp.com/en/B3906-90004/ch02s02.html#d0e1095 for ranges on
 * negative float and double values.
 * 
 * @todo finish the double precision tests per the single precision tests, e.g.,
 *       for successor(double), etc.
 * 
 * @todo write performance test for encoding strings, possibly in the context of
 *       parsed rdf data, and see if there are any easy wins in how the encoding
 *       to a sort key is handled or in alignment of the apis.
 * 
 * @todo use ICU4J as provider for compression for unicode fields, perhaps
 *       factoring a minimal module into extSer for that purpose.
 * 
 * @todo document use of ICU for forming keys but note that this feature really
 *       belongs in the client not the server. when dividing bigdata into client -
 *       server APIs, ICU will go into the client API. Also, a minimum ICU
 *       module could be built to support sort key generation. Integrate support
 *       for ICU versioning into the client and perhaps into the index metadata
 *       so clients can discover which version to use when generating keys for
 *       an index, and also the locale and other parameters for the collation
 *       (PRIMARY, SECONDARY, etc). Support the JNI integration of ICU (native
 *       code for faster generation of sort keys).
 * 
 * @todo write tests in that construct keys for a triple store, quad store,
 *       column store, and the metadata index for a paritioned index.
 * 
 * @todo refactor the bulk insert tests into another test suite since they
 *       introduce a dependency on the btree implementation rather than just a
 *       focus on the correctness and performance of the key encoder.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestKeyBuilder extends TestCase2 {

    /**
     * 
     */
    public TestKeyBuilder() {
    }

    /**
     * @param name
     */
    public TestKeyBuilder(String name) {
        super(name);
    }

    /**
     * ctor tests, including correct rejection.
     */
    public void test_ctor() {

        {
            KeyBuilder keyBuilder = new KeyBuilder();
            
            assertNotNull(keyBuilder.buf);
            assertEquals(KeyBuilder.DEFAULT_INITIAL_CAPACITY,keyBuilder.buf.length);
            assertEquals(0,keyBuilder.len);
            assertNotNull(keyBuilder.collator);

        }
        
        {
            KeyBuilder keyBuilder = new KeyBuilder(0);
            assertNotNull(keyBuilder.buf);
            assertEquals(0,keyBuilder.buf.length);
            assertEquals(0,keyBuilder.len);
            assertNotNull(keyBuilder.collator);
        }
        
        {
            KeyBuilder keyBuilder = new KeyBuilder(20);
            assertNotNull(keyBuilder.buf);
            assertEquals(20,keyBuilder.buf.length);
            assertEquals(0,keyBuilder.len);
            assertNotNull(keyBuilder.collator);
        }
        
        {
            KeyBuilder keyBuilder = new KeyBuilder((RuleBasedCollator)Collator.getInstance(Locale.UK),30);
            assertNotNull(keyBuilder.buf);
            assertEquals(30,keyBuilder.buf.length);
            assertEquals(0,keyBuilder.len);
            assertNotNull(keyBuilder.collator);
        }
        
        {
            final byte[] expected = new byte[]{1,2,3,4,5,6,7,8,9,10};
            KeyBuilder keyBuilder = new KeyBuilder((RuleBasedCollator)Collator.getInstance(Locale.UK),4,expected);
            assertNotNull(keyBuilder.buf);
            assertEquals(4,keyBuilder.len);
            assertEquals(10,keyBuilder.buf.length);
            assertTrue(expected==keyBuilder.buf);
            assertNotNull(keyBuilder.collator);
        }

        /*
         * correct rejection tests.
         */
        {
            try {
                new KeyBuilder(-1);
                fail("Expecting: "+IllegalArgumentException.class);
            } catch(IllegalArgumentException ex) {
                System.err.println("Ignoring expected exception: "+ex);
            }
        }

        {
            try {
                new KeyBuilder(null,20);
                fail("Expecting: "+IllegalArgumentException.class);
            } catch(IllegalArgumentException ex) {
                System.err.println("Ignoring expected exception: "+ex);
            }
        }

        {
            try {
                new KeyBuilder((RuleBasedCollator)Collator.getInstance(Locale.UK),-20);
                fail("Expecting: "+IllegalArgumentException.class);
            } catch(IllegalArgumentException ex) {
                System.err.println("Ignoring expected exception: "+ex);
            }
        }
        
    }
    
    public void test_keyBuilder_ensureCapacity() {
        
        KeyBuilder keyBuilder = new KeyBuilder(0);

        assertEquals(0,keyBuilder.len);
        assertNotNull( keyBuilder.buf);
        assertEquals(0,keyBuilder.buf.length);

        final byte[] originalBuffer = keyBuilder.buf;
        
        // correct rejection.
        try {
            keyBuilder.ensureCapacity(-1);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        assertTrue(originalBuffer==keyBuilder.buf); // same buffer.
        
        // no change.
        keyBuilder.ensureCapacity(0);
        assertEquals(0,keyBuilder.len);
        assertNotNull( keyBuilder.buf);
        assertEquals(0,keyBuilder.buf.length);
        assertTrue(originalBuffer==keyBuilder.buf); // same buffer.
    }
    
    public void test_keyBuilder_ensureCapacity02() {
        
        KeyBuilder keyBuilder = new KeyBuilder(0);

        assertEquals(0,keyBuilder.len);
        assertNotNull( keyBuilder.buf);
        assertEquals(0,keyBuilder.buf.length);

        final byte[] originalBuffer = keyBuilder.buf;
        
        // extends buffer.
        keyBuilder.ensureCapacity(100);
        assertEquals(0,keyBuilder.len);
        assertNotNull( keyBuilder.buf);
        assertEquals(100,keyBuilder.buf.length);
        assertTrue(originalBuffer!=keyBuilder.buf); // same buffer.
    }
    
    /**
     * verify that existing data is preserved if the capacity is extended.
     */
    public void test_keyBuilder_ensureCapacity03() {

        Random r = new Random();
        byte[] expected = new byte[20];
        r.nextBytes(expected);

        KeyBuilder keyBuilder = new KeyBuilder(20,expected);

        assertEquals(20,keyBuilder.len);
        assertNotNull( keyBuilder.buf);
        assertTrue(expected==keyBuilder.buf);

        keyBuilder.ensureCapacity(30);
        assertEquals(20,keyBuilder.len);
        assertEquals(30,keyBuilder.buf.length);

        assertEquals(0,BytesUtil.compareBytesWithLenAndOffset(0, expected.length, expected, 0, expected.length, keyBuilder.buf));
        
        for(int i=21;i<30; i++) {
            assertEquals(0,keyBuilder.buf[i]);
        }
        
    }

    public void test_keyBuilder_ensureFree() {
        
        KeyBuilder keyBuilder = new KeyBuilder(0);

        assertEquals(0,keyBuilder.len);
        assertNotNull( keyBuilder.buf);
        assertEquals(0,keyBuilder.buf.length);
    
        keyBuilder.ensureFree(2);
        
        assertEquals(0,keyBuilder.len);
        assertNotNull( keyBuilder.buf);
        assertTrue(keyBuilder.buf.length>=2);
        
    }
    
    /**
     * Tests ability to append to the buffer, including with overflow of the
     * buffer capacity.
     */
    public void test_keyBuilder_append_bytes() {
        
        // setup buffer with some data and two(2) free bytes.
        KeyBuilder keyBuilder = new KeyBuilder(5,new byte[]{1,2,3,4,5,0,0});
        
        /*
         * fill to capacity by copying two bytes from the middle of another
         * array. since this does not overflow we know the exact capacity of the
         * internal buffer (it is not reallocated).
         */
        byte[] tmp = new byte[]{4,5,6,7,8,9};
        keyBuilder.append(2,2,tmp);
        assertEquals(7,keyBuilder.len);
        assertEquals(new byte[]{1,2,3,4,5,6,7}, keyBuilder.buf);
        assertEquals(0,BytesUtil.compareBytes(new byte[]{1,2,3,4,5,6,7}, keyBuilder.buf));
        
        // overflow capacity (new capacity is not known in advance).
        tmp = new byte[] { 8, 9, 10 };
        byte[] expected = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        keyBuilder.append(tmp);
        assertEquals(10, keyBuilder.len);
        assertEquals(0, BytesUtil.compareBytesWithLenAndOffset(0, expected.length, expected, 0,
                keyBuilder.len, keyBuilder.buf));

        // possible overflow (old and new capacity are unknown).
        tmp = new byte[] { 11, 12 };
        expected = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        keyBuilder.append(tmp);
        assertEquals(12, keyBuilder.len);
        assertEquals(0, BytesUtil.compareBytesWithLenAndOffset(0, expected.length, expected, 0,
                keyBuilder.len, keyBuilder.buf));
        
    }

    /**
     * Test ability to extract and return a key.
     */
    public void test_keyBuilder_getKey() {
        
        KeyBuilder keyBuilder = new KeyBuilder(5,new byte[]{1,2,3,4,5,6,7,8,9,10});
        
        byte[] key = keyBuilder.getKey();
        
        assertEquals(5,key.length);
        assertEquals(new byte[]{1,2,3,4,5},key);
        
    }
    
    /**
     * Verify returns zero length byte[] when the key has zero bytes.
     */
    public void test_keyBuilder_getKey_len0() {

        KeyBuilder keyBuilder = new KeyBuilder();
        
        byte[] key = keyBuilder.getKey();

        assertEquals(0,key.length);
        
    }
    
    /**
     * Test ability to reset the key buffer (simply zeros the #of valid bytes
     * in the buffer without touching the buffer itself).
     */
    public void test_keyBuilder_reset() {

        byte[] expected = new byte[10];
    
        KeyBuilder keyBuilder = new KeyBuilder(5,expected);
        
        assertEquals(5,keyBuilder.len);
        assertTrue(expected==keyBuilder.buf);
        
        assertTrue(keyBuilder == keyBuilder.reset());
        
        assertEquals(0,keyBuilder.len);
        assertTrue(expected==keyBuilder.buf);

    }
    
    /*
     * test append keys for each data type, including that sort order of
     * successors around zero is correctly defined by the resulting key.
     */

    public void test_keyBuilder_byte_key() {
        
        KeyBuilder keyBuilder = new KeyBuilder();

        final byte bmin = Byte.MIN_VALUE;
        final byte bm1  = (byte)-1;
        final byte b0   = (byte) 0;
        final byte bp1  = (byte) 1;
        final byte bmax = Byte.MAX_VALUE;
        
        byte[] kmin = keyBuilder.reset().append(bmin).getKey();
        byte[] km1 = keyBuilder.reset().append(bm1).getKey();
        byte[] k0 = keyBuilder.reset().append(b0).getKey();
        byte[] kp1 = keyBuilder.reset().append(bp1).getKey();
        byte[] kmax = keyBuilder.reset().append(bmax).getKey();

        assertEquals(1,kmin.length);
        assertEquals(1,km1.length);
        assertEquals(1,k0.length);
        assertEquals(1,kp1.length);
        assertEquals(1,kmax.length);
        
        System.err.println("kmin("+bmin+")="+BytesUtil.toString(kmin));
        System.err.println("km1("+bm1+")="+BytesUtil.toString(km1));
        System.err.println("k0("+b0+")="+BytesUtil.toString(k0));
        System.err.println("kp1("+bp1+")="+BytesUtil.toString(kp1));
        System.err.println("kmax("+bmax+")="+BytesUtil.toString(kmax));
        
        assertTrue("kmin<km1",BytesUtil.compareBytes(kmin, km1)<0);
        assertTrue("km1<k0",BytesUtil.compareBytes(km1, k0)<0);
        assertTrue("k0<kp1",BytesUtil.compareBytes(k0, kp1)<0);
        assertTrue("kp1<kmax",BytesUtil.compareBytes(kp1, kmax)<0);

        /*
         * verify decoding.
         */
        assertEquals("kmin",bmin,KeyBuilder.decodeByte(kmin[0]));
        assertEquals("km1" ,bm1 ,KeyBuilder.decodeByte(km1[0]));
        assertEquals("k0"  ,b0  ,KeyBuilder.decodeByte(k0[0]));
        assertEquals("kp1" ,bp1 ,KeyBuilder.decodeByte(kp1[0]));
        assertEquals("kmax",bmax,KeyBuilder.decodeByte(kmax[0]));

    }
    
    public void test_keyBuilder_short_key() {
        
        KeyBuilder keyBuilder = new KeyBuilder();

        final short smin = Short.MIN_VALUE;
        final short sm1  = (short)-1;
        final short s0   = (short) 0;
        final short sp1  = (short) 1;
        final short smax = Short.MAX_VALUE;
        
        byte[] kmin = keyBuilder.reset().append(smin).getKey();
        byte[] km1 = keyBuilder.reset().append(sm1).getKey();
        byte[] k0 = keyBuilder.reset().append(s0).getKey();
        byte[] kp1 = keyBuilder.reset().append(sp1).getKey();
        byte[] kmax = keyBuilder.reset().append(smax).getKey();

        assertEquals(2,kmin.length);
        assertEquals(2,km1.length);
        assertEquals(2,k0.length);
        assertEquals(2,kp1.length);
        assertEquals(2,kmax.length);

        System.err.println("kmin("+smin+")="+BytesUtil.toString(kmin));
        System.err.println("km1("+sm1+")="+BytesUtil.toString(km1));
        System.err.println("k0("+s0+")="+BytesUtil.toString(k0));
        System.err.println("kp1("+sp1+")="+BytesUtil.toString(kp1));
        System.err.println("kmax("+smax+")="+BytesUtil.toString(kmax));
        
        assertTrue("kmin<km1",BytesUtil.compareBytes(kmin, km1)<0);
        assertTrue("km1<k0",BytesUtil.compareBytes(km1, k0)<0);
        assertTrue("k0<kp1",BytesUtil.compareBytes(k0, kp1)<0);
        assertTrue("kp1<kmax",BytesUtil.compareBytes(kp1, kmax)<0);

    }
    
    public void test_keyBuilder_int_key() {
        
        KeyBuilder keyBuilder = new KeyBuilder();

        final int imin = Integer.MIN_VALUE;
        final int im1 = -1;
        final int i0 = 0;
        final int ip1 = 1;
        final int imax = Integer.MAX_VALUE;

        byte[] kmin = keyBuilder.reset().append(imin).getKey();
        byte[] km1 = keyBuilder.reset().append(im1).getKey();
        byte[] k0 = keyBuilder.reset().append(i0).getKey();
        byte[] kp1 = keyBuilder.reset().append(ip1).getKey();
        byte[] kmax = keyBuilder.reset().append(imax).getKey();

        assertEquals(4,kmin.length);
        assertEquals(4,km1.length);
        assertEquals(4,k0.length);
        assertEquals(4,kp1.length);
        assertEquals(4,kmax.length);

        System.err.println("kmin("+imin+")="+BytesUtil.toString(kmin));
        System.err.println("km1("+im1+")="+BytesUtil.toString(km1));
        System.err.println("k0("+i0+")="+BytesUtil.toString(k0));
        System.err.println("kp1("+ip1+")="+BytesUtil.toString(kp1));
        System.err.println("kmax("+imax+")="+BytesUtil.toString(kmax));
        
        assertTrue("kmin<km1",BytesUtil.compareBytes(kmin, km1)<0);
        assertTrue("km1<k0",BytesUtil.compareBytes(km1, k0)<0);
        assertTrue("k0<kp1",BytesUtil.compareBytes(k0, kp1)<0);
        assertTrue("kp1<kmax",BytesUtil.compareBytes(kp1, kmax)<0);

    }

    public void test_keyBuilder_long_key() {
        
        KeyBuilder keyBuilder = new KeyBuilder();
        
        final long lmin = Long.MIN_VALUE;
        final long lm1 = -1L;
        final long l0 = 0L;
        final long lp1 = 1L;
        final long lmax = Long.MAX_VALUE;
        
        byte[] kmin = keyBuilder.reset().append(lmin).getKey();
        byte[] km1 = keyBuilder.reset().append(lm1).getKey();
        byte[] k0 = keyBuilder.reset().append(l0).getKey();
        byte[] kp1 = keyBuilder.reset().append(lp1).getKey();
        byte[] kmax = keyBuilder.reset().append(lmax).getKey();

        assertEquals(8,kmin.length);
        assertEquals(8,km1.length);
        assertEquals(8,k0.length);
        assertEquals(8,kp1.length);
        assertEquals(8,kmax.length);

        System.err.println("kmin("+lmin+")="+BytesUtil.toString(kmin));
        System.err.println("km1("+lm1+")="+BytesUtil.toString(km1));
        System.err.println("k0("+l0+")="+BytesUtil.toString(k0));
        System.err.println("kp1("+lp1+")="+BytesUtil.toString(kp1));
        System.err.println("kmax("+lmax+")="+BytesUtil.toString(kmax));
        
        assertTrue("kmin<km1",BytesUtil.compareBytes(kmin, km1)<0);
        assertTrue("km1<k0",BytesUtil.compareBytes(km1, k0)<0);
        assertTrue("k0<kp1",BytesUtil.compareBytes(k0, kp1)<0);
        assertTrue("kp1<kmax",BytesUtil.compareBytes(kp1, kmax)<0);

        /*
         * verify decoding.
         * 
         * @todo test decoding at offsets != 0.
         */
        assertEquals("kmin",lmin,KeyBuilder.decodeLong(kmin, 0));
        assertEquals("km1" ,lm1 ,KeyBuilder.decodeLong(km1 , 0));
        assertEquals("k0"  ,l0  ,KeyBuilder.decodeLong(k0  , 0));
        assertEquals("kp1" ,lp1 ,KeyBuilder.decodeLong(kp1 , 0));
        assertEquals("kmax",lmax,KeyBuilder.decodeLong(kmax, 0));
        
    }

    /**
     * Positive zero.
     */
    final protected float fp0 = Float.intBitsToFloat(0x00000000);

    /**
     * Negative zero.
     */
    final protected float fm0 = Float.intBitsToFloat(0x80000000);
    
    /**
     * Smallest non-zero positive value.
     */
    final protected float fpos = Float.intBitsToFloat(0x00000001);
    
    /**
     * Smallest non-zero negative value.
     */
    final protected float fneg = Float.intBitsToFloat(0x80000001);
    
    /**
     * Positive one (1f).
     */
    final protected float fp1 = 1f;

    /**
     * Negative one (-1f).
     */
    final protected float fn1 = -1f;

    /**
     * Largest negative float.
     */
    final protected float fmin = Float.intBitsToFloat(0xFF7FFFFF);
    
    /**
     * Largest positive float.
     */
    final protected float fmax = Float.MAX_VALUE;
    
    public void test_float_data_points() {

        // positive value space.
        assertTrue(fp0<fmax);
        assertTrue(fp0<fpos);
        assertTrue(fpos<fp1);
        
        // negative value space.
        assertTrue(fneg>fmin);
        assertTrue(fm0>fneg);
        assertTrue(fneg>fn1);

        /*
         * Note: +0f and -0f will compare as _equal_ in the langage. This means
         * that you can not write tests that directly distinguish positive and
         * negative zero using >, <, or ==.
         */
        assertTrue(fm0 == fp0);
        
    }
    
    /**
     * Positive zero.
     */
    final protected double dp0 = Double.longBitsToDouble(0x0000000000000000L);

    /**
     * Negative zero.
     */
    final protected double dm0 = Double.longBitsToDouble(0x8000000000000000L);
    
    /**
     * Smallest non-zero positive value.
     */
    final protected double dpos = Double.longBitsToDouble(0x0000000000000001L);
    
    /**
     * Smallest non-zero negative value.
     */
    final protected double dneg = Double.longBitsToDouble(0x8000000000000001L);
    
    /**
     * Positive one (1d).
     */
    final protected double dp1 = 1d;

    /**
     * Negative one (-1d).
     */
    final protected double dn1 = -1d;

    /**
     * Largest negative double.
     */
    final protected double dmin = Double.longBitsToDouble(0xFFEFFFFFFFFFFFFFL);
    
    /**
     * Largest positive double.
     */
    final protected double dmax = Double.MAX_VALUE;
    
    public void test_double_data_points() {

        // positive value space.
        assertTrue(dp0<dmax);
        assertTrue(dp0<dpos);
        assertTrue(dpos<dp1);
        
        // negative value space.
        assertTrue(dneg>dmin);
        assertTrue(dm0>dneg);
        assertTrue(dneg>dn1);

        /*
         * Note: +0f and -0f will compare as _equal_ in the langage. This means
         * that you can not write tests that directly distinguish positive and
         * negative zero using >, <, or ==.
         */
        assertTrue(dm0 == dp0);
        
    }
    
//    /**
//     * The double precision number that is one less than zero in the double
//     * precision value space.
//     */
//    final protected double dm1 = Double.longBitsToDouble(KeyBuilder.d2l(0d)-1);
//
//    /**
//     * The double precision number that is one more than zero in the double
//     * precision value space.
//     */
//    final protected double dp1 = Double.longBitsToDouble(KeyBuilder.d2l(0d)-1);
    
    /**
     * Derived from <a
     * href="http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
     * Comparing floating point numbers </a> by Bruce Dawson.
     *
     * @param A A floating point value.
     * @param B Another floating point value
     * @param maxUlps The maximum error in terms of Units in the Last
     * Place. This specifies how big an error we are willing to accept
     * in terms of the value of the least significant digit of the
     * floating point number’s representation.  <i>maxUlps</i> can
     * also be interpreted in terms of how many representable floats
     * we are willing to accept between A and B.  This function will
     * allow <code>maxUlps-1</code> floats between <i>A</i> and
     * <i>B</i>.
     */
    static protected int getUlps(float A, float B) {

        int aInt = Float.floatToIntBits(A); // *(int*)&A;

        // Make aInt lexicographically ordered as a twos-complement
        // int.

        if (aInt < 0) {

            aInt = 0x80000000 - aInt;

        }

        // Make bInt lexicographically ordered as a twos-complement
        // int.

        int bInt = Float.floatToIntBits(B); // *(int*)&B;

        if (bInt < 0) {

            bInt = 0x80000000 - bInt;

        }

        int intDiff = Math.abs(aInt - bInt);

        return intDiff;

    }

    /**
     * Derived from <a
     * href="http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
     * Comparing floating point numbers </a> by Bruce Dawson.
     *
     * @param A A double precision floating point value.
     * @param B Another double precision floating point value
     * @param maxUlps The maximum error in terms of Units in the Last
     * Place. This specifies how big an error we are willing to accept
     * in terms of the value of the least significant digit of the
     * floating point number’s representation.  <i>maxUlps</i> can
     * also be interpreted in terms of how many representable doubles
     * we are willing to accept between A and B.  This function will
     * allow <code>maxUlps-1</code> doubles between <i>A</i> and
     * <i>B</i>.
     */
    static protected long getUlps(double A, double B) {

        long aLong = Double.doubleToLongBits(A); // *(int*)&A;

        // Make aInt lexicographically ordered as a twos-complement
        // long.

        if (aLong < 0) {

            aLong = 0x8000000000000000L - aLong;

        }

        // Make bInt lexicographically ordered as a twos-complement
        // int.

        long bLong = Double.doubleToLongBits(B); // *(int*)&B;

        if (bLong < 0) {

            bLong = 0x8000000000000000L - bLong;

        }

        long longDiff = Math.abs(aLong - bLong);

        return longDiff;

    }

    /**
     * Verify zero ULPs difference between the values.
     * @param f1
     * @param f2
     */
    protected void assertZeroUlps(float f1, float f2) {

        int ulps = getUlps(f1,f2);
        
        if( ulps != 0 ) {

            fail("Expecting zero ulps, but found: "+ulps+"; f1="+f1+", f2="+f2);
            
        }
        
    }
    
    /**
     * The value space for <code>float</code> and <code>double</code> is
     * complex. The "next" floating point value is defined in terms of the next
     * bit pattern for the underlying floating point representation. Also, we
     * have to be aware of the trade off between precision and magnitude, the
     * maximum expressible value, NaNs, positive and negative infinity, etc.
     * <p>
     * 
     * First we verify the data that we are going to use in the test. Each of
     * these asserts verifies that a bit pattern, expressed as an int, is
     * equilivant to the specified float. This SHOULD be true, but it is down to
     * the metal enough to make it worth checking.
     * <p>
     * 
     * These test data were derived from: <a
     * href="http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
     * Comparing floating point numbers </a> by Bruce Dawson.
     */
    public void test_keyBuilder_successor_float_data()
    {

        assertZeroUlps
            ( 1.99999976f,  // 0x3FFFFFFE
              Float.intBitsToFloat( 1073741822 )
              );
    
        assertZeroUlps
            ( 1.99999988f,  //  0x3FFFFFFF
              Float.intBitsToFloat( 1073741823 )
              );
     
        assertZeroUlps
            ( 2.00000000f,  //  0x40000000
              Float.intBitsToFloat( 1073741824 )
              );
     
        assertZeroUlps
            ( 2.00000024f,  // 0x40000001
              Float.intBitsToFloat( 1073741825 )
              );
     
        assertZeroUlps
            ( 2.00000048f,  //  0x40000002
              Float.intBitsToFloat( 1073741826 )
              );
    
    }

    /**
     * Now verify the successor for each of the data points that we
     * verified in the previous test as being successors in the float
     * value space.
     */
    public void test_keyBuilder_successor_float_positiveValue()
        throws NoSuccessorException
    {

        assertZeroUlps(1.99999988f, KeyBuilder.successor(1.99999976f));

        assertZeroUlps(2.00000000f, KeyBuilder.successor(1.99999988f));

        assertZeroUlps(2.00000024f, KeyBuilder.successor(2.00000000f));

        assertZeroUlps(2.00000048f, KeyBuilder.successor(2.00000024f));

    }

    /**
     * Test a few values from the middle of the negative region of the value
     * space.  The successor is formed by subtracting one from the integer
     * representation when in the negative value space.
     */
    public void test_keyBuilder_successor_float_negativeValue()
        throws NoSuccessorException
    {

        float neg1 = Float.intBitsToFloat(0x81111111);
        
        float neg2 = Float.intBitsToFloat(0x81111110);
        
        System.err.println("neg1="+neg1);
        System.err.println("neg2="+neg2);
        
        assertTrue("neg1<neg2", neg1<neg2);
        
        assertZeroUlps(neg2, KeyBuilder.successor(neg1));

    }

    /**
     * Verifies some data points that we use in the next test. These test data
     * were derived from:
     * 
     * <a
     * href="http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
     * Comparing floating point numbers </a> by Bruce Dawson.
     */
    public void test_keyBuilder_successor_float_nearZero_data()
    {
    
        assertZeroUlps
            ( 4.2038954e-045f,  // 0x00000003
              Float.intBitsToFloat( 3 )
              );
     
        assertZeroUlps
            ( 2.8025969e-045f,  // 0x00000002
              Float.intBitsToFloat( 2 )
              );
     
        assertZeroUlps
            ( 1.4012985e-045f,  // 0x00000001
              Float.intBitsToFloat( 1 )
              );
     
        assertZeroUlps
            ( +0.00000000f, // 0x00000000
              Float.intBitsToFloat( 0 )
              );
     
        assertZeroUlps
            ( -0.00000000f, // 0x80000000
              Float.intBitsToFloat( -2147483648 )
              );


        // Note: -0.0f and +0.0f are often conflated by the language.
//        assertTrue( +0.00000000f !=   // 0x00000000
//                         -0.00000000f // 0x80000000
//                  );
//
//        assertEquals(1,
//        getUlps( +0.00000000f, // 0x00000000
//                 -0.00000000f) // 0x80000000
//          );

        assertZeroUlps
            ( -1.4012985e-045f, // 0x80000001
              Float.intBitsToFloat( -2147483647 )
              );
     
        assertZeroUlps
            ( -2.8025969e-045f, // 0x80000002
              Float.intBitsToFloat( -2147483646 )
              );
     
        assertZeroUlps
            ( -4.2038954e-045f, // 0x80000003
              Float.intBitsToFloat( -2147483645 )
              ); 
    
    }

    /**
     * Verifies that the successor function imposes the correct ordering as we
     * approach and cross zero from the positive region of the value space into
     * the negative region of the value space. Note that the value set includes
     * not only the finite nonzero values, NaN values, and positive infinity,
     * and negative infinity, but also positive zero and negative zero, which
     * Java hides from you most of the time since positive zero and negative
     * zero compare as equals (for Java).
     * <p>
     * 
     * Note: The data points used in this test are verified by the previous
     * test.
     * <p>
     */
    public void test_keyBuilder_successor_float_nearZero()
        throws NoSuccessorException
    {

//  assertEquals
//      ( 4.2038954e-045f,  // 0x00000003
//        Float.intBitsToFloat( 3 )
//        );

        /*
         * successor of small positive numbers.
         */
        assertZeroUlps
        ( 4.2038954e-045f, // 0x00000003 
          KeyBuilder.successor( 2.8025969e-045f ) // 0x00000002
          );
 
        assertZeroUlps
        ( 2.8025969e-045f,  // 0x00000002
          KeyBuilder.successor( 1.4012985e-045f)  // 0x00000001
          );
 
        assertZeroUlps
        ( 1.4012985e-045f,  // 0x00000001
          KeyBuilder.successor( +0.00000000f) // 0x00000000
          );

        /*
         * successor of negative zero is positive zero.
         */
        assertTrue(-0.00000000f == Float.intBitsToFloat(0x80000000));

        assertEquals(Float.intBitsToFloat(0x00000000), KeyBuilder
                .successor(Float.intBitsToFloat(0x80000000)));
        
        assertZeroUlps
        ( +0.00000000f,  // 0x00000000
          KeyBuilder.successor( -0.00000000f ) // 0x80000000
          );

        /*
         * successor of small negative numbers.
         */
        assertZeroUlps
        ( -0.00000000f,  // 0x80000000
          KeyBuilder.successor( -1.4012985e-045f ) // 0x80000001
          );
 
        assertZeroUlps
        ( -0.00000000f,  // 0x80000000
          KeyBuilder.successor( -1.4012985e-045f ) // 0x80000001
          );
 
        assertZeroUlps
        ( -1.4012985e-045f,  // 0x80000001
          KeyBuilder.successor( -2.8025969e-045f ) // 0x80000002
          );

    }

    /**
     * Verifies that the successor of the penultimate float is
     * correct.
     */
    public void test_keyBuilder_successor_float_penultimateValue()
        throws NoSuccessorException
    {

        int maxValueAsInt = Float.floatToIntBits(Float.MAX_VALUE);

        float penultimateValue = Float.intBitsToFloat(maxValueAsInt - 1);

        assertZeroUlps(Float.MAX_VALUE, KeyBuilder.successor(penultimateValue));

    }

    /**
     * Verifies that successor of the maximum float value is positive infinity.
     */
    public void test_keyBuilder_successor_float_maxValue() 
        throws NoSuccessorException
    {

        assertZeroUlps(Float.POSITIVE_INFINITY, KeyBuilder
                .successor(Float.MAX_VALUE));

    }

    /**
     * Verifies that there is no successor for a NaN.
     */
    public void test_keyBuilder_successor_float_NaN() {

        try {
    
            KeyBuilder.successor(Float.NaN);
            
            fail("Expecting: " + NoSuccessorException.class);
            
        } catch (NoSuccessorException ex) {
            
            System.err.println("Ignoring expected exception: " + ex);
            
        }

    }

    /**
     * Verifies that there is no successor for negative infinity.
     *
     * @todo Alternatively, we could make the successor the largest
     * negative floating point value - if we knew what that was.
     */
    public void test_keyBuilder_successor_float_negativeInfinity()
    {

        try {
            
            KeyBuilder.successor(Float.NEGATIVE_INFINITY);
            
            fail("Expecting: " + NoSuccessorException.class);
            
        } catch (NoSuccessorException ex) {
            
            System.err.println("Ignoring expected exception: " + ex);
            
        }

    }

    /**
     * Verifies that there is no successor for positive infinity.
     */

    public void test_GValue_successor_Number_Float_positiveInfinity()
    {

        try {
            
            KeyBuilder.successor(Float.POSITIVE_INFINITY);
            
            fail("Expecting: " + NoSuccessorException.class);
            
        } catch (NoSuccessorException ex) {
            
            System.err.println("Ignoring expected exception: " + ex);
            
        }

    }

    public void test_keyBuilder_float_key() throws NoSuccessorException {
        
        KeyBuilder keyBuilder = new KeyBuilder();
        
        byte[] kmin = keyBuilder.reset().append(fmin).getKey(); // largest negative float.
        byte[] kn1 = keyBuilder.reset().append(fn1).getKey(); // -1f
        byte[] kneg = keyBuilder.reset().append(fneg).getKey(); // smallest negative float.
        byte[] km0 = keyBuilder.reset().append(fm0).getKey(); // -0.0f
        byte[] kp0 = keyBuilder.reset().append(fp0).getKey(); // +0.0f
        byte[] kpos = keyBuilder.reset().append(fpos).getKey(); // smallest positive float.
        byte[] kp1 = keyBuilder.reset().append(fp1).getKey(); // +1f;
        byte[] kmax = keyBuilder.reset().append(fmax).getKey(); // max pos float.

        assertEquals(4,kmin.length);
        assertEquals(4,kn1.length);
        assertEquals(4,kneg.length);
        assertEquals(4,km0.length);
        assertEquals(4,kp0.length);
        assertEquals(4,kpos.length);
        assertEquals(4,kp1.length);
        assertEquals(4,kmax.length);

        System.err.println("kmin("+fmin+")="+BytesUtil.toString(kmin));
        System.err.println("kn1("+fn1+")="+BytesUtil.toString(kn1));
        System.err.println("kneg("+fneg+")="+BytesUtil.toString(kneg));
        System.err.println("km0("+fm0+")="+BytesUtil.toString(km0));
        System.err.println("kp0("+fp0+")="+BytesUtil.toString(kp0));
        System.err.println("kpos("+fpos+")="+BytesUtil.toString(kpos));
        System.err.println("kp1("+fp1+")"+BytesUtil.toString(kp1));
        System.err.println("kmax("+fmax+")="+BytesUtil.toString(kmax));
        
        assertTrue("kmin<kn1",BytesUtil.compareBytes(kmin, kn1)<0);
        assertTrue("kn1<kneg",BytesUtil.compareBytes(kn1, kneg)<0);
        assertTrue("kneg<km0",BytesUtil.compareBytes(kneg, km0)<0);
        assertTrue("km0 == kp0",BytesUtil.compareBytes(km0, kp0) == 0);
        assertTrue("kp0<kpos",BytesUtil.compareBytes(kp0, kpos)<0);
        assertTrue("kpos<kp1",BytesUtil.compareBytes(kpos, kp1)<0);
        assertTrue("kp1<kmax",BytesUtil.compareBytes(kp1, kmax)<0);
        
    }

    public void test_keyBuilder_double_key() throws NoSuccessorException {
        
        KeyBuilder keyBuilder = new KeyBuilder();
        
        byte[] kmin = keyBuilder.reset().append(dmin).getKey(); // largest negative double.
        byte[] kn1 = keyBuilder.reset().append(dn1).getKey(); // -1f
        byte[] kneg = keyBuilder.reset().append(dneg).getKey(); // smallest negative double.
        byte[] km0 = keyBuilder.reset().append(dm0).getKey(); // -0.0f
        byte[] kp0 = keyBuilder.reset().append(dp0).getKey(); // +0.0f
        byte[] kpos = keyBuilder.reset().append(dpos).getKey(); // smallest positive double.
        byte[] kp1 = keyBuilder.reset().append(dp1).getKey(); // +1f;
        byte[] kmax = keyBuilder.reset().append(dmax).getKey(); // max pos double.

        assertEquals(8,kmin.length);
        assertEquals(8,kn1.length);
        assertEquals(8,kneg.length);
        assertEquals(8,km0.length);
        assertEquals(8,kp0.length);
        assertEquals(8,kpos.length);
        assertEquals(8,kp1.length);
        assertEquals(8,kmax.length);

        System.err.println("kmin("+dmin+")="+BytesUtil.toString(kmin));
        System.err.println("kn1("+dn1+")="+BytesUtil.toString(kn1));
        System.err.println("kneg("+dneg+")="+BytesUtil.toString(kneg));
        System.err.println("km0("+dm0+")="+BytesUtil.toString(km0));
        System.err.println("kp0("+dp0+")="+BytesUtil.toString(kp0));
        System.err.println("kpos("+dpos+")="+BytesUtil.toString(kpos));
        System.err.println("kp1("+dp1+")"+BytesUtil.toString(kp1));
        System.err.println("kmax("+dmax+")="+BytesUtil.toString(kmax));
        
        assertTrue("kmin<kn1",BytesUtil.compareBytes(kmin, kn1)<0);
        assertTrue("kn1<kneg",BytesUtil.compareBytes(kn1, kneg)<0);
        assertTrue("kneg<km0",BytesUtil.compareBytes(kneg, km0)<0);
        assertTrue("km0 == kp0",BytesUtil.compareBytes(km0, kp0) == 0);
        assertTrue("kp0<kpos",BytesUtil.compareBytes(kp0, kpos)<0);
        assertTrue("kpos<kp1",BytesUtil.compareBytes(kpos, kp1)<0);
        assertTrue("kp1<kmax",BytesUtil.compareBytes(kp1, kmax)<0);
        
    }

    public void test_keyBuilder_unicode_char_key() {
        
        fail("write test");
    }
    
    public void test_keyBuilder_unicode_chars_key() {
        
        fail("write test");
    }

    /**
     * Test ability to encode unicode data into a variable length byte[] that
     * allows direct byte-by-byte comparisons which maintain the local-specific
     * sort order of the original strings.
     */
    public void test_keyBuilder_unicode_string_key_us_primary() {

        /*
         * Get the Collator for US English and set its strength to PRIMARY.
         */
        RuleBasedCollator usCollator = (RuleBasedCollator) Collator
        .getInstance(Locale.US);
            
        usCollator.setStrength(Collator.PRIMARY);
        
        assertEquals(0,usCollator.compare("abc", "ABC"));

        KeyBuilder keyBuilder = new KeyBuilder(usCollator,1000);
            
        byte[] key1 = keyBuilder.reset().append("abc").getKey();
        byte[] key2 = keyBuilder.reset().append("ABC").getKey();
        byte[] key3 = keyBuilder.reset().append("Abc").getKey();

        System.err.println("abc: "+BytesUtil.toString(key1));
        System.err.println("ABC: "+BytesUtil.toString(key2));
        System.err.println("Abc: "+BytesUtil.toString(key3));

        // all are equal using PRIMARY strength.
        assertEquals(0,BytesUtil.compareBytes(key1, key2));
        assertEquals(0,BytesUtil.compareBytes(key2, key3));
        
    }

    public void test_keyBuilder_unicode_string_key_us_identical() {

        /*
         * Get the Collator for US English and set its strength to IDENTICAL.
         */
        RuleBasedCollator usCollator = (RuleBasedCollator) Collator
        .getInstance(Locale.US);
            
        usCollator.setStrength(Collator.IDENTICAL);
        
        assertNotSame(0,usCollator.compare("abc", "ABC"));

        KeyBuilder keyBuilder = new KeyBuilder(usCollator,1000);
            
        byte[] key1 = keyBuilder.reset().append("abc").getKey();
        byte[] key2 = keyBuilder.reset().append("ABC").getKey();
        byte[] key3 = keyBuilder.reset().append("Abc").getKey();

        System.err.println("abc: "+BytesUtil.toString(key1));
        System.err.println("ABC: "+BytesUtil.toString(key2));
        System.err.println("Abc: "+BytesUtil.toString(key3));

        // verify ordering for IDENTICAL comparison.
        assertTrue(BytesUtil.compareBytes(key1, key2)<0);
        assertTrue(BytesUtil.compareBytes(key2, key3)>0);
        
    }

    /**
     * Tests encoding of characters assumed to be US-ASCII. For each character,
     * this method simply chops of the high byte and converts the low byte to an
     * unsigned byte.
     */
    public void test_encodeASCII() throws UnsupportedEncodingException {
        
        KeyBuilder keyBuilder = new KeyBuilder();
            
        byte[] key1 = keyBuilder.reset().appendASCII("abc").getKey();
        byte[] key2 = keyBuilder.reset().appendASCII("ABC").getKey();
        byte[] key3 = keyBuilder.reset().appendASCII("Abc").getKey();

        System.err.println("abc: "+BytesUtil.toString(key1));
        System.err.println("ABC: "+BytesUtil.toString(key2));
        System.err.println("Abc: "+BytesUtil.toString(key3));
        
        // unlike a unicode encoding, this produces one byte per character.
        assertEquals(3,key1.length);
        assertEquals(3,key2.length);
        assertEquals(3,key3.length);

        /*
         * verify ordering for US-ASCII comparison.
         * 
         * Note: unlike the default unicode sort order, lowercase ASCII sorts
         * after uppercase ASCII.
         */
        assertTrue(BytesUtil.compareBytes(key1, key2)>0);
        assertTrue(BytesUtil.compareBytes(key2, key3)<0);
        
//        /* verify that we can convert back to Unicode.
//         * 
//         * Note: to do this we have to convert from unsigned to signed bytes
//         * before invoking the string constructor.
//         */
//        assertEquals("abc",new String(key1,"UTF-8"));
//        assertEquals("ABC",new String(key1,"UTF-8"));
//        assertEquals("Abc",new String(key1,"UTF-8"));
        
    }

    /**
     * Tests for keys formed from the application key, a column name, and a long
     * timestamp. A zero(0) byte is used as a delimiter between components of
     * the key.
     * 
     * @todo this is not testing much yet.
     */
    public void test_cstore_keys() {
        
        KeyBuilder keyBuilder = new KeyBuilder();
        
        final byte[] colname1 = keyBuilder.reset().append("column1").getKey();
        
        final byte[] colname2 = keyBuilder.reset().append("another column").getKey();
        
        final long timestamp = System.currentTimeMillis();
        
        byte[] k1 = keyBuilder.reset().append(12L).appendNul().append(colname1)
        .appendNul().append(timestamp).getKey();

        byte[] k2 = keyBuilder.reset().append(12L).appendNul().append(colname2)
        .appendNul().append(timestamp).getKey();

        System.err.println("k1="+BytesUtil.toString(k1));
        System.err.println("k2="+BytesUtil.toString(k2));

        fail("this does not test anything yet");
    }
    
    /*
     * verify that the ordering of floating point values when converted to
     * unsigned byte[]s is maintained.
     */
    
    /**
     * Verify that we can convert float keys to unsigned byte[]s while
     * preserving the value space order.
     */
    public void test_float_order() {

        Random r = new Random();

        // #of distinct keys to generate.
        final int limit = 100000;
        
        /*
         * Generate a set of distinct float keys distributed across the value
         * space. For each generated key, obtain the representation of that
         * float as a unsigned byte[].
         */
        class X {
            final float val;
            final byte[] key;
            public X(float val,byte[] key) {
                this.val = val;
                this.key = key;
            }
        };
        /**
         * imposes ordering based on {@link X#val}.
         */
        class XComp implements Comparator<X> {
            public int compare(X o1, X o2) {
                float ret = o1.val - o2.val;
                if( ret <0 ) return -1;
                if( ret > 0 ) return 1;
                return 0;
            }
        };
//        final float[] vals = new float[limit];
//        final byte[][] keys = new byte[limit][];
        Set<Float> set = new HashSet<Float>(limit);
        final X[] data = new X[limit];
        KeyBuilder keyBuilder = new KeyBuilder();
        {

            int nkeys = 0;
            
            while (set.size() < limit) {

                float val = r.nextFloat();

                if (set.add(val)) {

                    // this is a new point in the value space.

                    byte[] key = keyBuilder.reset().append(val).getKey();

                    data[nkeys] = new X(val,key);

                    nkeys++;
                    
                }
                
            }
            
        }

        /*
         * sort the tuples.
         */
        Arrays.sort(data,new XComp());
        
        /*
         * Insert the int keys paired to their float values into an ordered map.
         * We insert the data in random order (paranoia), but that should not
         * matter.
         */

        System.err.println("Populating map");
        
        TreeMap<byte[],Float> map = new TreeMap<byte[],Float>(BytesUtil.UnsignedByteArrayComparator.INSTANCE);

        int[] order = getRandomOrder(limit);
        
        for( int i=0; i<limit; i++) {

            float val = data[order[i]].val;
            
            byte[] key = data[order[i]].key;
            
            if( key == null ) {
            
                fail("key is null at index="+i+", val="+val);
                
            }
            
            Float oldval = map.put(key,val);
            
            if( oldval != null ) {

                fail("Key already exists: " + BytesUtil.toString(key)
                        + " with value=" + oldval);
                
            }
            
        }
        
        assertEquals(limit,map.size());
        
        /*
         * traverse the map in key order and verify that the total ordering
         * maintained by the keys is correct for the values.
         */
        
        System.err.println("Testing map order");
        
        Iterator<Map.Entry<byte[],Float>> itr = map.entrySet().iterator();
        
        int i = 0;
        
        while(itr.hasNext() ) {
        
            Map.Entry<byte[], Float> entry = itr.next();
            
            byte[] key = entry.getKey();
            
            assert key != null;
            
            float val = entry.getValue();
            
            if (BytesUtil.compareBytes(data[i].key, key) != 0) {
                fail("keys[" + i + "]: expected=" + BytesUtil.toString(data[i].key)
                        + ", actual=" + BytesUtil.toString(key));
            }
            
            if(data[i].val != val) {
                assertEquals("vals["+i+"]", data[i].val, val);
            }
            
            i++;
            
        }
        
    }
    
    /**
     * Verify that we can convert double keys to unsigned byte[]s while
     * preserving the value space order.
     */
    public void test_double_order() {

        Random r = new Random();

        // #of distinct keys to generate.
        final int limit = 100000;
        
        /*
         * Generate a set of distinct double keys distributed across the value
         * space. For each generated key, obtain the representation of that
         * double as a unsigned byte[].
         */
        class X {
            final double val;
            final byte[] key;
            public X(double val,byte[] key) {
                this.val = val;
                this.key = key;
            }
        };
        /**
         * imposes ordering based on {@link X#val}.
         */
        class XComp implements Comparator<X> {
            public int compare(X o1, X o2) {
                double ret = o1.val - o2.val;
                if( ret <0 ) return -1;
                if( ret > 0 ) return 1;
                return 0;
            }
        };
        Set<Double> set = new HashSet<Double>(limit);
        final X[] data = new X[limit];
        KeyBuilder keyBuilder = new KeyBuilder();
        {

            int nkeys = 0;
            
            while (set.size() < limit) {

                double val = r.nextDouble();

                if (set.add(val)) {

                    // this is a new point in the value space.

                    byte[] key = keyBuilder.reset().append(val).getKey();

                    data[nkeys] = new X(val,key);

                    nkeys++;
                    
                }
                
            }
            
        }

        /*
         * sort the tuples.
         */
        Arrays.sort(data,new XComp());
        
        /*
         * Insert the int keys paired to their double values into an ordered map.
         * We insert the data in random order (paranoia), but that should not
         * matter.
         */

        System.err.println("Populating map");
        
        TreeMap<byte[],Double> map = new TreeMap<byte[],Double>(BytesUtil.UnsignedByteArrayComparator.INSTANCE);

        int[] order = getRandomOrder(limit);
        
        for( int i=0; i<limit; i++) {

            double val = data[order[i]].val;
            
            byte[] key = data[order[i]].key;
            
            if( key == null ) {
            
                fail("key is null at index="+i+", val="+val);
                
            }
            
            Double oldval = map.put(key,val);
            
            if( oldval != null ) {

                fail("Key already exists: " + BytesUtil.toString(key)
                        + " with value=" + oldval);
                
            }
            
        }
        
        assertEquals(limit,map.size());
        
        /*
         * traverse the map in key order and verify that the total ordering
         * maintained by the keys is correct for the values.
         */
        
        System.err.println("Testing map order");
        
        Iterator<Map.Entry<byte[],Double>> itr = map.entrySet().iterator();
        
        int i = 0;
        
        while(itr.hasNext() ) {
        
            Map.Entry<byte[], Double> entry = itr.next();
            
            byte[] key = entry.getKey();
            
            assert key != null;
            
            double val = entry.getValue();
            
            if (BytesUtil.compareBytes(data[i].key, key) != 0) {
                fail("keys[" + i + "]: expected=" + BytesUtil.toString(data[i].key)
                        + ", actual=" + BytesUtil.toString(key));
            }
            
            if(data[i].val != val) {
                assertEquals("vals["+i+"]", data[i].val, val);
            }
            
            i++;
            
        }
        
    }

    /**
     * Encodes a collection of strings, returning sort keys for those
     * strings. Each sort key has the property that a bit-wise (or
     * byte-wise) comparison of sort keys produced by the same collator will
     * have the same ordering as the strings (according to the rules of the
     * collator). Keys produced by different collators must not be mixed.
     * The returned keys can be sorted using {@link Arrays#sort(byte[])}.
     * 
     * @todo the problem with the approach is that we want to sort the keys
     * before presenting them to the btree.  However, the application needs
     * to know which key is associated with which input string.  If we use
     * {@link CollatorKey} then that relationship is preserved.  If we use
     * {@link RawCollatorKey} then the code is more efficient, but we loose
     * the relationship and it is hard to sort the data ....
     * 
     * create internally a tuple {string, key, id}, where id is a one up
     * integer for the batch.  sort those tuples by key and id will provide
     * the permutation of the original strings that will present them in 
     * sorted order to the btree.  the btree responses will come out in the
     * same tuple ordering.
     * 
     * @param collator
     *            The collator.
     * @param strings
     *            The strings (in).
     *            
     * @return The sort keys.
     */
    public static Tuple[] encodeString(RuleBasedCollator collator,String[] strings) {
        
        Tuple[] tuples = new Tuple[strings.length];

        KeyBuilder keyBuilder = new KeyBuilder(collator,Bytes.kilobyte32);
        
        for(int i=0; i<strings.length; i++) {
            
            String s = strings[i];
            
            keyBuilder.reset();
            
            keyBuilder.append(s);
            
            // copy out the sort key from the buffer.
            byte[] sortKey = keyBuilder.getKey();

            // form tuple.
            tuples[i] = new Tuple(i,s,sortKey);
            
        }
        
        return tuples;
        
    }

    public static void batchInsert(BTree btree,Tuple[] tuples,boolean sorted) {

        final int ntuples = tuples.length;

        /*
         * Someone must sort the tuples before presenting to the btree. In
         * general, the client has more information, especially whether or
         * not the data are already sorted. As an alternative, we could set
         * a flag in the batch api or the network api by which the caller
         * declares sorted data and sort the data when it is not already
         * sorted.
         */
        if (!sorted) {
            { // shallow copy
                Tuple[] tuples2 = new Tuple[ntuples];
                for (int i = 0; i < ntuples; i++) {
                    tuples2[i] = tuples[i];
                }
                tuples = tuples2;
            }
            // sort the copy.
            Arrays.sort(tuples, TupleComparator.INSTANCE);
        }

        byte[][] keys = new byte[ntuples][];
        { // shallow copy of sort keys.
            for(int i=0; i<ntuples; i++) {
                keys[i] = tuples[i].sortKey;
            }
        }
        
        Object[] values = new Object[ntuples];
        { // shallow copy of values paired to sort keys.
            for(int i=0; i<ntuples; i++) {
                values[i] = tuples[i].value;
            }
        }
        
        /*
         * batch insert operation - no outputs.
         * 
         * @todo if this was a batch lookup or batch remove operation then
         * the values:Object[] would be the output and we would assign them
         * to the corresponding tuple in a loop so as to pair the output
         * values with the input data.
         * 
         * @todo do I need to allow a [null] value on insert if I refactor
         * the timestamp into a wrapper object put into place by the btree
         * class? frankly, it is all the same thing... one approach has more
         * object creation (the wrapper objects) while the other has more
         * data copying (the timestamps in addition to the values). using
         * wrapper objects is more general if I want to minimize the inpact
         * of the isolation mechanism on the code.
         */
        btree.insert(ntuples, keys, values);
        
    }
    
    public static class Tuple implements Comparable<Tuple> {
        public final int id;
        public final String string;
        public final byte[] sortKey;
        public Object value;
        public Tuple(int id,String string, byte[] sortKey) {
            this.id = id;
            this.string = string;
            this.sortKey = sortKey;
        }
        public int compareTo(Tuple o) {
            return BytesUtil.compareBytes(sortKey,o.sortKey);
        }
    }

    public static class TupleComparator implements Comparator<Tuple> {
        
        public static transient final Comparator<Tuple> INSTANCE = new TupleComparator();
        
        public int compare(Tuple o1, Tuple o2) {
            return BytesUtil.compareBytes(o1.sortKey,o2.sortKey);
        }
    }

}
