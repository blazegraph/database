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
 * Created on Feb 3, 2007
 */

package com.bigdata.objndx;

import junit.framework.TestCase2;

/**
 * Test suite for {@link SuccessorUtil}.
 * 
 * @todo finish the double precision tests per the single precision tests, e.g.,
 *       for successor(double), etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSuccessorUtil extends TestCase2 {

    /**
     * 
     */
    public TestSuccessorUtil() {
    }

    /**
     * @param name
     */
    public TestSuccessorUtil(String name) {
        super(name);
    }

    public void test_float_data_points() {

        // positive value space.
        assertTrue(SuccessorUtil.FPOS_ZERO<SuccessorUtil.FPOS_MAX);
        assertTrue(SuccessorUtil.FPOS_ZERO<SuccessorUtil.FPOS_MIN);
        assertTrue(SuccessorUtil.FPOS_MIN<SuccessorUtil.FPOS_ONE);
        
        // negative value space.
        assertTrue(SuccessorUtil.FNEG_MIN>SuccessorUtil.FNEG_MAX);
        assertTrue(SuccessorUtil.FNEG_ZERO>SuccessorUtil.FNEG_MIN);
        assertTrue(SuccessorUtil.FNEG_MIN>SuccessorUtil.FNEG_ONE);

        /*
         * Note: +0f and -0f will compare as _equal_ in the language. This means
         * that you can not write tests that directly distinguish positive and
         * negative zero using >, <, or ==.
         */
        assertTrue(SuccessorUtil.FNEG_ZERO == SuccessorUtil.FPOS_ZERO);
        
    }
    
    public void test_double_data_points() {

        // positive value space.
        assertTrue(SuccessorUtil.DPOS_ZERO<SuccessorUtil.DPOS_MAX);
        assertTrue(SuccessorUtil.DPOS_ZERO<SuccessorUtil.DPOS_MIN);
        assertTrue(SuccessorUtil.DPOS_MIN<SuccessorUtil.DPOS_ONE);
        
        // negative value space.
        assertTrue(SuccessorUtil.DNEG_MIN>SuccessorUtil.DNEG_MAX);
        assertTrue(SuccessorUtil.DNEG_ZERO>SuccessorUtil.DNEG_MIN);
        assertTrue(SuccessorUtil.DNEG_MIN>SuccessorUtil.DNEG_ONE);

        /*
         * Note: +0f and -0f will compare as _equal_ in the language. This means
         * that you can not write tests that directly distinguish positive and
         * negative zero using >, <, or ==.
         */
        assertTrue(SuccessorUtil.DNEG_ZERO == SuccessorUtil.DPOS_ZERO);
        
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
//    final protected double DPOS_ONE = Double.longBitsToDouble(KeyBuilder.d2l(0d)-1);
    
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

        assertZeroUlps(1.99999988f, SuccessorUtil.successor(1.99999976f));

        assertZeroUlps(2.00000000f, SuccessorUtil.successor(1.99999988f));

        assertZeroUlps(2.00000024f, SuccessorUtil.successor(2.00000000f));

        assertZeroUlps(2.00000048f, SuccessorUtil.successor(2.00000024f));

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
        
        assertZeroUlps(neg2, SuccessorUtil.successor(neg1));

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
          SuccessorUtil.successor( 2.8025969e-045f ) // 0x00000002
          );
 
        assertZeroUlps
        ( 2.8025969e-045f,  // 0x00000002
          SuccessorUtil.successor( 1.4012985e-045f)  // 0x00000001
          );
 
        assertZeroUlps
        ( 1.4012985e-045f,  // 0x00000001
          SuccessorUtil.successor( +0.00000000f) // 0x00000000
          );

        /*
         * successor of negative zero is positive zero.
         */
        assertTrue(-0.00000000f == Float.intBitsToFloat(0x80000000));

        assertEquals(Float.intBitsToFloat(0x00000000), SuccessorUtil
                .successor(Float.intBitsToFloat(0x80000000)));
        
        assertZeroUlps
        ( +0.00000000f,  // 0x00000000
          SuccessorUtil.successor( -0.00000000f ) // 0x80000000
          );

        /*
         * successor of small negative numbers.
         */
        assertZeroUlps
        ( -0.00000000f,  // 0x80000000
          SuccessorUtil.successor( -1.4012985e-045f ) // 0x80000001
          );
 
        assertZeroUlps
        ( -0.00000000f,  // 0x80000000
          SuccessorUtil.successor( -1.4012985e-045f ) // 0x80000001
          );
 
        assertZeroUlps
        ( -1.4012985e-045f,  // 0x80000001
          SuccessorUtil.successor( -2.8025969e-045f ) // 0x80000002
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

        assertZeroUlps(Float.MAX_VALUE, SuccessorUtil.successor(penultimateValue));

    }

    /**
     * Verifies that successor of the maximum float value is positive infinity.
     */
    public void test_keyBuilder_successor_float_maxValue() 
        throws NoSuccessorException
    {

        assertZeroUlps(Float.POSITIVE_INFINITY, SuccessorUtil
                .successor(Float.MAX_VALUE));

    }

    /**
     * Verifies that there is no successor for a NaN.
     */
    public void test_keyBuilder_successor_float_NaN() {

        try {
    
            SuccessorUtil.successor(Float.NaN);
            
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
            
            SuccessorUtil.successor(Float.NEGATIVE_INFINITY);
            
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
            
            SuccessorUtil.successor(Float.POSITIVE_INFINITY);
            
            fail("Expecting: " + NoSuccessorException.class);
            
        } catch (NoSuccessorException ex) {
            
            System.err.println("Ignoring expected exception: " + ex);
            
        }

    }

}
