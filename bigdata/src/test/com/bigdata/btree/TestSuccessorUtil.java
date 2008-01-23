/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Feb 3, 2007
 */

package com.bigdata.btree;

import java.util.Arrays;

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
//    final protected double dm1 = Double.longBitsToDouble(UnicodeKeyBuilder.d2l(0d)-1);
//
//    /**
//     * The double precision number that is one more than zero in the double
//     * precision value space.
//     */
//    final protected double DPOS_ONE = Double.longBitsToDouble(UnicodeKeyBuilder.d2l(0d)-1);
    
    
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
    public void test_keyBuilder_successor_float_positiveInfinity()
    {

        try {
            
            SuccessorUtil.successor(Float.POSITIVE_INFINITY);
            
            fail("Expecting: " + NoSuccessorException.class);
            
        } catch (NoSuccessorException ex) {
            
            System.err.println("Ignoring expected exception: " + ex);
            
        }

    }

    /**
     * Test computation of the successor of a fixed length bit string.
     */
    public void test_bitString_successor() {
        
        // simplest case
        {
            
            // original data.
            byte[] b = new byte[]{0x00};
            
            // successor.
            byte[] s = SuccessorUtil.successor(b.clone());
            
            // expected successor.
            byte[] e = new byte[]{0x01};
            
            assertEquals(e,s);
            
        }

        // successor of the smallest value.
        {
            
            // original data.
            byte[] b = new byte[]{Byte.MIN_VALUE};
            
            // successor.
            byte[] s = SuccessorUtil.successor(b.clone());
            
            // expected successor.
            byte[] e = new byte[]{Byte.MIN_VALUE+1};
            
            assertEquals(e,s);
            
        }

        // handle overflow in the 1st byte.
        {

            // original data.
//            byte[] b = new byte[]{0x00,Byte.MAX_VALUE};
            byte[] b = new byte[]{0x00,-1};
            
            // successor.
            byte[] s = SuccessorUtil.successor(b.clone());
            
            System.err.println("b: "+Arrays.toString(b));
            System.err.println("s: "+Arrays.toString(s));
            
            // expected successor.
//            byte[] e = new byte[]{0x01,Byte.MIN_VALUE};
            byte[] e = new byte[]{0x01,0x00};
            
            assertEquals(e,s);
            
        }

        // no successor - byte[0].
        {

            // original data.
            byte[] b = new byte[]{};
            
            // successor.
            try {
                SuccessorUtil.successor(b.clone());
                fail("Expecting: " + NoSuccessorException.class);
            } catch (NoSuccessorException ex) {
                System.err.println("Ignoring expected exception: "+ex);
            }

        }

        // no successor - byte[1].
        {

            // original data.
//            byte[] b = new byte[]{Byte.MAX_VALUE};
            byte[] b = new byte[]{-1};
            
            // successor.
            try {
                SuccessorUtil.successor(b.clone());
                fail("Expecting: " + NoSuccessorException.class);
            } catch (NoSuccessorException ex) {
                System.err.println("Ignoring expected exception: "+ex);
            }
            
        }
        
        // compare behavior with (int+1)
        {
        
//            Random r = new Random();
            
//            int LIMIT = 100000;
//            int LIMIT = 1;
            
//            System.err.println("-1: " + Integer.toBinaryString(-1));
//            System.err.println("MAX_BYTE: " + Integer.toBinaryString(Byte.MAX_VALUE));
//            System.err.println("MIN_BYTE: " + Integer.toBinaryString(Byte.MIN_VALUE));
            
            for(int i=Short.MIN_VALUE; i<=Short.MAX_VALUE; i++) {

//                int v = r.nextInt();
//                int v = 1197420429;
//                int v = -1950493185;

                short v = (short)i;
                
//                short v = -1;
                
                // original data.
                byte[] b = KeyBuilder.asSortKey(v);

                if(true){
                boolean allones = true;
                for(int k=0;k<b.length;k++) {
                    if(b[k]!=-1) allones = false;
                }
                if(allones) {
                    System.err.println("short "+v+" is all ones.");
                }}
                if (v == Short.MAX_VALUE) {
                    // no successor.
                    try {
//                        b = KeyBuilder.asSortKey(v); // @todo remove.
                        SuccessorUtil.successor(b.clone());
                        fail("Expecting: " + NoSuccessorException.class);
                    } catch (NoSuccessorException ex) {
                        System.err
                                .println("Ignoring expected exception: " + ex);
                    }
                } else {
                    // expected successor.
                    byte[] e = KeyBuilder.asSortKey((short) (v + 1));

                    // successor.
                    byte[] s = SuccessorUtil.successor(b.clone());

                    if (false) {
                        System.err.println("v = " + v);
                        System.err.println("b: " + Arrays.toString(b));
                        System.err.println("s: " + Arrays.toString(s));
                        System.err.println("e: " + Arrays.toString(e));
                    }

                    assertEquals(e, s);
                }
            }
            
        }
        
    }
    
}
