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

package com.bigdata.btree;


/**
 * Utility methods for computing the successor of a value for various data
 * types.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BytesUtil#successor(byte[]), which computes the successor of a variable
 *      length unsigned byte[].
 */
public class SuccessorUtil {

    /*
     * useful single precision values.
     */
    
    /**
     * Positive zero (+0f).
     * <p>
     * Note: +0f and -0f will compare as _equal_ in the language. This means
     * that you can not write tests that directly distinguish positive and
     * negative zero using >, <, or ==.
     */
    final public static float FPOS_ZERO = Float.intBitsToFloat(0x00000000);

    /**
     * Negative zero (-0f).
     * <p>
     * Note: +0f and -0f will compare as _equal_ in the language. This means
     * that you can not write tests that directly distinguish positive and
     * negative zero using >, <, or ==.
     */
    final public static float FNEG_ZERO = Float.intBitsToFloat(0x80000000);
    
    /**
     * Smallest non-zero positive value.
     */
    final public static float FPOS_MIN = Float.intBitsToFloat(0x00000001);
    
    /**
     * Smallest non-zero negative value.
     */
    final public static float FNEG_MIN = Float.intBitsToFloat(0x80000001);
    
    /**
     * Positive one (1f).
     */
    final public static float FPOS_ONE = 1f;

    /**
     * Negative one (-1f).
     */
    final public static float FNEG_ONE = -1f;

    /**
     * Largest positive float.
     */
    final public static float FPOS_MAX = Float.MAX_VALUE;

    /**
     * Largest negative float.
     */
    final public static float FNEG_MAX = Float.intBitsToFloat(0xFF7FFFFF);
    
    /*
     * useful double precision values.
     */
    /**
     * Positive zero (+0d).
     * <p>
     * Note: +0d and -0d will compare as _equal_ in the language. This means
     * that you can not write tests that directly distinguish positive and
     * negative zero using >, <, or ==.
     */
    final public static double DPOS_ZERO = Double.longBitsToDouble(0x0000000000000000L);

    /**
     * Negative zero (-0d).
     * <p>
     * Note: +0d and -0d will compare as _equal_ in the language. This means
     * that you can not write tests that directly distinguish positive and
     * negative zero using >, <, or ==.
     */
    final public static double DNEG_ZERO = Double.longBitsToDouble(0x8000000000000000L);
    
    /**
     * Smallest non-zero positive value.
     */
    final public static double DPOS_MIN = Double.longBitsToDouble(0x0000000000000001L);
    
    /**
     * Smallest non-zero negative value.
     */
    final public static double DNEG_MIN = Double.longBitsToDouble(0x8000000000000001L);
    
    /**
     * Positive one (1d).
     */
    final public static double DPOS_ONE = 1d;

    /**
     * Negative one (-1d).
     */
    final public static double DNEG_ONE = -1d;
    
    /**
     * Largest positive double.
     */
    final public static double DPOS_MAX = Double.MAX_VALUE;

    /**
     * Largest negative double.
     */
    final public static double DNEG_MAX = Double.longBitsToDouble(0xFFEFFFFFFFFFFFFFL);
    
    /*
     * successor methods.
     */
    
    /**
     * Computes the successor of a <code>byte</code> value.
     * 
     * @param n
     *            A value
     *            
     * @return The successor of that value.
     * 
     * @exception NoSuccessorException
     *                if there is no successor for that value.
     */
    public static byte successor( byte n ) throws NoSuccessorException {

        if (Byte.MAX_VALUE == n) {

            throw new NoSuccessorException();

        } else {

            return (byte) (n + 1);

        }

    }
    
    /**
     * Computes the successor of a <code>char</code> value.
     * 
     * @param n
     *            A value
     *            
     * @return The successor of that value.
     * 
     * @exception NoSuccessorException
     *                if there is no successor for that value.
     */
    public static char successor( char n ) throws NoSuccessorException
    {
        
        if (Character.MAX_VALUE == n) {

            throw new NoSuccessorException();

        } else {

            return (char) (n + 1);

        }
        
    }
    
    /**
     * Computes the successor of a <code>short</code> value.
     * 
     * @param n
     *            A value
     *            
     * @return The successor of that value.
     * 
     * @exception NoSuccessorException
     *                if there is no successor for that value.
     */
    public static short successor( short n ) throws NoSuccessorException
    {
        
        if (Short.MAX_VALUE == n) {

            throw new NoSuccessorException();

        } else {

            return (short) (n + 1);

        }
        
    }
    
    /**
     * Computes the successor of an <code>int</code> value.
     * 
     * @param n
     *            A value
     *            
     * @return The successor of that value.
     * 
     * @exception NoSuccessorException
     *                if there is no successor for that value.
     */
    public static int successor( int n ) throws NoSuccessorException
    {
    
        if (Integer.MAX_VALUE == n) {

            throw new NoSuccessorException();

        } else {

            return n + 1;

        }

    }

    /**
     * Computes the successor of a <code>long</code> value.
     * 
     * @param n
     *            A value
     *            
     * @return The successor of that value.
     * 
     * @exception NoSuccessorException
     *                if there is no successor for that value.
     */
    public static long successor( long n ) throws NoSuccessorException
    {

        if (Long.MAX_VALUE == n) {

            throw new NoSuccessorException();

        } else {

            return n + 1L;

        }
   
    }
    
    /**
     * <p>
     * Computes the successor of a <code>float</code> value.
     * </p>
     * <p>
     * The IEEE floating point standard provides a means for computing the next
     * larger or smaller floating point value using a bit manipulation trick.
     * See <a
     * href="http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
     * Comparing floating point numbers </a> by Bruce Dawson. The Java
     * {@link Float} and {@link Double} clases provide the static methods
     * required to convert a float or double into its IEEE 754 floating point
     * bit layout, which can be treated as an int (for floats) or a long (for
     * doubles). By testing for the sign, you can just add (or subtract) one (1)
     * to get the bit pattern of the successor (see the above referenced
     * article). Special exceptions must be made for NaNs, negative infinity and
     * positive infinity.
     * </p>
     * 
     * @param f
     *            The float value.
     * 
     * @return The next value in the value space for <code>float</code>.
     * 
     * @exception NoSuccessorException
     *                if there is no next value in the value space.
     */
    static public float successor( float f )
        throws NoSuccessorException
    {
        
        if (f == Float.MAX_VALUE) {

            return Float.POSITIVE_INFINITY;

        }

        if (Float.isNaN(f)) {

            throw new NoSuccessorException("NaN");

        }

        if (Float.isInfinite(f)) {

            if (f > 0) {

                throw new NoSuccessorException("Positive Infinity");

            } else {

                /* no successor for negative infinity (could be the largest
                 * negative value).
                 */

                throw new NoSuccessorException("Negative Infinity");

            }

        }

        int bits = Float.floatToIntBits(f);

        if (bits == 0x80000000) {
            
            /*
             * the successor of -0.0f is +0.0f
             * 
             * @todo Java defines the successor of floating point zeros as the
             * first non-zero value so maybe we should change this.
             */
            return +0.0f;
            
        }

        if (f >= +0.0f) {

            bits += 1;

        } else {

            bits -= 1;

        }

        float nxt = Float.intBitsToFloat(bits);
        
        return nxt;
        
    }
    
    /**
     * <p>
     * Computes the successor of a <code>double</code> value.
     * </p>
     * <p>
     * The IEEE floating point standard provides a means for computing the next
     * larger or smaller floating point value using a bit manipulation trick.
     * See <a
     * href="http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
     * Comparing floating point numbers </a> by Bruce Dawson. The Java
     * {@link Float} and {@link Double} clases provide the static methods
     * required to convert a float or double into its IEEE 754 floating point
     * bit layout, which can be treated as an int (for floats) or a long (for
     * doubles). By testing for the sign, you can just add (or subtract) one (1)
     * to get the bit pattern of the successor (see the above referenced
     * article). Special exceptions must be made for NaNs, negative infinity and
     * positive infinity.
     * </p>
     * 
     * @param d The double value.
     * 
     * @return The next value in the value space for <code>double</code>.
     * 
     * @exception NoSuccessorException
     *                if there is no next value in the value space.
     */
    public static double successor( double d ) 
        throws NoSuccessorException
    {
        
        if (d == Double.MAX_VALUE) {

            return Double.POSITIVE_INFINITY;

        }

        if (Double.isNaN(d)) {

            throw new NoSuccessorException("Nan");

        }

        if (Double.isInfinite(d)) {

            if (d > 0) {

                throw new NoSuccessorException("Positive Infinity");

            } else {

                // The successor of negative infinity.

                return Double.MIN_VALUE;

            }

        }

        long bits = Double.doubleToLongBits(d);

        if (bits == 0x8000000000000000L) {
            
            /* the successor of -0.0d is +0.0d
             * 
             * @todo Java defines the successor of floating point zeros as the
             * first non-zero value so maybe we should change this.
             */
            return +0.0d;
            
        }

//        if (f >= +0.0f) {

        if (d >= +0.0) {

            bits += 1;

        } else {

            bits -= 1;

        }

        double nxt = Double.longBitsToDouble(bits);

        return nxt;

    }
    
    /**
     * The successor of a string value is formed by appending a <code>nul</code>.
     * The successor of a <code>null</code> string reference is an empty
     * string. The successor of a string value is defined unless the string is
     * too long.
     * 
     * @param s
     *            The string reference or <code>null</code>.
     * 
     * @return The successor and never <code>null</code>
     */
    public static String successor(String s) {

        if (s == null)
            return "\0";

        return s + "\0";

    }

}
