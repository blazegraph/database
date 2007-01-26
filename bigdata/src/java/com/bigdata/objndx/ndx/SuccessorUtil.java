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
 * Created on Jun 7, 2006
 */

package com.bigdata.objndx.ndx;

/**
 * Utility methods for calculating the sucessor for various data types.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated the routines are being moved to {@link KeyBuilder}.
 */

public class SuccessorUtil {

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

                // The successor of negative infinity.

                return Float.MIN_VALUE;

            }

        }

        int bits = Float.floatToIntBits(f);

        if (f >= 0.0) {

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

        if (d >= 0.0) {

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
     * @param s The string reference or <code>null</code>.
     * 
     * @return The successor and never <code>null</code>
     */

    public static String successor(String s) {

        if (s == null)
            return "\0";

        return s + "\0";

    }
    
}
