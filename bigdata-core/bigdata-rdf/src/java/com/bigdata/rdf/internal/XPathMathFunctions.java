/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Sep 7, 2011
 */

package com.bigdata.rdf.internal;


/**
 * Support for the picky xpath math functions: abs, ceiling, floor, and round. 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Implement correct version of xpath math functions with test suite.
 */
public class XPathMathFunctions {

    public static final IV abs(final IV iv) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the smallest (closest to negative infinity) number with no
     * fractional part that is not less than the value of $arg. If type of $arg
     * is one of the four numeric types xs:float, xs:double, xs:decimal or
     * xs:integer the type of the result is the same as the type of $arg. If the
     * type of $arg is a type derived from one of the numeric types, the result
     * is an instance of the base numeric type.
     * 
     * For xs:float and xs:double arguments, if the argument is positive zero,
     * then positive zero is returned. If the argument is negative zero, then
     * negative zero is returned. If the argument is less than zero and greater
     * than -1, negative zero is returned.
     * 
     * @see http://www.w3.org/TR/xpath-functions/#func-ceiling
     */
    public static final IV ceiling(final IV iv) {

        // switch (iv.getDTE()) {
        // case XSDFloat:
        // case XSDDouble:
        // case XSDInteger:
        // case XSDDecimal:
        // }
        // return new XSDDecimalIV(left.round(new MathContext(0/* precision */,
        // RoundingMode.CEILING)));

        throw new UnsupportedOperationException();
    }

    public static final IV floor(final IV iv) {
        throw new UnsupportedOperationException();
    }

    public static final IV round(final IV iv) {
        throw new UnsupportedOperationException();
    }

}
