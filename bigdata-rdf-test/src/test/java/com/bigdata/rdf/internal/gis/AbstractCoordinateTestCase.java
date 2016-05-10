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
 * Created on Aug 3, 2007
 */
package com.bigdata.rdf.internal.gis;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractCoordinateTestCase extends TestCase2 {
    /**
     * 
     */
    public AbstractCoordinateTestCase() {
        super();
    }

    /**
     * @param arg0
     */
    public AbstractCoordinateTestCase(String arg0) {
        super(arg0);
    }

    public void assertEquals(ICoordinate expected, ICoordinate actual) {
        assertEquals(null, expected, actual);
    }

    /** @todo msg is ignored. */
    public void assertEquals(String msg, ICoordinate expected,
            ICoordinate actual) {
        if (expected instanceof CoordinateDD) {
            assertEquals(((CoordinateDD) expected), actual.toDD());
        } else if (expected instanceof CoordinateDD) {
            assertEquals(((CoordinateDMS) expected), actual.toDMS());
        } else if (expected instanceof CoordinateDDM) {
            assertEquals(((CoordinateDDM) expected), actual.toDDM());
        } else {
            throw new AssertionError();
        }
    }

    /**
     * Asserts that two {@link CoordinateDD}s are equal.
     * 
     * @param expected
     * @param actual
     */
    public void assertEquals(CoordinateDD expected, CoordinateDD actual) {
        if (expected.equals(actual))
            return;
        throw new AssertionFailedError("Expected: " + expected + ", but found "
                + actual);
    }

    /**
     * Asserts that two {@link CoordinateDMS}s are equal.
     * 
     * @param expected
     * @param actual
     */
    public void assertEquals(CoordinateDMS expected, CoordinateDMS actual) {
        if (expected.equals(actual))
            return;
        throw new AssertionFailedError("Expected: " + expected + ", but found "
                + actual);
    }

    /**
     * Asserts that two {@link CoordinateDDM}s are equal.
     * 
     * @param expected
     * @param actual
     */
    public void assertEquals(CoordinateDDM expected, CoordinateDDM actual) {
        if (expected.equals(actual))
            return;
        throw new AssertionFailedError("Expected: " + expected + ", but found "
                + actual);
    }

    /**
     * Round off a double precision number to a given number of digits after the
     * decimal.
     * 
     * @param d
     *            The value.
     * 
     * @param digitsAfterTheDecimal
     * 
     * @return The value rounded off at the indicated number of digits after the
     *         decimal.
     */
    protected double round(double d, int digitsAfterTheDecimal) {
        assert digitsAfterTheDecimal >= 0; // real limit
        assert digitsAfterTheDecimal < 20; // artifical limit.
        double m = Math.pow(10, digitsAfterTheDecimal);
        return Math.round(d * m) / m;
    }

    /**
     * Round off a double precision number to 1 digit after the decimal.
     * 
     * @param d
     *            The value.
     * 
     * @return The value rounded off at the 1st digit after the decimal.
     */
    protected double round1(double d) {
        return Math.round(d * 10d) / 10d;
    }

    /**
     * Round off a double precision number to 2 digits after the decimal.
     * 
     * @param d
     *            The value.
     * 
     * @return The value rounded off at the 2nd digit after the decimal.
     */
    protected double round2(double d) {
        return Math.round(d * 100d) / 100d;
    }

    /**
     * Round off a double precision number to 5 digits after the decimal.
     * 
     * @param d
     *            The value.
     * 
     * @return The value rounded off at the 5th digit after the decimal.
     */
    protected double round5(double d) {
        return Math.round(d * 100000d) / 100000d;
    }
}
