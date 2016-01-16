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
 * Created on Aug 2, 2007
 */
package com.bigdata.rdf.internal.gis;

import java.text.ParseException;

/**
 * Test suite for {@link CoordinateDD}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCoordinateDD extends AbstractCoordinateTestCase {
    /**
     * 
     */
    public TestCoordinateDD() {
        super();
    }

    /**
     * @param arg0
     */
    public TestCoordinateDD(String arg0) {
        super(arg0);
    }

    /**
     * Verify basic constructor and formatter.
     */
    public void test_ctor() {
        CoordinateDD c = new CoordinateDD(+32.30642, -122.61458);
        assertEquals(c.northSouth, +32.30642);
        assertEquals(c.eastWest, -122.61458);
        assertEquals("+32.30642,-122.61458", c.toString());
    }

    /**
     * Test for correct rejection of decimal degrees that are out of range.
     */
    public void test_ctor_correctRejection() {
        try {
            new CoordinateDD(9000001, 0);
            fail("Expecting " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new CoordinateDD(-9000001, 0);
            fail("Expecting " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new CoordinateDD(0, 18000001);
            fail("Expecting " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            new CoordinateDD(0, -18000001);
            fail("Expecting " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
    }

    /**
     * Test suite for parsing coordinates expressed in decimal degrees.
     * 
     * @throws ParseException
     */
    public void test_parse() throws ParseException {
        {
            CoordinateDD c = new CoordinateDD(+32.30642, -122.61458);
            assertEquals(c, CoordinateDD.parse("+32.30642, -122.61458"));
            assertEquals(c, CoordinateDD.parse("+32.30642,-122.61458"));
            assertEquals(c, CoordinateDD.parse("+32.30642 -122.61458"));
            assertEquals(c, CoordinateDD.parse("+32.30642 / -122.61458"));
            assertEquals(c, CoordinateDD.parse("+32.30642/-122.61458"));
            assertEquals(c, CoordinateDD.parse("+32.30642-122.61458"));
        }
        {
            CoordinateDD c = new CoordinateDD(+36.0726355d, -79.7919754d);
            /*
             * Note: Should round up for both lat and long for this case.
             */
            assertEquals(c, CoordinateDD.parse("+36.0726355 -79.7919754"));
        }
    }
}
