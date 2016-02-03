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
package com.bigdata.rdf.internal.gis;

import java.text.ParseException;
import java.util.regex.Matcher;
import com.bigdata.rdf.internal.gis.ICoordinate.UNITS;

/**
 * Test suite for {@link CoordinateDDM}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCoordinateDDM extends AbstractCoordinateTestCase {
    /**
     * 
     */
    public TestCoordinateDDM() {
        super();
    }

    /**
     * @param arg0
     */
    public TestCoordinateDDM(String arg0) {
        super(arg0);
    }

    /**
     * Test of the constructor and formatter.
     * 
     * <pre>
     * 32� 18.385' N 122� 36.875' W
     * </pre>
     * 
     * @todo test in all quadrants.
     */
    public void test_ctor() {
        CoordinateDDM c = new CoordinateDDM(//
                32, 18385,// northSouth
                -122, -36875 // eastWest
        );
        assertEquals(c.degreesNorth, 32);
        assertEquals(c.thousandthsOfMinutesNorth, 18385);
        assertEquals(c.degreesEast, -122);
        assertEquals(c.thousandthsOfMinutesEast, -36875);
        // Validate the representation of the coordinate as text.
        assertEquals("32 18.385N 122 36.875W", c.toString());
    }

    /**
     * Correct rejection tests for the constructor.
     */
    public void test_ctor_correctRejection() {
//        fail("write tests");
    }

    /**
     * Test verifies the groups into which the pattern breaks the parsed text.
     * 
     * @see CoordinateDDM#pattern_ddm
     */
    public void test_pattern_ddm_groups() {
        {
            String text = "32� 18.385' N 122� 36.875' W";
            Matcher m = CoordinateDDM.pattern_ddm.matcher(text);
            assertTrue(m.matches());
            assertEquals("32", m.group(CoordinateDDM.group_degreesNorth));
            assertEquals("18.385", m.group(CoordinateDDM.group_minutesNorth));
            assertEquals("N", m.group(CoordinateDDM.group_northSouth));
            assertEquals("122", m.group(CoordinateDDM.group_degreesEast));
            assertEquals("36.875", m.group(CoordinateDDM.group_minutesEast));
            assertEquals("W", m.group(CoordinateDDM.group_eastWest));
        }
    }

    /**
     * Tests of the ability to parse a decimal minutes coordinate from text.
     */
    public void test_parse() throws ParseException {
        {
            CoordinateDDM c = CoordinateDDM
                    .parse("32� 18.385' N 122� 36.875' W");
            assertEquals(32, c.degreesNorth);
            assertEquals(18385, c.thousandthsOfMinutesNorth);
            assertEquals(-122, c.degreesEast);
            assertEquals(-36875, c.thousandthsOfMinutesEast);
        }
        {
            CoordinateDDM c = CoordinateDDM.parse("32 18.385 N 122 36.875 W");
            assertEquals(32, c.degreesNorth);
            assertEquals(18385, c.thousandthsOfMinutesNorth);
            assertEquals(-122, c.degreesEast);
            assertEquals(-36875, c.thousandthsOfMinutesEast);
        }
        {
            CoordinateDDM c = CoordinateDDM.parse("32 18.385N 122 36.875W");
            assertEquals(32, c.degreesNorth);
            assertEquals(18385, c.thousandthsOfMinutesNorth);
            assertEquals(-122, c.degreesEast);
            assertEquals(-36875, c.thousandthsOfMinutesEast);
        }
        {
            CoordinateDDM c = CoordinateDDM.parse("32:18.385N 122:36.875W");
            assertEquals(32, c.degreesNorth);
            assertEquals(18385, c.thousandthsOfMinutesNorth);
            assertEquals(-122, c.degreesEast);
            assertEquals(-36875, c.thousandthsOfMinutesEast);
        }
    }

    /**
     * Test of {@link CoordinateDDM#toDD()}. The test data are based on
     * 
     * <pre>
     *      Degrees and Decimal Minutes
     *     
     *      DDD� MM.MMM'
     *      32� 18.385' N 122� 36.875' W
     *     
     *      Decimal Degrees
     *     
     *      DDD.DDDDD�
     *      32.30642� N 122.61458� W
     * </pre>
     */
    public void test_toDD() throws ParseException {
        CoordinateDDM ddm = CoordinateDDM.parse("32� 18.385' N 122� 36.875' W");
        CoordinateDD dd = CoordinateDD.parse("+32.30642� -122.61458�");
        // less than one meter distance between the two points.
        assertEquals(0, (int) dd.distance(ddm.toDD(), UNITS.Meters));
    }
}
