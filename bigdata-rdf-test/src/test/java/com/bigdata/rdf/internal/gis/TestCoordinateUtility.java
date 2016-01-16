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

import java.text.NumberFormat;
import java.text.ParseException;
import com.bigdata.rdf.internal.gis.ICoordinate.UNITS;

/**
 * Test suite for {@link CoordinateUtility}.
 * 
 * @todo test distance between coordinates based on some known points in each
 *       quadrant.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCoordinateUtility extends AbstractCoordinateTestCase {
    /**
     * 
     */
    public TestCoordinateUtility() {
        super();
    }

    /**
     * @param arg0
     */
    public TestCoordinateUtility(String arg0) {
        super(arg0);
    }

    // /**
    // * Test routine to convert a latitude into the angle in the transverse
    // * graticule used by trig functions.
    // */
    // public void test_toTransverseGraticule() {
    //
    // assertEquals(90d,CoordinateUtility.toTransverseGraticule(0));
    //
    // assertEquals(0d,CoordinateUtility.toTransverseGraticule(90));
    //        
    // }
    public void test_toRadians() {
        assertEquals(Math.PI / 180d/* ~.0175 radians */, CoordinateUtility
                .toRadians(1/* degree */));
    }

    public void test_toDegrees() {
        assertEquals(180d / Math.PI/* ~57.296 degrees */, CoordinateUtility
                .toDegrees(1/* radian */));
    }

    /**
     * Cross check for some constants (on Earth).
     */
    public void test_metersLatitude() {
        assertEquals(30.82d,
                CoordinateUtility.metersPerSecondOfLatitudeAtSeaLevel);
        assertEquals(1849.2d,
                CoordinateUtility.metersPerMinuteOfLatitudeAtSeaLevel);
        assertEquals(110952d,
                CoordinateUtility.metersPerDegreeOfLatitudeAtSeaLevel);
    }

    /**
     * Cross check for approximate meters per second of longitude at some known
     * latitudes on Earth.
     * <p>
     * Note: The computed values are rounded off to integers (zero digits after
     * the decimal) since we are comparing to ground truth actual meters per
     * second of longitude but we are computing using a formula that does not
     * adjust for the flattening of the Earth. This makes the test comparisons
     * accurate to within 1m, which is sufficient to verify the calculation.
     */
    public void test_approxMetersLongitude() throws ParseException {
        // The #of digits after the decimal at which to round for the test.
        final int dad = 0;
        // at the equator.
        assertEquals(round(30.92d, dad), round(CoordinateUtility
                .approxMetersPerDegreeOfLongitudeAtSeaLevel(0d) / 3600d, dad));
        // at 30 degrees North/South.
        assertEquals(round(26.76d, dad), round(CoordinateUtility
                .approxMetersPerDegreeOfLongitudeAtSeaLevel(30d) / 3600d, dad));
        assertEquals(round(26.76d, dad), round(CoordinateUtility
                .approxMetersPerDegreeOfLongitudeAtSeaLevel(-30d) / 3600d, dad));
        // at 60 degrees North/South.
        assertEquals(round(15.42d, dad), round(CoordinateUtility
                .approxMetersPerDegreeOfLongitudeAtSeaLevel(60d) / 3600d, dad));
        assertEquals(round(15.42d, dad), round(CoordinateUtility
                .approxMetersPerDegreeOfLongitudeAtSeaLevel(-60d) / 3600d, dad));
        { // at greenwich (51� 28' 38")
            final double degrees = CoordinateUtility.toDecimalDegrees(51, 28,
                    38d);
            System.err.println("Greenwich latitude in decimal degrees: "
                    + degrees);
            assertEquals(
                    round(19.22d, dad),
                    round(
                            CoordinateUtility
                                    .approxMetersPerDegreeOfLongitudeAtSeaLevel(degrees) / 3600d,
                            dad));
            assertEquals(
                    round(19.22d, dad),
                    round(
                            CoordinateUtility
                                    .approxMetersPerDegreeOfLongitudeAtSeaLevel(-degrees) / 3600d,
                            dad));
        }
    }

    /**
     * Cross check for meters per unit of longitude at some known latitudes on
     * Earth.
     * 
     * @todo reduce rounding or remove altogether for this test.
     */
    public void test_realMetersLongitude() throws ParseException {
        // The #of digits after the decimal at which to round for the test.
        final int dad = 2;
        // at the equator.
        assertEquals(30.92d, round(CoordinateUtility
                .realMetersPerDegreeOfLongitudeAtSeaLevel(0d) / 3600d, dad));
        // at 30 degrees North/South.
        assertEquals(26.76d, round(CoordinateUtility
                .realMetersPerDegreeOfLongitudeAtSeaLevel(30d) / 3600d, dad));
        assertEquals(26.76d, round(CoordinateUtility
                .realMetersPerDegreeOfLongitudeAtSeaLevel(-30d) / 3600d, dad));
        // at 60 degrees North/South.
        assertEquals(15.42d, round(CoordinateUtility
                .realMetersPerDegreeOfLongitudeAtSeaLevel(60d) / 3600d, dad));
        assertEquals(15.42d, round(CoordinateUtility
                .realMetersPerDegreeOfLongitudeAtSeaLevel(-60d) / 3600d, dad));
        { // at greenwich (51� 28' 38")
            final double degrees = CoordinateUtility.toDecimalDegrees(51, 28,
                    38d);
            System.err.println("Greenwich latitude in decimal degrees: "
                    + degrees);
            assertEquals(19.22d, round(CoordinateUtility
                    .realMetersPerDegreeOfLongitudeAtSeaLevel(degrees) / 3600d,
                    dad));
            assertEquals(
                    19.22d,
                    round(
                            CoordinateUtility
                                    .realMetersPerDegreeOfLongitudeAtSeaLevel(-degrees) / 3600d,
                            dad));
        }
    }

    /**
     * A test with two points that are exactly the same.
     * 
     * @throws ParseException
     */
    public void test_distance_noDistance01() throws ParseException {
        CoordinateDD c = new CoordinateDD(0, 0);
        assertEquals(0d, c.northSouth);
        assertEquals(0d, c.eastWest);
        assertEquals(0d, CoordinateUtility.distance(c, c, UNITS.Meters));
    }

    public void test_distance_noDistance02() throws ParseException {
        CoordinateDD c = CoordinateDD.parse("+36.0726355 -79.7919754");
        assertEquals(0d, CoordinateUtility.distance(c, c, UNITS.Meters));
    }

    /**
     * Note: The road distance by the most direct route (US 29) between
     * Greensboro, NC and Washington, DC is 299 miles.
     * 
     * <pre>
     *    
     *    36.0726355 -79.7919754 360421N 0794731W Greensboro North Carolina
     *    
     *    38.8951118 -77.0363658 385342N 0770211W Washington West (Washington, DC)
     * </pre>
     * 
     * @see http://geonames.usgs.gov/
     * @see http://geonames.usgs.gov/pls/gnispublic/f?p=181:1:16460812721599042146
     * 
     */
    public void test_distance_washingtonDC_greensboroNC() throws ParseException {
        CoordinateDD p1 = CoordinateDD.parse("36.0726355 -79.7919754");
        CoordinateDD p2 = CoordinateDD.parse("38.8951118 -77.0363658");
        double d = CoordinateUtility.distance(p1, p2, UNITS.Miles);
        NumberFormat fmt = NumberFormat.getNumberInstance();
        fmt.setMinimumFractionDigits(3);
        System.err
                .println("The distance between Washington, DC and Greensboro, NC is "
                        + fmt.format(d) + " miles");
    }
}
