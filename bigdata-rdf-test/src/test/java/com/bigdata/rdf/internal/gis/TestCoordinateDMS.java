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
import java.util.regex.Pattern;

/**
 * Test suite for {@link CoordinateDMS}.
 * 
 * @todo write tests for normalization @ +/-180 east/west.
 * 
 * @todo write correct rejection tests for the constructor.
 * 
 * @todo write conversion tests to/from decimal degrees and decimal minutes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCoordinateDMS extends AbstractCoordinateTestCase {
    /**
     * 
     */
    public TestCoordinateDMS() {
        super();
    }

    /**
     * @param arg0
     */
    public TestCoordinateDMS(String arg0) {
        super(arg0);
    }

    /**
     * Test the routine that parses text containing seconds and optional tenths
     * of a second.
     * 
     * @throws ParseException
     */
    public void test_parseTenthsOfSecond() throws ParseException {
        assertEquals(525, CoordinateDMS.parseTenthsOfSecond("52.5"));
        assertEquals(520, CoordinateDMS.parseTenthsOfSecond("52.0"));
        assertEquals(520, CoordinateDMS.parseTenthsOfSecond("52."));
        assertEquals(520, CoordinateDMS.parseTenthsOfSecond("52"));
    }

    /**
     * Test the routine that formats tenths of a second.
     */
    public void test_formatTenthsOfSecond() {
        assertEquals("52.0", CoordinateDMS.formatTenthsOfSecond(520));
        assertEquals("52.5", CoordinateDMS.formatTenthsOfSecond(525));
    }

    /**
     * Verify basic constructor and formatter using decimal degrees
     * 
     * <pre>
     * 32� 18' 23.1&quot; N 122� 36' 52.5&quot; W
     * </pre>
     * 
     * @todo test ctor in all quadrants.
     */
    public void test_ctor() throws ParseException {
        CoordinateDMS c = new CoordinateDMS(//
                32, 18, 231,// northSouth
                -122, -36, -525 // eastWest
        );
        assertEquals(c.degreesNorth, 32);
        assertEquals(c.minutesNorth, 18);
        assertEquals(c.tenthsOfSecondsNorth, 231);
        assertEquals(c.degreesEast, -122);
        assertEquals(c.minutesEast, -36);
        assertEquals(c.tenthsOfSecondsEast, -525);
        // Validate the representation of the coordinate as text.
        assertEquals("32 18 23.1N 122 36 52.5W", c.toString());
    }

    /**
     * Test of {@link CoordinateDMS#roundSeconds()}
     * 
     * @todo test rounding in all quadrants.
     */
    public void test_roundSeconds() {
        CoordinateDMS c = new CoordinateDMS(//
                32, 18, 231,// northSouth
                -122, -36, -525 // eastWest
        );
        CoordinateDMS c1 = new CoordinateDMS(//
                32, 18, 230,// northSouth
                -122, -36, -530 // eastWest
        );
        assertEquals(c1, c.roundSeconds());
    }

    /**
     * Test of {@link CoordinateDMS#roundMinutes()}.
     * 
     * @todo test rounding in all quadrants.
     */
    public void test_roundMinutes() {
        CoordinateDMS c = new CoordinateDMS(//
                32, 18, 231,// northSouth
                -122, -36, -525 // eastWest
        );
        CoordinateDMS expected = new CoordinateDMS(//
                32, 18, 0,// northSouth
                -122, -37, 0 // eastWest
        );
        CoordinateDMS actual = c.roundMinutes();
        assertEquals(expected, actual);
    }

    /**
     * Test of the regular expression used to match a multipart latitude.
     */
    public void test_regex_lat() {
        Pattern p = Pattern.compile(CoordinateDMS.regex_lat);
        {
            String text = "32� 18' 23.1\" N";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("32", m.group(2));
            assertEquals("18", m.group(3));
            assertEquals("23.1", m.group(5));
            assertEquals("N", m.group(6));
        }
        {
            String text = "32� 18' 23.1\"N";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("32", m.group(2));
            assertEquals("18", m.group(3));
            assertEquals("23.1", m.group(5));
            assertEquals("N", m.group(6));
        }
        {
            String text = "32 18 23.1 N";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("32", m.group(2));
            assertEquals("18", m.group(3));
            assertEquals("23.1", m.group(5));
            assertEquals("N", m.group(6));
        }
        {
            String text = "32 18 23.1N";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("32", m.group(2));
            assertEquals("18", m.group(3));
            assertEquals("23.1", m.group(5));
            assertEquals("N", m.group(6));
        }
        {
            String text = "32 18 23 N";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("32", m.group(2));
            assertEquals("18", m.group(3));
            assertEquals("23", m.group(5));
            assertEquals("N", m.group(6));
        }
        {
            String text = "32 18 23N";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("32", m.group(2));
            assertEquals("18", m.group(3));
            assertEquals("23", m.group(5));
            assertEquals("N", m.group(6));
        }
        {
            String text = "32 18 N";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("32", m.group(2));
            assertEquals("18", m.group(3));
            assertEquals(null, m.group(5));
            assertEquals("N", m.group(6));
        }
        {
            String text = "32 18N";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("32", m.group(2));
            assertEquals("18", m.group(3));
            assertEquals(null, m.group(5));
            assertEquals("N", m.group(6));
        }
        {
            String text = "32:18:23.1 N";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("32", m.group(2));
            assertEquals("18", m.group(3));
            assertEquals("23.1", m.group(5));
            assertEquals("N", m.group(6));
        }
        {
            String text = "32:18:23.1N";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("32", m.group(2));
            assertEquals("18", m.group(3));
            assertEquals("23.1", m.group(5));
            assertEquals("N", m.group(6));
        }
        {
            String text = "32:18:23 N";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("32", m.group(2));
            assertEquals("18", m.group(3));
            assertEquals("23", m.group(5));
            assertEquals("N", m.group(6));
        }
        {
            String text = "32:18:23N";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("32", m.group(2));
            assertEquals("18", m.group(3));
            assertEquals("23", m.group(5));
            assertEquals("N", m.group(6));
        }
        // /*
        // * Correct rejection tests for formats without separator characters.
        // */
        // {
        //            
        // String text = "321823N";
        // Matcher m = p.matcher(text);
        // assertFalse("Correct rejection fails: "+text, m.matches());
        //            
        // }
        //        
        // {
        //            
        // String text = "3218N";
        // Matcher m = p.matcher(text);
        // assertFalse("Correct rejection fails: "+text, m.matches());
        //            
        // }
    }

    /**
     * Test of the regular expression used to match a multipart longitude.
     */
    public void test_regex_long() {
        Pattern p = Pattern.compile(CoordinateDMS.regex_long);
        {
            String text = "122� 36' 52.5\" W";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("122", m.group(2));
            assertEquals("36", m.group(3));
            assertEquals("52.5", m.group(5));
            assertEquals("W", m.group(6));
        }
        {
            String text = "122� 36' 52.5\"W";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("122", m.group(2));
            assertEquals("36", m.group(3));
            assertEquals("52.5", m.group(5));
            assertEquals("W", m.group(6));
        }
        {
            String text = "122 36 52.5 W";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("122", m.group(2));
            assertEquals("36", m.group(3));
            assertEquals("52.5", m.group(5));
            assertEquals("W", m.group(6));
        }
        {
            String text = "122 36 52.5W";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("122", m.group(2));
            assertEquals("36", m.group(3));
            assertEquals("52.5", m.group(5));
            assertEquals("W", m.group(6));
        }
        {
            String text = "122 36 52 W";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("122", m.group(2));
            assertEquals("36", m.group(3));
            assertEquals("52", m.group(5));
            assertEquals("W", m.group(6));
        }
        {
            String text = "122 36 52W";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("122", m.group(2));
            assertEquals("36", m.group(3));
            assertEquals("52", m.group(5));
            assertEquals("W", m.group(6));
        }
        {
            String text = "122 36 W";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("122", m.group(2));
            assertEquals("36", m.group(3));
            assertEquals(null, m.group(5));
            assertEquals("W", m.group(6));
        }
        {
            String text = "122 36W";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("122", m.group(2));
            assertEquals("36", m.group(3));
            assertEquals(null, m.group(5));
            assertEquals("W", m.group(6));
        }
        {
            String text = "122:36:52.5 W";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("122", m.group(2));
            assertEquals("36", m.group(3));
            assertEquals("52.5", m.group(5));
            assertEquals("W", m.group(6));
        }
        {
            String text = "122:36:52.5W";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("122", m.group(2));
            assertEquals("36", m.group(3));
            assertEquals("52.5", m.group(5));
            assertEquals("W", m.group(6));
        }
        {
            String text = "122:36:52 W";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("122", m.group(2));
            assertEquals("36", m.group(3));
            assertEquals("52", m.group(5));
            assertEquals("W", m.group(6));
        }
        {
            String text = "122:36:52W";
            Matcher m = p.matcher(text);
            assertTrue(m.matches());
            assertEquals("122", m.group(2));
            assertEquals("36", m.group(3));
            assertEquals("52", m.group(5));
            assertEquals("W", m.group(6));
        }
        // /*
        // * Correct rejection tests for formats without separator characters.
        // */
        // {
        //            
        // String text = "1223652W";
        // Matcher m = p.matcher(text);
        // assertFalse("Correct rejection fails: "+text, m.matches());
        //            
        // }
        //        
        // {
        //            
        // String text = "3218N/12236W";
        // Matcher m = p.matcher(text);
        // assertFalse("Correct rejection fails: "+text, m.matches());
        //            
        // }
    }

    // /**
    // * Test for the regular expression {@link
    // CoordinateDMS#regex_latLong_minutes}.
    // */
    // public void test_regex_latLong_minutes() {
    //
    // Pattern p = Pattern.compile(CoordinateDMS.regex_latLong_minutes);
    //
    // {
    //            
    // String text = "3218N/12236W";
    // Matcher m = p.matcher(text);
    // assertTrue(m.matches());
    // assertEquals("3218",m.group(2));
    // assertEquals("N",m.group(3));
    // assertEquals("12236",m.group(5));
    // assertEquals("W",m.group(6));
    //            
    // }
    //
    // {
    //            
    // String text = "3218N / 12236W";
    // Matcher m = p.matcher(text);
    // assertTrue(m.matches());
    // assertEquals("3218",m.group(2));
    // assertEquals("N",m.group(3));
    // assertEquals("12236",m.group(5));
    // assertEquals("W",m.group(6));
    //            
    // }
    //
    // {
    //            
    // String text = "3218N12236W";
    // Matcher m = p.matcher(text);
    // assertTrue(m.matches());
    // assertEquals("3218",m.group(2));
    // assertEquals("N",m.group(3));
    // assertEquals("12236",m.group(5));
    // assertEquals("W",m.group(6));
    //            
    // }
    //
    // {
    //            
    // String text = "3218N 12236W";
    // Matcher m = p.matcher(text);
    // assertTrue(m.matches());
    // assertEquals("3218",m.group(2));
    // assertEquals("N",m.group(3));
    // assertEquals("12236",m.group(5));
    // assertEquals("W",m.group(6));
    //            
    // }
    //
    // }
    //    
    // /**
    // * Test for the regular expression {@link
    // CoordinateDMS#regex_latLong_seconds}.
    // */
    // public void test_regex_latLong_seconds() {
    //        
    // Pattern p = Pattern.compile(CoordinateDMS.regex_latLong_seconds);
    //
    // {
    //            
    // String text = "321823N/1223652W";
    // Matcher m = p.matcher(text);
    // assertTrue(m.matches());
    // assertEquals("321823",m.group(2));
    // assertEquals("N",m.group(3));
    // assertEquals("1223652",m.group(5));
    // assertEquals("W",m.group(6));
    //            
    // }
    //
    // {
    //            
    // String text = "321823N / 1223652W";
    // Matcher m = p.matcher(text);
    // assertTrue(m.matches());
    // assertEquals("321823",m.group(2));
    // assertEquals("N",m.group(3));
    // assertEquals("1223652",m.group(5));
    // assertEquals("W",m.group(6));
    //            
    // }
    //
    // {
    //            
    // String text = "321823N 1223652W";
    // Matcher m = p.matcher(text);
    // assertTrue(m.matches());
    // assertEquals("321823",m.group(2));
    // assertEquals("N",m.group(3));
    // assertEquals("1223652",m.group(5));
    // assertEquals("W",m.group(6));
    //            
    // }
    //
    // {
    //            
    // String text = "321823N1223652W";
    // Matcher m = p.matcher(text);
    // assertTrue(m.matches());
    // assertEquals("321823",m.group(2));
    // assertEquals("N",m.group(3));
    // assertEquals("1223652",m.group(5));
    // assertEquals("W",m.group(6));
    //            
    // }
    //
    // }
    /**
     * Test groups satisified by {@link CoordinateDMS#pattern_dms1}.
     */
    public void test_pattern_dms1_groups() {
        {
            String text = "32 18 23.1 N 122 36 52.5 W";
            Matcher m = CoordinateDMS.pattern_dms1.matcher(text);
            assertTrue(m.matches());
            assertEquals("32", m.group(CoordinateDMS.group_degreesNorth));
            assertEquals("18", m.group(CoordinateDMS.group_minutesNorth));
            assertEquals("23.1", m.group(CoordinateDMS.group_secondsNorth));
            assertEquals("N", m.group(CoordinateDMS.group_northSouth));
            assertEquals("122", m.group(CoordinateDMS.group_degreesEast));
            assertEquals("36", m.group(CoordinateDMS.group_minutesEast));
            assertEquals("52.5", m.group(CoordinateDMS.group_secondsEast));
            assertEquals("W", m.group(CoordinateDMS.group_eastWest));
        }
    }

    /**
     * Test parsing of coordinates represented as degrees, minutes, and seconds.
     * 
     * @throws ParseException
     */
    public void test_parse01() throws ParseException {
        CoordinateDMS c = new CoordinateDMS(//
                32, 18, 231,// northSouth
                -122, -36, -525 // eastWest
        );
        assertEquals(c, CoordinateDMS
                .parse("32� 18' 23.1\" N 122� 36' 52.5\" W"));
        assertEquals(c, CoordinateDMS
                .parse("32* 18' 23.1\" N 122* 36' 52.5\" W"));
        assertEquals(c, CoordinateDMS
                .parse("32 * 18 ' 23.1 \" N 122 * 36 ' 52.5 \" W"));
        // @todo accept "deg" for degrees - this will change the group indices.
        //
        // assertEquals(c, CoordinateDMS.parse("32 deg 18' 23.1\" N 122� 36'
        // 52.5\" W"));
        /*
         * * 32 18 23.1N 122 36 52.5 W
         * 
         * 32 18 23.1N/122 36 52.5W
         * 
         * 32:18:23N/122:36:52W
         * 
         * 321823N/1223652W (zeros would need to go in front of single digits
         * and two zeros in front of the longitude degrees because it�s range is
         * up to 180 � the latitude range is only up to 90)
         * 
         * 3218N/12236W
         */
        assertEquals(c, CoordinateDMS.parse("32 18 23.1N 122 36 52.5W"));
        assertEquals(c, CoordinateDMS.parse("32 18 23.1 N 122 36 52.5 W"));
        assertEquals(c, CoordinateDMS.parse("32 18 23.1N/122 36 52.5W"));
        assertEquals(c, CoordinateDMS.parse("32 18 23.1N / 122 36 52.5W"));
        assertEquals(c, CoordinateDMS.parse("32 18 23.1 N / 122 36 52.5 W"));
        assertEquals(c.roundSeconds(), CoordinateDMS
                .parse("32 18 23 N / 122 36 53 W"));
        assertEquals(c.roundSeconds(), CoordinateDMS
                .parse("32 18 23N / 122 36 53W"));
        assertEquals(c.roundSeconds(), CoordinateDMS
                .parse("32 18 23N, 122 36 53W"));
        assertEquals(c.roundMinutes(), CoordinateDMS.parse("32 18N / 122 37W"));
        assertEquals(c.roundMinutes(), CoordinateDMS.parse("32 18N, 122 37W"));
        assertEquals(c.roundMinutes(), CoordinateDMS.parse("32 18N 122 37W"));
        assertEquals(c, CoordinateDMS.parse("32:18:23.1N/122:36:52.5W"));
        assertEquals(c, CoordinateDMS.parse("32:18:23.1N / 122:36:52.5W"));
        assertEquals(c, CoordinateDMS.parse("32:18:23.1N 122:36:52.5W"));
        assertEquals(c.roundSeconds(), CoordinateDMS
                .parse("32:18:23N/122:36:53W"));
        assertEquals(c.roundSeconds(), CoordinateDMS
                .parse("32:18:23N / 122:36:53W"));
        assertEquals(c.roundSeconds(), CoordinateDMS
                .parse("32:18:23N 122:36:53W"));
        assertEquals(c.roundSeconds(), CoordinateDMS.parse("321823N/1223653W"));
        assertEquals(c.roundSeconds(), CoordinateDMS
                .parse("321823N / 1223653W"));
        assertEquals(c.roundSeconds(), CoordinateDMS.parse("321823N 1223653W"));
        assertEquals(c.roundMinutes(), CoordinateDMS.parse("3218N/12237W"));
        assertEquals(c.roundMinutes(), CoordinateDMS.parse("3218N / 12237W"));
        assertEquals(c.roundMinutes(), CoordinateDMS.parse("3218N 12237W"));
    }

    /**
     * Test conversion to decimal degrees. The source for the ground truth data
     * points is the USGS:
     * 
     * <pre>
     *     36.0726355 -79.7919754 360421N 0794731W Greensboro North Carolina
     * </pre>
     * 
     * @see http://www.fcc.gov/mb/audio/bickel/DDDMMSS-decimal.html
     * 
     * @todo test within allowable error (e.g., in feet or meters or ticks).
     */
    public void test_toDD() throws ParseException {
        CoordinateDMS dms = CoordinateDMS.parse("360421N 0794731W");
        assertEquals(36, dms.degreesNorth);
        assertEquals(04, dms.minutesNorth);
        assertEquals(210, dms.tenthsOfSecondsNorth);
        assertEquals(-79, dms.degreesEast);
        assertEquals(-47, dms.minutesEast);
        assertEquals(-310, dms.tenthsOfSecondsEast);
        CoordinateDD dd = dms.toDD();
        System.err.println("dms(" + dms + ") => dd(" + dd + ")");
        assertEquals(36.07250d, round5(dd.northSouth));
        assertEquals(-79.79194d, round5(dd.eastWest));
    }
    // /**
    // * Verify that 180W (-18000000) is normalized to 180E by the constructor.
    // */
    // public void test_ctor_180W() {
    //
    // CoordinateDD c = new CoordinateDD(0, -18000000);
    //        
    // assertEquals(c.eastWest, 18000000);
    //        
    // }
    //
    // /**
    // * Test for correct rejection of decimal degrees that are out of range.
    // */
    // public void test_ctor_correctRejection() {
    //
    // try {
    // new CoordinateDD(9000001, 0);
    // fail("Expecting " + IllegalArgumentException.class);
    // } catch (IllegalArgumentException ex) {
    // System.err.println("Ignoring expected exception: " + ex);
    // }
    //
    // try {
    // new CoordinateDD(-9000001, 0);
    // fail("Expecting " + IllegalArgumentException.class);
    // } catch (IllegalArgumentException ex) {
    // System.err.println("Ignoring expected exception: " + ex);
    // }
    //
    // try {
    // new CoordinateDD(0, 18000001);
    // fail("Expecting " + IllegalArgumentException.class);
    // } catch (IllegalArgumentException ex) {
    // System.err.println("Ignoring expected exception: " + ex);
    // }
    //
    // try {
    // new CoordinateDD(0, -18000001);
    // fail("Expecting " + IllegalArgumentException.class);
    // } catch (IllegalArgumentException ex) {
    // System.err.println("Ignoring expected exception: " + ex);
    // }
    //
    // }
}
