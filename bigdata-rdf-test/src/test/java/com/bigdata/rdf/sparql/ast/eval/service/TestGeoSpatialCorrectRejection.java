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
 * Created on March 16, 2016
 */
package com.bigdata.rdf.sparql.ast.eval.service;

import java.util.Properties;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.internal.impl.extensions.InvalidGeoSpatialLiteralError;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.service.geospatial.GeoSpatialSearchException;

/**
 * Correct rejection test for GeoSpatial data, making sure that appropriate error
 * messages are thrown in cases where we encounter data format problems or service
 * calls are not properly configured.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestGeoSpatialCorrectRejection extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestGeoSpatialCorrectRejection() {
    }

    /**
     * @param name
     */ 
    public TestGeoSpatialCorrectRejection(String name) {
        super(name);
    }
    
    
    /**
     * Test case with geospatial literal that has too many elements.
     */
    public void testCRWrongDataFormat01() throws Exception {

        try {

            new TestHelper(
               "geo-cr-wrongdataformat01",
               "geo-cr-wrongdataformat.rq", 
               "geo-cr-wrongdataformat01.nt",
               "geo-cr-empty.srx").runTest();

        } catch (Throwable e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(InvalidGeoSpatialLiteralError.class.getName()));
            
            return; // expected
        }
        
        throw new RuntimeException("Invalid geospatial literal should have been rejected.");
    }

    /**
     * Test case with geospatial literal that has not enough elements.
     */
    public void testCRWrongDataFormat02() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-wrongdataformat01",
                "geo-cr-wrongdataformat.rq", 
                "geo-cr-wrongdataformat01.nt",
                "geo-cr-empty.srx").runTest();  
            
        } catch (Throwable e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(InvalidGeoSpatialLiteralError.class.getName()));
            
            return; // expected
        }
        
        throw new RuntimeException("Invalid geospatial literal should have been rejected.");
    }

    /**
     * Test case with geospatial literal that is not numerical.
     */
    public void testCRWrongDataFormat03() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-wrongdataformat03",
                "geo-cr-wrongdataformat.rq", 
                "geo-cr-wrongdataformat03.nt",
                "geo-cr-empty.srx").runTest();

        } catch (Throwable e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(InvalidGeoSpatialLiteralError.class.getName()));
            
            return; // expected
        }
        
        throw new RuntimeException("Invalid geospatial literal should have been rejected.");
        
    }

    /**
     * Test case with timeStart specification missing.
     */
    public void testCRMissingTimeStart() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-missingtimestart",
                "geo-cr-missingtimestart.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();

        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }
        
        throw new RuntimeException("Expected to run into exception.");

    }
    
    /**
     * Test case with timeEnd specification missing.
     */
    public void testCRMissingTimeEnd() throws Exception {

        try {

            new TestHelper(
                "geo-cr-missingtimeend",
                "geo-cr-missingtimeend.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");

    }
    
    
    /**
     * Test case with timeStart specification given, but not present in index.
     */
    public void testCRUnusableTimeStart() throws Exception {
        
        try {

            new TestHelper(
                "geo-cr-unusabletimestart",
                "geo-cr-unusabletimestart.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }
        
        throw new RuntimeException("Expected to run into exception.");
        
    }
    
    /**
     * Test case with timeEnd specification missing, but not present in index.
     */
    public void testCRUnusableTimeEnd() throws Exception {
        
        try {
            
            new TestHelper(
                "geo-cr-unusabletimeend",
                "geo-cr-unusabletimeend.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");

    }
    
    /**
     * Test case with customFields specification missing.
     */
    public void testCRMissingCustomFields01() throws Exception {
        
        try {
            
            new TestHelper(
                "geo-cr-missingcustomfields",
                "geo-cr-missingcustomfields01.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");

    }
    
    /**
     * Test case with customFields specification missing.
     */
    public void testCRMissingCustomFields02() throws Exception {
        
        try {
            
            new TestHelper(
                "geo-cr-missingcustomfields",
                "geo-cr-missingcustomfields02.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");

    }
    
    /**
     * Test case with customFields specification missing.
     */
    public void testCRMissingCustomFields03() throws Exception {
        
        try {
            
            new TestHelper(
                "geo-cr-missingcustomfields",
                "geo-cr-missingcustomfields03.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");

    }
    
    /**
     * Test case with customFields specification missing.
     */
    public void testCRMissingCustomFields04() throws Exception {
        
        try {
            
            new TestHelper(
                "geo-cr-missingcustomfields",
                "geo-cr-missingcustomfields04.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");

    }
    
    /**
     * Test case with customFields specification being incomplete.
     */
    public void testCRIncompleteCustomFields() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-incompletecustomfields",
                "geo-cr-incompletecustomfields.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");

    }
    
    /**
     * Test case with customFields given, but no custom fields being defined in the index.
     */
    public void testCRUnusableCustomFields() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-unusablecustomfields",
                "geo-cr-unusablecustomfields.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");

    }
    
    /**
     * Test case with customFields specification containing too many elements.
     */
    public void testCRTooManyCustomFields() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-toomanycustomfields",
                "geo-cr-toomanycustomfields.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");

    }
    
    /**
     * Test case where geo function is not given, but index requires it.
     */
    public void testCRMissingGeoFunction() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-missinggeofunction",
                "geo-cr-missinggeofunction.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");

    }
    
    /**
     * Test case where geo function is invalid.
     */
    public void testCRInvalidGeoFunction() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-invalidgeofunction",
                "geo-cr-invalidgeofunction.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");

    }
    
    /**
     * Test case where geo function is given, but index cannot use it.
     */
    public void testCRUnusableGeoFunction() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-unusablegeofunction",
                "geo-cr-unusablegeofunction.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");

    }
    
    /**
     * Test case where coordinate system is given, but index requires it.
     */
    public void testCRMissingCoordSystem() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-missingcoordsystem",
                "geo-cr-missingcoordsystem.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");

    }

    /**
     * Test case where coordinate system is given, but index cannot use it.
     */
    public void testCRUnusableCoordSystem() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-unusablecoordsystem",
                "geo-cr-unusablecoordsystem.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
        
    }
    
    /**
     * Test case where rectangle query is given, but no south-west coordinate is specified.
     */
    public void testCRInRectangleNoSouthWest() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-inrectanglenosouthwest",
                "geo-cr-inrectanglenosouthwest.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }
    
    /**
     * Test case where rectangle query is given and south-west is not a valid coordinate.
     */
    public void testCRInRectangleInvalidSouthWest() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-inrectangleinvalidsouthwest",
                "geo-cr-inrectangleinvalidsouthwest.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }
    
    /**
     * Test case where rectangle query is given, but no north-east coordinate is specified.
     */
    public void testCRInRectangleNoNorthEast() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-inrectanglenonortheast",
                "geo-cr-inrectanglenonortheast.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
        
    }
    
    /**
     * Test case where rectangle query is given and north-east is not a valid coordinate.
     */
    public void testCRInRectangleInvalidNorthEast() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-inrectangleinvalidnortheast",
                "geo-cr-inrectangleinvalidnortheast.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }
    
    /**
     * Test case where rectangle query is given in combination with a circle center.
     */
    public void testCRInRectangleCenterGiven() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-inrectanglecentergiven",
                "geo-cr-inrectanglecentergiven.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }
    
    /**
     * Test case where rectangle query is given in combination with a circle radius.
     */
    public void testCRInRectangleRadiusGiven() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-inrectangleradiusgiven",
                "geo-cr-inrectangleradiusgiven.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }
    
    
    /**
     * Test case where circle query is given in combination with a south-west coordinate.
     */
    public void testCRInCircleSouthWestGiven() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-incirclesouthwestgiven",
                "geo-cr-incirclesouthwestgiven.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }
    
    /**
     * Test case where circle query is given in combination with a north-east coordinate.
     */
    public void testCRInCircleNorthEastGiven() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-incirclenortheastgiven",
                "geo-cr-incirclenortheastgiven.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }
    
    /**
     * Test case where circle query is given but no circle center is specified.
     */
    public void testCRInCircleNoCenterGiven() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-incirclenocentergiven",
                "geo-cr-incirclenocentergiven.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }
    
    /**
     * Test case where circle query is given with an invalid center
     */
    public void testCRInCircleInvalidCenterGiven() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-incircleinvalidcentergiven",
                "geo-cr-incircleinvalidcentergiven.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }

    /**
     * Test case where circle query is given but no radius is specified.
     */
    public void testCRInCircleNoRadiusGiven() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-incirclenoradiusgiven",
                "geo-cr-incirclenoradiusgiven.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }
    
    /**
     * Test case where circle query is given with an invalid radius
     */
    public void testCRInCircleInvalidRadiusGiven() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-incircleinvalidradiusgiven",
                "geo-cr-incircleinvalidradiusgiven.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }
    
    /**
     * Test case where we aim at extracting the location value, but the index contains no location.
     */
    public void testCRLocationValueButNoLocation() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-locationvaluebutnolocation",
                "geo-cr-locationvaluebutnolocation.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }

    /**
     * Test case where we aim at extracting the locationAndTime value, but the index contains no location.
     */
    public void testCRLocationAndTimeValueButNoLocation() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-locationandtimevaluebutnolocation",
                "geo-cr-locationandtimevaluebutnolocation.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }
    
    /**
     * Test case where we aim at extracting the locationAndTime value, but the index contains no time.
     */
    public void testCRLocationAndTimeValueButNoTime() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-locationandtimevaluebutnotime",
                "geo-cr-locationandtimevaluebutnotime.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }

    /**
     * Test case where we aim at extracting the latitude value, but the index contains no location.
     */
    public void testCRLatValueButNoLocation() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-latvaluebutnolocation",
                "geo-cr-latvaluebutnolocation.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }
    
    /**
     * Test case where we aim at extracting the longitude value, but the index contains no location.
     */
    public void testCRLonValueButNoLocation() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-lonvaluebutnolocation",
                "geo-cr-lonvaluebutnolocation.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }
    
    /**
     * Test case where we aim at extracting the time value, but the index contains no time.
     */
    public void testCRTimeValueButNoTime() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-timevaluebutnotime",
                "geo-cr-timevaluebutnotime.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }
    
    /**
     * Test case where we aim at extracting the distance, but the index contains no latitude/longitude.
     */
    public void testCRDistanceButNoGeospatialComponent() {
        
        try {
            
            new TestHelper(
                "geo-cr-distancevaluebutnogeospatial",
                "geo-cr-distancevaluebutnogeospatial.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }
    
    /**
     * Test case where we aim at extracting the distance, but the index contains no latitude/longitude.
     */
    public void testCRDistanceButRectangleQuery() {
        
        try {
            
            new TestHelper(
                "geo-cr-distancevaluebutrectanglequery",
                "geo-cr-distancevaluebutrectanglequery.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }

    /**
     * Test case where we aim at extracting the time value, but the index contains no time.
     */
    public void testCRCustomFieldsValueButNoCustomFields() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-customfieldsvaluebutnocustomfields",
                "geo-cr-customfieldsvaluebutnocustomfields.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }

    /**
     * Test case where we query a datatype that is not a geospatial one.
     */
    public void testCRUnknownGeoSpatialDatatype() throws Exception {

        try {
            
            new TestHelper(
                "geo-cr-unknowngeospatialdatatype",
                "geo-cr-unknowngeospatialdatatype.rq", 
                "empty.trig",
                "geo-cr-empty.srx").runTest();
        
        } catch (Exception e) {
            
            // check for wrapped exception
            assertTrue(e.toString().contains(GeoSpatialSearchException.class.getName()));
            
            return; // expected
        }

        throw new RuntimeException("Expected to run into exception.");
    }
    
    @Override
    public Properties getProperties() {

        // Note: clone to avoid modifying!!!
        final Properties properties = (Properties) super.getProperties().clone();

        // turn on quads.
        properties.setProperty(AbstractTripleStore.Options.QUADS, "false");

        // TM not available with quads.
        properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE,"false");

        // turn off axioms.
        properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
                NoAxioms.class.getName());

        // no persistence.
        properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE,
                BufferMode.Transient.toString());

        // enable GeoSpatial index
        properties.setProperty(
           com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL, "true");
        
        properties.setProperty(
           com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL_DATATYPE_CONFIG + ".0",
           "{\"config\": "
           + "{ \"uri\": \"http://my.custom.datatype/x-y-z\", "
           + "\"fields\": [ "
           + "{ \"valueType\": \"DOUBLE\", \"multiplier\": \"1000\", \"serviceMapping\": \"x\" }, "
           + "{ \"valueType\": \"DOUBLE\", \"multiplier\": \"1000\", \"serviceMapping\": \"y\" }, "
           + "{ \"valueType\": \"DOUBLE\", \"multiplier\": \"1000\", \"serviceMapping\": \"z\" } "
           + "]}}");

        properties.setProperty(
           com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL_DATATYPE_CONFIG + ".1",
           "{\"config\": "
           + "{ \"uri\": \"http://my.custom.datatype/time-x-y-z\", "
           + "\"fields\": [ "
           + "{ \"valueType\": \"LONG\", \"minVal\" : \"0\", \"multiplier\": \"1\", \"serviceMapping\": \"TIME\" }, "
           + "{ \"valueType\": \"DOUBLE\", \"multiplier\": \"1000\", \"serviceMapping\": \"x\" }, "
           + "{ \"valueType\": \"DOUBLE\", \"multiplier\": \"1000\", \"serviceMapping\": \"y\" }, "
           + "{ \"valueType\": \"DOUBLE\", \"multiplier\": \"1000\", \"serviceMapping\": \"z\" }"
           + "]}}");

        properties.setProperty(
                com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL_DATATYPE_CONFIG + ".2",
                "{\"config\": "
                + "{ \"uri\": \"http://my.custom.datatype/x-y-z-lat-lon\", "
                + "\"fields\": [ "
                + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"-1000\", \"multiplier\": \"10\", \"serviceMapping\": \"x\" }, "
                + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"-10\", \"multiplier\": \"100\", \"serviceMapping\": \"y\" }, "
                + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"-2\", \"multiplier\": \"1000\", \"serviceMapping\": \"z\" }, "
                + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"1000000\", \"serviceMapping\": \"LATITUDE\" }, "
                + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"100000\", \"serviceMapping\": \"LONGITUDE\" } "
                + "]}}");
        
        properties.setProperty(
                com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL_DATATYPE_CONFIG + ".3",
                "{\"config\": "
                + "{ \"uri\": \"http://my.custom.datatype/x-y-z-lat-lon-time\", "
                + "\"fields\": [ "
                + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"-1000\", \"multiplier\": \"10\", \"serviceMapping\": \"x\" }, "
                + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"-10\", \"multiplier\": \"100\", \"serviceMapping\": \"y\" }, "
                + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"-2\", \"multiplier\": \"1000\", \"serviceMapping\": \"z\" }, "
                + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"1000000\", \"serviceMapping\": \"LATITUDE\" }, "
                + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"100000\", \"serviceMapping\": \"LONGITUDE\" }, "
                + "{ \"valueType\": \"LONG\", \"minVal\" : \"0\", \"multiplier\": \"1\", \"serviceMapping\": \"TIME\" } "
                + "]}}");
        
        properties.setProperty(
           com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL_DATATYPE_CONFIG + ".4",
           "{\"config\": "
           + "{ \"uri\": \"http://my.custom.datatype/x-y-z-lat-lon-time-coord\", "
           + "\"fields\": [ "
           + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"-1000\", \"multiplier\": \"10\", \"serviceMapping\": \"x\" }, "
           + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"-10\", \"multiplier\": \"100\", \"serviceMapping\": \"y\" }, "
           + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"-2\", \"multiplier\": \"1000\", \"serviceMapping\": \"z\" }, "
           + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"1000000\", \"serviceMapping\": \"LATITUDE\" }, "
           + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"100000\", \"serviceMapping\": \"LONGITUDE\" }, "
           + "{ \"valueType\": \"LONG\", \"minVal\" : \"0\", \"multiplier\": \"1\", \"serviceMapping\": \"TIME\" }, "
           + "{ \"valueType\": \"LONG\", \"minVal\" : \"0\", \"multiplier\": \"1\", \"serviceMapping\": \"COORD_SYSTEM\" } "
           + "]}}");

        properties.setProperty(
                com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL_DATATYPE_CONFIG + ".5",
                "{\"config\": "
                + "{ \"uri\": \"http://my.custom.datatype/lat-lon\", "
                + "\"fields\": [ "
                + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"1000000\", \"serviceMapping\": \"LATITUDE\" }, "
                + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"100000\", \"serviceMapping\": \"LONGITUDE\" } "
                + "]}}");        
        
        properties.setProperty(
                com.bigdata.rdf.store.AbstractLocalTripleStore.Options.VOCABULARY_CLASS,
                "com.bigdata.rdf.sparql.ast.eval.service.GeoSpatialTestVocabulary");

        return properties;

    }
}
