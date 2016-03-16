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
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.rdf.store.AbstractTripleStore;

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
        // TODO: implement        
    }

    /**
     * Test case with geospatial literal that has not enough elements.
     */
    public void testCRWrongDataFormat02() throws Exception {
        // TODO: implement        
    }

    /**
     * Test case with geospatial literal that is not numerical.
     */
    public void testCRWrongDataFormat03() throws Exception {
        // TODO: implement        
    }

    /**
     * Test case with timeStart specification missing.
     */
    public void testCRMissingTimeStart() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case with timeEnd specification missing.
     */
    public void testCRMissingTimeEnd() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case with timeStart specification given, but not present in index.
     */
    public void testCRUnusableTimeStart() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case with timeEnd specification missing, but not present in index.
     */
    public void testCRUnusableTimeEnd() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case with customFields specification missing.
     */
    public void testCRMissingCustomFields() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case with customFields specification being incomplete.
     */
    public void testCRIncompleteCustomFields() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case with customFields given, but no custom fields being defined in the index.
     */
    public void testCRUnusableCustomFields() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case with customFields specification containing too many elements.
     */
    public void testCRTooManyCustomFields() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case with customFields specification containing non-existent elements.
     */
    public void testCRNonExistsentCustomFieldsElements() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case where geo function is not given, but index requires it.
     */
    public void testCRMissingGeoFunction() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case where geo function is given, but index cannot use it.
     */
    public void testCRUnusableGeoFunction() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case where coordinate system is given, but index requires it.
     */
    public void testCRMissingCoordSystem() throws Exception {
        // TODO: implement
    }

    /**
     * Test case where coordinate system is given, but index cannot use it.
     */
    public void testCRUnusableCoordSystem() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case where rectangle query is given, but no south-west coordinate is specified.
     */
    public void testCRInRectangleNoSouthWest() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case where rectangle query is given, but no north-east coordinate is specified.
     */
    public void testCRInRectangleNoNorthEast() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case where rectangle query is given in combination with a circle center.
     */
    public void testCRInRectangleCircleCenterGiven() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case where circle query is given in combination with a south-west coordinate.
     */
    public void testCRInCircleSouthWestGiven() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case where circle query is given in combination with a north-east coordinate.
     */
    public void testCRInCircleNoNorthEastGiven() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case where circle query is given bit no circle center is specified.
     */
    public void testCRInCircleNoCenterGiven() throws Exception {
        // TODO: implement
    }

    /**
     * Test case where we aim at extracting the location value, but the index contains no location.
     */
    public void testCRLocationValueButNoLocation() throws Exception {
        // TODO: implement
    }

    /**
     * Test case where we aim at extracting the locationAndTime value, but the index contains no location.
     */
    public void testCRLocationAndTimeValueButNoLocation() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case where we aim at extracting the locationAndTime value, but the index contains no time.
     */
    public void testCRLocationAndTimeValueButNoTime() throws Exception {
        // TODO: implement
    }

    /**
     * Test case where we aim at extracting the latitude value, but the index contains no location.
     */
    public void testCRLatValueButNoLocations() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case where we aim at extracting the longitude value, but the index contains no location.
     */
    public void testCRLonValueButNoLocations() throws Exception {
        // TODO: implement
    }
    
    /**
     * Test case where we aim at extracting the time value, but the index contains no time.
     */
    public void testCRTimeValueButNoTime() throws Exception {
        // TODO: implement
    }

    /**
     * Test case where we aim at extracting the time value, but the index contains no time.
     */
    public void testCRCustomFieldsValueButNoCustomFields() throws Exception {
        // TODO: implement
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
                com.bigdata.rdf.store.AbstractLocalTripleStore.Options.VOCABULARY_CLASS,
                "com.bigdata.rdf.sparql.ast.eval.service.GeoSpatialTestVocabulary");

        return properties;

    }
}
