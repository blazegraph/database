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
 * Data driven test suite for GeoSpatial service feature aiming at the definition of
 * custom fields (possibly combined with predefined fields).
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestGeoSpatialCustomFields extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestGeoSpatialCustomFields() {
    }

    /**
     * @param name
     */ 
    public TestGeoSpatialCustomFields(String name) {
        super(name);
    }
    
    
    /**
     * Simple basic test case for (x,y,z) index
     */
    public void testCustomFieldsXYZ01() throws Exception {
        
        new TestHelper(
           "geo-customfields-xyz01",
           "geo-customfields-xyz01.rq", 
           "geo-customfields.nt",
           "geo-customfields-xyz01.srx").runTest();
        
    }
    
    /**
     * Simple basic test case for (TIME,x,y,z) index
     */
    public void testCustomFieldsTimeXYZ01() throws Exception {
        
        new TestHelper(
           "geo-customfields-txyz01",
           "geo-customfields-txyz01.rq", 
           "geo-customfields.nt",
           "geo-customfields-txyz01.srx").runTest();
        
    }

    /**
     * Simple basic test case for (x,y,z,TIME,LAT,LON) index
     */
    public void testCustomFieldsXYZLatLonTime01() throws Exception {
        
        new TestHelper(
           "geo-customfields-xyzllt01",
           "geo-customfields-xyzllt01.rq", 
           "geo-customfields.nt",
           "geo-customfields-xyzllt01.srx").runTest();
        
    }
    
    /**
     * Simple basic test case for (x,y,z) index, where we additionally
     * extract values from the index.
     */
    public void testCustomFieldsXYZ02() throws Exception {
        
        new TestHelper(
           "geo-customfields-xyz02",
           "geo-customfields-xyz02.rq", 
           "geo-customfields.nt",
           "geo-customfields-xyz02.srx").runTest();
        
    }
    
    /**
     * Simple basic test case for (TIME,x,y,z) index, where we additionally
     * extract values from the index.
     */
    public void testCustomFieldsTimeXYZ02() throws Exception {
        
        new TestHelper(
           "geo-customfields-txyz02",
           "geo-customfields-txyz02.rq", 
           "geo-customfields.nt",
           "geo-customfields-txyz02.srx").runTest();
        
    }

    /**
     * Simple basic test case for (x,y,z,TIME,LAT,LON) index, where we 
     * additionally extract values from the index.
     */
    public void testCustomFieldsXYZLatLonTime02() throws Exception {
        
        new TestHelper(
           "geo-customfields-xyzllt02",
           "geo-customfields-xyzllt02.rq", 
           "geo-customfields.nt",
           "geo-customfields-xyzllt02.srx").runTest();
        
    }
    

    // TODO: test cases with shifts for the custom fields...

  
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
           + "{ \"uri\": \"http://my.custom.datatype/x-y-z-lat-lon-time\", "
           + "\"fields\": [ "
           + "{ \"valueType\": \"DOUBLE\", \"multiplier\": \"1000\", \"serviceMapping\": \"x\" }, "
           + "{ \"valueType\": \"DOUBLE\", \"multiplier\": \"1000\", \"serviceMapping\": \"y\" }, "
           + "{ \"valueType\": \"DOUBLE\", \"multiplier\": \"1000\", \"serviceMapping\": \"z\" }, "
           + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"1000000\", \"serviceMapping\": \"LATITUDE\" }, "
           + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"100000\", \"serviceMapping\": \"LONGITUDE\" }, "
           + "{ \"valueType\": \"LONG\", \"minVal\" : \"0\", \"multiplier\": \"1\", \"serviceMapping\": \"TIME\" } "
           + "]}}");

        properties.setProperty(
                com.bigdata.rdf.store.AbstractLocalTripleStore.Options.VOCABULARY_CLASS,
                "com.bigdata.rdf.sparql.ast.eval.service.GeoSpatialTestVocabulary");

        return properties;

    }
}
