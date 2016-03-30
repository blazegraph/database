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
 * Created on March 21, 2016
 */
package com.bigdata.rdf.sparql.ast.eval.service;

import java.util.Properties;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Data driven test suite for querying of (i) our built-in datatypes and (ii) a
 * comprehensive custom datatype where we restrict only one of the existing dimensions,
 * in order to make sure that the constraints for all dimensions are properly considered.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestGeoSpatialQueryVaryOneDimension extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestGeoSpatialQueryVaryOneDimension() {
    }

    /**
     * @param name
     */ 
    public TestGeoSpatialQueryVaryOneDimension(String name) {
        super(name);
    }


    /**
     * Vary latitude only.
     */
    public void testBuiltinLatLon01() throws Exception {
       
       new TestHelper(
           "geo-vary1dim-builtin-ll01",
           "geo-vary1dim-builtin-ll01.rq", 
           "geo-vary1dim.nt",
           "geo-vary1dim-builtin-ll01.srx").runTest();
       
    }

    /**
     * Vary longitude only.
     */
    public void testBuiltinLatLon02() throws Exception {
        
        new TestHelper(
            "geo-vary1dim-builtin-ll02",
            "geo-vary1dim-builtin-ll02.rq", 
            "geo-vary1dim.nt",
            "geo-vary1dim-builtin-ll02.srx").runTest();
        
    }

    /**
     * Vary latitude only.
     */
    public void testBuiltinLatLonTime01() throws Exception {
        
        new TestHelper(
            "geo-vary1dim-builtin-llt01",
            "geo-vary1dim-builtin-llt01.rq", 
            "geo-vary1dim.nt",
            "geo-vary1dim-builtin-llt01.srx").runTest();
        
    }

    /**
     * Vary longitude only.
     */
    public void testBuiltinLatLonTime02() throws Exception {
         
        new TestHelper(
            "geo-vary1dim-builtin-llt02",
            "geo-vary1dim-builtin-llt02.rq", 
            "geo-vary1dim.nt",
            "geo-vary1dim-builtin-llt02.srx").runTest();
         
    }
     
    /**
     * Vary time only.
     */
    public void testBuiltinLatLonTime03() throws Exception {
          
        new TestHelper(
            "geo-vary1dim-builtin-llt03",
            "geo-vary1dim-builtin-llt03.rq", 
            "geo-vary1dim.nt",
            "geo-vary1dim-builtin-llt03.srx").runTest();
          
    }
     

    /**
     * Vary x only.
     */
    public void testCustomXYZLatLonTimeCoord01() throws Exception {

        new TestHelper(
            "geo-vary1dim-custom-xyzlltc01",
            "geo-vary1dim-custom-xyzlltc01.rq", 
            "geo-vary1dim.nt",
            "geo-vary1dim-custom-xyzlltc01.srx").runTest();
    }

    /**
     * Vary y only.
     */
    public void testCustomXYZLatLonTimeCoord02() throws Exception {

        new TestHelper(
            "geo-vary1dim-custom-xyzlltc02",
            "geo-vary1dim-custom-xyzlltc02.rq", 
            "geo-vary1dim.nt",
            "geo-vary1dim-custom-xyzlltc02.srx").runTest();
    }

    /**
     * Vary z only.
     */
    public void testCustomXYZLatLonTimeCoord03() throws Exception {

        new TestHelper(
            "geo-vary1dim-custom-xyzlltc03",
            "geo-vary1dim-custom-xyzlltc03.rq", 
            "geo-vary1dim.nt",
            "geo-vary1dim-custom-xyzlltc03.srx").runTest();
    }

    /**
     * Vary latitude only.
     */
    public void testCustomXYZLatLonTimeCoord04() throws Exception {

        new TestHelper(
            "geo-vary1dim-custom-xyzlltc04",
            "geo-vary1dim-custom-xyzlltc04.rq", 
            "geo-vary1dim.nt",
            "geo-vary1dim-custom-xyzlltc04.srx").runTest();
    }

    /**
     * Vary longitude only.
     */
    public void testCustomXYZLatLonTimeCoord05() throws Exception {

        new TestHelper(
            "geo-vary1dim-custom-xyzlltc05",
            "geo-vary1dim-custom-xyzlltc05.rq", 
            "geo-vary1dim.nt",
            "geo-vary1dim-custom-xyzlltc05.srx").runTest();
    }

    /**
     * Vary time only.
     */
    public void testCustomXYZLatLonTimeCoord06() throws Exception {

        new TestHelper(
            "geo-vary1dim-custom-xyzlltc06",
            "geo-vary1dim-custom-xyzlltc06.rq", 
            "geo-vary1dim.nt",
            "geo-vary1dim-custom-xyzlltc06.srx").runTest();
    }

    /**
     * Vary coordinate system only.
     */
    public void testCustomXYZLatLonTimeCoord07() throws Exception {

        new TestHelper(
            "geo-vary1dim-custom-xyzlltc07",
            "geo-vary1dim-custom-xyzlltc07.rq", 
            "geo-vary1dim.nt",
            "geo-vary1dim-custom-xyzlltc07.srx").runTest();
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

        // set up a datatype containing everything, including a dummy literal serializer
        properties.setProperty(
           com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL_DATATYPE_CONFIG + ".0",
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
