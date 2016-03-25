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
 * Created on March 02, 2016
 */
package com.bigdata.rdf.sparql.ast.eval.service;

import java.util.Properties;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Data driven test suite for custom serializer, testing basic feasibility 
 * for WKT literals (not strictly following the standard).
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestGeoSpatialCustomSerializerWKT extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestGeoSpatialCustomSerializerWKT() {
    }

    /**
     * @param name
     */ 
    public TestGeoSpatialCustomSerializerWKT(String name) {
        super(name);
    }


    /**
     * Simple rectangle query looking for WKT-style literals.
     */
    public void testWKTLiteral01() throws Exception {
       
       new TestHelper(
          "geo-wktliteral01",
          "geo-wktliteral01.rq", 
          "geo-wktliteral.nt",
          "geo-wktliteral01.srx").runTest();
       
    }
    
    /**
     * Simple rectangle query looking for WKT-style literal, with
     * custom deserialization of geo:locationValue.
     */
    public void testWKTLiteral02() throws Exception {
        
        new TestHelper(
           "geo-wktliteral02",
           "geo-wktliteral02.rq", 
           "geo-wktliteral.nt",
           "geo-wktliteral02.srx").runTest();
        
    }

    /**
     * Simple rectangle query looking for WKT-style literals.
     */
    public void testWKTLiteral03() throws Exception {

        new TestHelper(
            "geo-wktliteral03",
            "geo-wktliteral03.rq", 
            "geo-wktliteral.nt",
            "geo-wktliteral03.srx").runTest();

    }

    /**
     * Simple circle query looking for WKT-style literal, with
     * custom deserialization of geo:locationValue.
     */
    public void testWKTLiteral04() throws Exception {

        new TestHelper(
            "geo-wktliteral04",
            "geo-wktliteral04.rq", 
            "geo-wktliteral.nt",
            "geo-wktliteral04.srx").runTest();

    }
    
    /**
     * Simple circle query looking for WKT-style literal, with
     * custom deserialization of full literal.
     */
    public void testWKTLiteral05() throws Exception {

        new TestHelper(
            "geo-wktliteral05",
            "geo-wktliteral05.rq", 
            "geo-wktliteral.nt",
            "geo-wktliteral05.srx").runTest();

    }
    
    /**
     * Test passing in of WKT point as parameter of geo:spatialCircleCenter.
     */
    public void testWKTLiteral06() throws Exception {

        new TestHelper(
            "geo-wktliteral06",
            "geo-wktliteral06.rq", 
            "geo-wktliteral.nt",
            "geo-wktliteral06.srx").runTest();

    }

    /**
     * Test passing in of WKT point as parameter of geo:spatialRectangleSouthWest
     * and geo:spatialRectangleNorthEast.
     */
    public void testWKTLiteral07() throws Exception {

        new TestHelper(
            "geo-wktliteral07",
            "geo-wktliteral07.rq", 
            "geo-wktliteral.nt",
            "geo-wktliteral07.srx").runTest();

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

        // set GeoSpatial configuration: use a higher precision and range shifts; 
        // the test accounts for this higher precision (and assert that range shifts
        // actually do not harm the evaluation process)
        properties.setProperty(
           com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL_DATATYPE_CONFIG + ".0",
           "{\"config\": "
           + "{ \"uri\": \"http://www.opengis.net/ont/geosparql#wktLiteral\", "
           + "\"literalSerializer\": \"com.bigdata.rdf.sparql.ast.eval.service.GeoSpatialTestWKTLiteralSerializer\",  "
           + "\"fields\": [ "
           + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"1000000\", \"serviceMapping\": \"LATITUDE\" }, "
           + "{ \"valueType\": \"DOUBLE\", \"minVal\" : \"0\", \"multiplier\": \"100000\", \"serviceMapping\": \"LONGITUDE\" } "
           + "]}}");
        
        // make our dummy WKT datatype default to ease querying
        properties.setProperty(
            com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL_DEFAULT_DATATYPE, 
            "http://www.opengis.net/ont/geosparql#wktLiteral");
        
        properties.setProperty(
           com.bigdata.rdf.store.AbstractLocalTripleStore.Options.VOCABULARY_CLASS,
           "com.bigdata.rdf.sparql.ast.eval.service.GeoSpatialTestVocabulary");
        
        return properties;

    }
}
