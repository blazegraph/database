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
 * Created on March 20, 2016
 */
package com.bigdata.rdf.sparql.ast.eval.service;

import java.util.Properties;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Data driven test suite asserting that the examples in the geospatial
 * documentation are running through properly.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestGeoSpatialExamplesFromDocumentation extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestGeoSpatialExamplesFromDocumentation() {
    }

    /**
     * @param name
     */ 
    public TestGeoSpatialExamplesFromDocumentation(String name) {
        super(name);
    }
    
    /**
     * Built-in datatype example
     */
    public void testDocumentationBuiltin01() throws Exception {
        
        new TestHelper(
           "geo-documentation-builtin01",
           "geo-documentation-builtin01.rq", 
           "geo-documentation.ttl",
           "geo-documentation-builtin01.srx").runTest();
        
    }

    /**
     * Built-in datatype example
     */
    public void testDocumentationBuiltin02() throws Exception {
        
        new TestHelper(
           "geo-documentation-builtin02",
           "geo-documentation-builtin02.rq", 
           "geo-documentation.ttl",
           "geo-documentation-builtin02.srx").runTest();
        
    }

    /**
     * Built-in datatype example
     */
    public void testDocumentationBuiltin03() throws Exception {
        
        new TestHelper(
           "geo-documentation-builtin03",
           "geo-documentation-builtin03.rq", 
           "geo-documentation.ttl",
           "geo-documentation-builtin03.srx").runTest();
        
    }

    /**
     * Built-in datatype example
     */
    public void testDocumentationBuiltin04() throws Exception {
        
        new TestHelper(
           "geo-documentation-builtin04",
           "geo-documentation-builtin04.rq", 
           "geo-documentation.ttl",
           "geo-documentation-builtin04.srx").runTest();
        
    }

    /**
     * latitude-longitude-starttime-endtime example from documentation
     */
    public void testDocumentationCustomLLTT01() throws Exception {
        
        new TestHelper(
           "geo-documentation-custom-lltt01",
           "geo-documentation-custom-lltt01.rq", 
           "geo-documentation.ttl",
           "geo-documentation-custom-lltt01.srx").runTest();
        
    }

    /**
     * width-height-length datatype example from documentation
     */
    public void testDocumentationCustomWHL01() throws Exception {
        
        new TestHelper(
           "geo-documentation-custom-whl01",
           "geo-documentation-custom-whl01.rq", 
           "geo-documentation.ttl",
           "geo-documentation-custom-whl01.srx").runTest();
        
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
           "{ \"config\": "
           + "{ \"uri\": \"http://my-lat-lon-starttime-endtime-dt\", "
           + "\"fields\": [ "
           + "{ \"valueType\": \"DOUBLE\", \"multiplier\": \"100000\", \"serviceMapping\": \"LATITUDE\" },"
           + "{ \"valueType\": \"DOUBLE\", \"multiplier\": \"100000\", \"serviceMapping\": \"LONGITUDE\" }, "
           + "{ \"valueType\": \"LONG\", \"serviceMapping\": \"starttime\" }, "
           + "{ \"valueType\": \"LONG\", \"serviceMapping\": \"endtime\" } ] } }");

        properties.setProperty(
           com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL_DATATYPE_CONFIG + ".1",
           "{ \"config\": { "
           + "\"uri\": \"http://width-height-length-dt\", "
           + "\"fields\": [ "
           + "{ \"valueType\": \"LONG\", \"serviceMapping\": \"width\" }, "
           + "{ \"valueType\": \"LONG\", \"serviceMapping\": \"height\" }, "
           + "{ \"valueType\": \"LONG\", \"serviceMapping\": \"length\" } ] } }");

        properties.setProperty(
                com.bigdata.rdf.store.AbstractLocalTripleStore.Options.VOCABULARY_CLASS,
                "com.bigdata.rdf.sparql.ast.eval.service.GeoSpatialTestVocabulary");

        return properties;

    }
}
