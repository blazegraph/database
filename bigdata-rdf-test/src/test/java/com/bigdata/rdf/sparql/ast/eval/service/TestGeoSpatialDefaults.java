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
 * Tests covering the geospatial default configuration, i.e. that the default datatypes
 * are properly registered on startup, and the defaults for the geospatial service
 * configuration are properly taken over.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestGeoSpatialDefaults extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestGeoSpatialDefaults() {
    }

    /**
     * @param name
     */ 
    public TestGeoSpatialDefaults(String name) {
        super(name);
    }
    
    /**
     * Query against default datatype explicitly naming default.
     */
    public void testDefaultDatatypeExplicit() throws Exception {

        new TestHelper(
            "geo-defaults01",
            "geo-defaults01.rq", 
            "geo-defaults.nt",
            "geo-defaults0102.srx").runTest();
        
    }

    /**
     * Query against default datatype explicitly not naming default.
     */
    public void testDefaultDatatypeImplicit() throws Exception {

        new TestHelper(
            "geo-defaults02",
            "geo-defaults02.rq", 
            "geo-defaults.nt",
            "geo-defaults0102.srx").runTest();
    }
    
    /**
     * Non-geospatial ?s ?p ?o query retrieving ?s
     * @throws Exception
     */
    public void testNonGeoSpatialQuery01() throws Exception {

        new TestHelper(
            "geo-defaults03",
            "geo-defaults03.rq", 
            "geo-defaults.nt",
            "geo-defaults03.srx").runTest();
    }
    
    /**
     * Non-geospatial ?s ?p ?o query with FILTER retrieving ?s and ?o
     * @throws Exception
     */
    public void testNonGeoSpatialQuery02() throws Exception {

        new TestHelper(
            "geo-defaults04",
            "geo-defaults04.rq", 
            "geo-defaults.nt",
            "geo-defaults04.srx").runTest();
    }


    public void testNonDefaultDatatype() throws Exception {

        new TestHelper(
            "geo-defaults05",
            "geo-defaults05.rq", 
            "geo-defaults.nt",
            "geo-defaults05.srx").runTest();
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
        properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE, BufferMode.Transient.toString());

        // enable GeoSpatial index with default configuration
        properties.setProperty(com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL, "true");

        properties.setProperty(
            com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL_DEFAULT_DATATYPE,
            "http://www.bigdata.com/rdf/geospatial/literals/v1#lat-lon");
        
        return properties;

    }
}
