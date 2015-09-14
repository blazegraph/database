/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on September 10, 2015
 */
package com.bigdata.rdf.sparql.ast.eval.service;

import java.util.Properties;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Data driven test suite for GeoSpatial service feature, GeoSpatial in
 * triples vs. quads mode, testing of different service configurations,
 * as well as correctness of the GeoSpatial service itself.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestGeoSpatialServiceEvaluation extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestGeoSpatialServiceEvaluation() {
    }

    /**
     * @param name
     */ 
    public TestGeoSpatialServiceEvaluation(String name) {
        super(name);
    }


    /**
     * Verify rectangle search with simple query:
     * 
     * 
     */
    public void testInRectangleQuery01() throws Exception {
       
       new TestHelper(
          "geo-rectangle01",
          "geo-rectangle01.rq", 
          "geo-grid101010.nt",
          "geo-rectangle01.srx").runTest();
       
    }
    
    /**
     * Verify rectangle search with simple query:
     * 
     * 
     */
    public void testInRectangleQuery02() throws Exception {
       
       new TestHelper(
          "geo-rectangle02",
          "geo-rectangle02.rq", 
          "geo-grid101010.nt",
          "geo-rectangle02.srx").runTest();
       
    }

    /**
     * Verify rectangle search with simple query:
     * 
     * 
     */
    public void testInRectangleQuery03() throws Exception {
       
       new TestHelper(
          "geo-rectangle03",
          "geo-rectangle03.rq", 
          "geo-grid101010.nt",
          "geo-rectangle030405.srx").runTest();
       
    }

    /**
     * Verify rectangle search with simple query:
     * 
     * 
     */
    public void testInRectangleQuery04() throws Exception {
       
       new TestHelper(
          "geo-rectangle04",
          "geo-rectangle04.rq", 
          "geo-grid101010.nt",
          "geo-rectangle030405.srx").runTest();
       
    }

    /**
     * Verify rectangle search with simple query:
     * 
     * 
     */
    public void testInRectangleQuery05() throws Exception {
       
       new TestHelper(
          "geo-rectangle05",
          "geo-rectangle05.rq", 
          "geo-grid101010.nt",
          "geo-rectangle030405.srx").runTest();
       
    }
    
    @Override
    public Properties getProperties() {

        // Note: clone to avoid modifying!!!
        final Properties properties = (Properties) super.getProperties().clone();

        // turn on quads.
        // TODO: enable quads at some point
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

        return properties;

    }
}