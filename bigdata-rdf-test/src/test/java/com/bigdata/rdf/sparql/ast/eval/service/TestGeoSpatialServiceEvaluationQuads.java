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
public class TestGeoSpatialServiceEvaluationQuads extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestGeoSpatialServiceEvaluationQuads() {
    }

    /**
     * @param name
     */ 
    public TestGeoSpatialServiceEvaluationQuads(String name) {
        super(name);
    }



    /**
     * Verify rectangle search with simple query:
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * 
     * SELECT ?res WHERE {
     *   SERVICE geo:search {
     *     ?res geo:search "inRectangle" .
     *     ?res geo:predicate <http://p> .
     *     ?res geo:spatialRectangleSouthWest "2#2" .
     *     ?res geo:spatialRectangleNorthEast "3#6" .
     *     ?res geo:timeStart "4" .
     *     ?res geo:timeEnd "4" .
     *   }
     * } 
     * 
     * Note that this query does not make use of the context
     * position, but is executed in quads mode.
     */    
    public void testInRectangleQuery01() throws Exception {
       
        if (!store.isQuads()) {
            return;
        }

        new TestHelper(
            "geo-quads-rectangle01",
            "geo-quads-rectangle01.rq", 
            "geo-quads-grid101010.nq",
            "geo-quads-rectangle01.srx").runTest();
       
    }
    
    /**
     * Verify rectangle search with simple query:
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * 
     * SELECT ?res WHERE {
     *   SERVICE geo:search {
     *     ?res geo:search "inRectangle" .
     *     ?res geo:predicate <http://p> .
     *     ?res geo:context <http://c4> .
     *     ?res geo:spatialRectangleSouthWest "2#2" .
     *     ?res geo:spatialRectangleNorthEast "3#6" .
     *     ?res geo:timeStart "4" .
     *     ?res geo:timeEnd "4" .
     *   }
     * } 
     * 
     * This query provides an explicit context restriction.
     * Note that, in the generated data, the context index
     * coincides with the index of the time span, so when
     * restricting to <http://c4> this is essentially the
     * same as setting timeStart=4 and timeEnd=4 (and which
     * thus has no effect in the example above).
     */    
    public void testInRectangleQuery02a() throws Exception {
       
        if (!store.isQuads()) {
            return;
        }

        new TestHelper(
            "geo-quads-rectangle02a",
            "geo-quads-rectangle02a.rq", 
            "geo-quads-grid101010.nq",
            "geo-quads-rectangle02a.srx").runTest();
       
    }
    
    /**
     * Verify rectangle search with simple query:
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * 
     * SELECT ?res WHERE {
     *   SERVICE geo:search {
     *     ?res geo:search "inRectangle" .
     *     ?res geo:predicate <http://p> .
     *     ?res geo:context <http://c5> .
     *     ?res geo:spatialRectangleSouthWest "2#2" .
     *     ?res geo:spatialRectangleNorthEast "3#6" .
     *     ?res geo:timeStart "4" .
     *     ?res geo:timeEnd "4" .
     *   }
     * } 
     * 
     * This query provides an explicit context restriction.
     * Note that, in the generated data, the context index
     * coincides with the index of the time span, so when
     * restricting to <http://c4> this is essentially the
     * same as setting timeStart=4 and timeEnd=4 (and which
     * thus removes all solutions in the example above).
     */    
    public void testInRectangleQuery02b() throws Exception {
       
        if (!store.isQuads()) {
            return;
        }

        new TestHelper(
            "geo-quads-rectangle02b",
            "geo-quads-rectangle02b.rq", 
            "geo-quads-grid101010.nq",
            "geo-quads-rectangle02b.srx").runTest();
       
    }
    
    /**
     * Test query
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * 
     * SELECT * WHERE {
     *   SERVICE geo:search {
     *     ?res geo:search "inCircle" .
     *     ?res geo:predicate <http://p> .
     *     ?res geo:spatialCircleCenter "4#4" .
     *     ?res geo:spatialCircleRadius "1" . #km
     *     ?res geo:timeStart "5" .
     *     ?res geo:timeEnd "7" .
     *   }
     * }
     * 
     * Note that this query does not make use of the context
     * position, but is executed in quads mode.
     * 
     * @throws Exception
     */
    public void testInCircleQuery01() throws Exception {
       
        if (!store.isQuads()) {
            return;
        }

        new TestHelper(
            "geo-quads-circle01",
            "geo-quads-circle01.rq", 
            "geo-quads-grid101010.nq",
            "geo-quads-circle01.srx").runTest();
       
    }

    /**
     * Test query
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * 
     * SELECT * WHERE {
     *   SERVICE geo:search {
     *     ?res geo:search "inCircle" .
     *     ?res geo:predicate <http://p> .
     *     ?res geo:context <http://c6> .
     *     ?res geo:spatialCircleCenter "4#4" .
     *     ?res geo:spatialCircleRadius "1" . #km
     *     ?res geo:timeStart "5" .
     *     ?res geo:timeEnd "7" .
     *   }
     * }
     * 
     * This query provides an explicit context restriction.
     * Note that, in the generated data, the context index
     * coincides with the index of the time span, so when
     * restricting to <http://c6> this is essentially the
     * same as setting timeStart=6 and timeEnd=6.
     * 
     * @throws Exception
     */
    public void testInCircleQuery02() throws Exception {
       
        if (!store.isQuads()) {
            return;
        }

        new TestHelper(
            "geo-quads-circle02",
            "geo-quads-circle02.rq", 
            "geo-quads-grid101010.nq",
            "geo-quads-circle02.srx").runTest();
       
    }
    
    /**
     * Test extraction of index dimensions in quads mode:
     * 
     * SELECT * WHERE {
     *   SERVICE geo:search {
     *     ?res geo:search "inCircle" .
     *     ?res geo:predicate <http://p> .
     *     ?res geo:spatialCircleCenter "4#4" .
     *     ?res geo:spatialCircleRadius "1" . #km
     *     ?res geo:timeStart "5" .
     *     ?res geo:timeEnd "7" .
     *     ?res geo:locationValue ?location .
     *     ?res geo:timeValue ?time .
     *     ?res geo:locationAndTimeValue ?locationAndTime .    
     *   }
     * } 
     * @throws Exception
     */
    public void testValueExtraction() throws Exception {
        
        if (!store.isQuads()) {
            return;
        }
       
        new TestHelper(
            "geo-quads-valueextr",
            "geo-quads-valueextr.rq", 
            "geo-quads-grid101010.nq",
            "geo-quads-valueextr.srx").runTest();
       
    }

    
    @Override
    public Properties getProperties() {

        // Note: clone to avoid modifying!!!
        final Properties properties = (Properties) super.getProperties().clone();

        // turn on quads.
        properties.setProperty(AbstractTripleStore.Options.QUADS, "true");

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
