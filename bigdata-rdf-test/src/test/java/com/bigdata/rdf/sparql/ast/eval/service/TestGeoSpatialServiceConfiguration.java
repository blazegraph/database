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
 * Data driven test suite testing configurability of GeoSpatial service.
 * The query set is a subset of queries from {@link TestGeoSpatialServiceEvaluation},
 * with possible modifications to account for the changed configuration.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestGeoSpatialServiceConfiguration extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestGeoSpatialServiceConfiguration() {
    }

    /**
     * @param name
     */ 
    public TestGeoSpatialServiceConfiguration(String name) {
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
     */    
    public void testInRectangleQuery01() throws Exception {
       
       new TestHelper(
          "geo-rectangle01",
          "geo-rectangle01.rq", 
          "geo-grid101010.nt",
          "geo-rectangle01.srx").runTest();
       
    }
    
    /**
     * Compared to the circle01* queries, the query
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * 
     * SELECT * WHERE {
     *   SERVICE geo:search {
     *     ?res geo:search "inCircle" .
     *     ?res geo:predicate <http://p> .
     *     ?res geo:spatialCircleCenter "4#4" .
     *     ?res geo:spatialCircleRadius "112" . #km
     *     ?res geo:timeStart "5" .
     *     ?res geo:timeEnd "5" .
     *   }
     * } 
     * 
     * extends the radius such that the point's neighbors in the east, west,
     * south, and nord are matched now.
     * 
     * @throws Exception
     */
    public void testInCircleQuery02() throws Exception {
       
       new TestHelper(
          "geo-circle02",
          "geo-circle02.rq", 
          "geo-grid101010.nt",
          "geo-circle0203.srx").runTest();
       
    }
    
    /**
     * Verify precision handling, which is changed in the configuration to be 6 for
     * the first component and 5 for the second one. See 
     * {@link TestGeoSpatialServiceEvaluation#testInRectangleQuery08()} for baseline.
     * 
     * The query
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * SELECT ?res ?o WHERE {
     *   SERVICE geo:search {
     *     ?res geo:search "inRectangle" .
     *     ?res geo:predicate <http://p> .
     *     ?res geo:spatialRectangleSouthWest "1.10#1.100000001111" .
     *     ?res geo:spatialRectangleNorthEast "6.666666#7" .
     *     ?res geo:timeStart "0" .
     *     ?res geo:timeEnd "0" .
     *   }
     *   ?res ?p ?o .
     * }
     * 
     * is evaluated over data
     * 
     * <http://s0> <http://p> "0#0#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s1> <http://p> "1.1#1.1#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s2> <http://p> "2.22#2.22#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s3> <http://p> "3.333#3.333#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s4> <http://p> "4.4444#4.4444#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s5> <http://p> "5.55555#5.55555#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s6> <http://p> "6.666666#6.6666666#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s6b> <http://p> "6.66667#6.6666666#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s6c> <http://p> "6.66666#6.66667#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s6c> <http://p> "6.66667#6.66667#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * 
     * With the given precision, the query is equivalent to
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * SELECT ?res ?o WHERE {
     *   SERVICE geo:search {
     *     ?res geo:search "inRectangle" .
     *     ?res geo:predicate <http://p> .
     *     ?res geo:spatialRectangleSouthWest "1.10#1.1" .
     *     ?res geo:spatialRectangleNorthEast "6.666666#7" .
     *     ?res geo:timeStart "0" .
     *     ?res geo:timeEnd "0" .
     *   }
     *   ?res ?p ?o .
     * }
     * 
     * and the data is equivalent to 
     * 
     * <http://s0> <http://p> "0#0#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s1> <http://p> "1.1#1.1#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s2> <http://p> "2.22#2.22#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s3> <http://p> "3.333#3.333#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s4> <http://p> "4.4444#4.4444#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s5> <http://p> "5.55555#5.55555#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s6> <http://p> "6.666666#6.666666#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s6b> <http://p> "6.66667#6.666666#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s6c> <http://p> "6.66666#6.66667#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s6d> <http://p> "6.66667#6.66667#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     *  
     * Consequently, only subjects s0 and s6a, s6b, and s6d are *not* contained in the
     * result, while all others are.
     */
    public void testInRectangleQuery08mod() throws Exception {
       
       new TestHelper(
          "geo-rectangle08",
          "geo-rectangle08mod.rq", 
          "geo-rectangle08.nt",
          "geo-rectangle08mod.srx").runTest();
       
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
        // the test account for this higher precision (and assert that range shifts
        // actually do not harm the evaluation process)
        properties.setProperty(
           com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL_CONFIG, 
           "DOUBLE#1000000#0;DOUBLE#100000#0;LONG#1#0");


        return properties;

    }
}
