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
 * Created on September 18, 2015
 */
package com.bigdata.rdf.sparql.ast.eval.service;

import java.util.Properties;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Data driven test suite for GeoSpatial service feature in quads mode,
 * testing of different service configurations,
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
     * Verify rectangle search with simple query:
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * 
     * SELECT ?res WHERE {
     *   SERVICE geo:search {
     *     ?res geo:search "inRectangle" .
     *     ?res geo:predicate <http://p> .
     *     ?res geo:spatialRectangleSouthWest "5#5" .
     *     ?res geo:spatialRectangleNorthEast "5#5" .
     *     ?res geo:timeStart "1" .
     *     ?res geo:timeEnd "10" .
     *   }
     * } 
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
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * 
     * SELECT ?res WHERE {
     *   SERVICE geo:search {
     *     ?res geo:search "inRectangle" .
     *     ?res geo:predicate <http://p> .
     *     ?res geo:spatialRectangleSouthWest "-1#9" .
     *     ?res geo:spatialRectangleNorthEast "1#12" .
     *     ?res geo:timeStart "7" .
     *     ?res geo:timeEnd "12" .
     *   }
     * } 
     */
    public void testInRectangleQuery03() throws Exception {
       
       new TestHelper(
          "geo-rectangle03",
          "geo-rectangle03.rq", 
          "geo-grid101010.nt",
          "geo-rectangle03040506.srx").runTest();
       
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
     *     ?res geo:spatialRectangleSouthWest "-90#8.8837" .
     *     ?res geo:spatialRectangleNorthEast "1.00#12.2344" .
     *     ?res geo:timeStart "7" .
     *     ?res geo:timeEnd "100000000000000" .
     *   }
     * } 
     */
    public void testInRectangleQuery04() throws Exception {
       
       new TestHelper(
          "geo-rectangle04",
          "geo-rectangle04.rq", 
          "geo-grid101010.nt",
          "geo-rectangle03040506.srx").runTest();
       
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
     *     ?res geo:spatialRectangleSouthWest "-90E0#.88837E1" .
     *     ?res geo:spatialRectangleNorthEast "1.00#179.2344" .
     *     ?res geo:timeStart "7" .
     *     ?res geo:timeEnd "1000000" .
     *   }
     * } 
     */
    public void testInRectangleQuery05() throws Exception {
       
       new TestHelper(
          "geo-rectangle05",
          "geo-rectangle05.rq", 
          "geo-grid101010.nt",
          "geo-rectangle03040506.srx").runTest();
       
    }
    

    /**
     * Verify rectangle search with simple query (not wrapped into service):
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * 
     * SELECT ?res WHERE {
     *   ?res geo:search "inRectangle" .
     *   ?res geo:predicate <http://p> .
     *   ?res geo:spatialRectangleSouthWest "-90E0#.88837E1" .
     *   ?res geo:spatialRectangleNorthEast "1.00#179.2344" .
     *   ?res geo:timeStart "7" .
     *   ?res geo:timeEnd "1000000" .
     * } 
     */
    public void testInRectangleQuery06() throws Exception {
       
       new TestHelper(
          "geo-rectangle06",
          "geo-rectangle06.rq", 
          "geo-grid101010.nt",
          "geo-rectangle03040506.srx").runTest();
       
    }
    
    /**
     * Verify rectangle search with simple query and subsequent join of result.
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
     *   ?res ?p ?o .
     *   FILTER(?p = <http://p>)
     * } 
     */
    public void testInRectangleQuery07() throws Exception {
       
       new TestHelper(
          "geo-rectangle07",
          "geo-rectangle07.rq", 
          "geo-grid101010.nt",
          "geo-rectangle07.srx").runTest();
       
    }
    
    
    /**
     * Verify precision handling. The query
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
     * Since the geospatial datatype has precision=5 (in our standard datatype), 
     * the query is equivalent to
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * SELECT ?res ?o WHERE {
     *   SERVICE geo:search {
     *     ?res geo:search "inRectangle" .
     *     ?res geo:predicate <http://p> .
     *     ?res geo:spatialRectangleSouthWest "1.10#1.1" .
     *     ?res geo:spatialRectangleNorthEast "6.66666#7" .
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
     * <http://s6> <http://p> "6.66666#6.666666#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s6b> <http://p> "6.66667#6.666666#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s6c> <http://p> "6.66666#6.66667#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     * <http://s6d> <http://p> "6.66667#6.66667#0"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral> .
     *  
     * Consequently, subjects s0, s6b, and s6d are *not* contained in the
     * result, while all others are.
     */
    public void testInRectangleQuery08() throws Exception {
       
       new TestHelper(
          "geo-rectangle08",
          "geo-rectangle08.rq", 
          "geo-rectangle08.nt",
          "geo-rectangle08.srx").runTest();
       
    }
    
    /**
     * Verify rectangle search with simple:
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * 
     * SELECT ?res WHERE {
     *   SERVICE geo:search {
     *     ?res geo:search "inRectangle" .
     *     ?res geo:predicate <http://p> .
     *     ?res geo:spatialRectangleSouthWest "4#4" .
     *     ?res geo:spatialRectangleNorthEast "5#5" .
     *     ?res geo:timeStart "1" .
     *     ?res geo:timeEnd "10" .
     *   }
     *   hint:Prior <http://www.bigdata.com/queryHints#maxParallel> "10" .
     *   hint:Prior <http://www.bigdata.com/queryHints#com.bigdata.relation.accesspath.BlockingBuffer.chunkOfChunksCapacity> "10" .
     *   hint:Prior <http://www.bigdata.com/queryHints#com.bigdata.relation.accesspath.IBuffer.chunkCapacity> "100" .
     *   hint:Prior <http://www.bigdata.com/queryHints#com.bigdata.bop.join.PipelineJoin.avgDataPointsPerThread> "10" .
     * } 
     * 
     * which implies setup of parallel threads for data extraction.
     */
    public void testRectangleQueryParallel01() throws Exception {
       
       final TestHelper helper = new TestHelper(
          "geo-rectangle-parallel01",
          "geo-rectangle-parallel01.rq", 
          "geo-grid101010.nt",
          "geo-rectangle-parallel.srx");
       
       helper.runTest();
    }
    
    /**
     * Variant of testRectangleQueryParallel01.
     */
    public void testRectangleQueryParallel02() throws Exception {
       
       final TestHelper helper = new TestHelper(
          "geo-rectangle-parallel02",
          "geo-rectangle-parallel02.rq", 
          "geo-grid101010.nt",
          "geo-rectangle-parallel.srx");
       
       helper.runTest();
    }
    
    /**
     * Variant of testRectangleQueryParallel01.
     */
    public void testRectangleQueryParallel03() throws Exception {
       
       final TestHelper helper = new TestHelper(
          "geo-rectangle-parallel03",
          "geo-rectangle-parallel03.rq", 
          "geo-grid101010.nt",
          "geo-rectangle-parallel.srx");
       
       helper.runTest();
    }
    
    /**
     * Variant of testRectangleQueryParallel01.
     */
    public void testRectangleQueryParallel04() throws Exception {
       
       final TestHelper helper = new TestHelper(
          "geo-rectangle-parallel04",
          "geo-rectangle-parallel04.rq", 
          "geo-grid101010.nt",
          "geo-rectangle-parallel.srx");
       
       helper.runTest();
    }
    
    /**
     * Variant of testRectangleQueryParallel01.
     */
    public void testRectangleQueryParallel05() throws Exception {
       
       final TestHelper helper = new TestHelper(
          "geo-rectangle-parallel05",
          "geo-rectangle-parallel05.rq", 
          "geo-grid101010.nt",
          "geo-rectangle-parallel.srx");
       
       helper.runTest();
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
     * , which extracts the center point 4#4 with three timestamps.
     * 
     * @throws Exception
     */
    public void testInCircleQuery01a() throws Exception {
       
       new TestHelper(
          "geo-circle01a",
          "geo-circle01a.rq", 
          "geo-grid101010.nt",
          "geo-circle01.srx").runTest();
       
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
     *     ?res geo:spatialCircleRadius "105" . #km
     *     ?res geo:timeStart "5" .
     *     ?res geo:timeEnd "7" .
     *   }
     * } 
     * 
     * , which extracts the center point 4#4 with three timestamps (105km
     * is still too small to match any points of 1 lat/lon distance in the grid)
     * 
     * @throws Exception
     */    
    public void testInCircleQuery01b() throws Exception {
       
       new TestHelper(
          "geo-circle01b",
          "geo-circle01b.rq", 
          "geo-grid101010.nt",
          "geo-circle01.srx").runTest();
       
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
     *     ?res geo:spatialCircleRadius "65" . 
     *     ?res geo:spatialUnit "Miles" .
     *     ?res geo:timeStart "5" .
     *     ?res geo:timeEnd "7" .
     *   }
     * } 
     * 
     * , which is the same as the circle01b, just using a (roughly) equivalent
     * value specified in miles rather than kilometers.
     * 
     * @throws Exception
     */ 
    public void testInCircleQuery01c() throws Exception {
       
       new TestHelper(
          "geo-circle01c",
          "geo-circle01c.rq", 
          "geo-grid101010.nt",
          "geo-circle01.srx").runTest();
       
    }
    
    /**
     * Test query
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * 
     * SELECT * WHERE {
     *   ?res geo:search "inCircle" .
     *   ?res geo:predicate <http://p> .
     *   ?res geo:spatialCircleCenter "4#4" .
     *   ?res geo:spatialCircleRadius "1" . #km
     *   ?res geo:timeStart "5" .
     *   ?res geo:timeEnd "7" .
     * }
     * 
     * , which is a variante of circle01a just not wrapped into a SERVICE.
     * 
     * @throws Exception
     */
    public void testInCircleQuery01d() throws Exception {
       
       new TestHelper(
          "geo-circle01d",
          "geo-circle01d.rq", 
          "geo-grid101010.nt",
          "geo-circle01.srx").runTest();
       
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
     * A variant of circle02 where the unit is specified in miles rather
     * than kilometers (delivering the same result).
     */
    public void testInCircleQuery03() throws Exception {
       
       new TestHelper(
          "geo-circle03",
          "geo-circle03.rq", 
          "geo-grid101010.nt",
          "geo-circle0203.srx").runTest();
       
    }
    
    /**
     * Compared to queries circle02 and circle03, the query
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * 
     * SELECT * WHERE {
     *   SERVICE geo:search {
     *     ?res geo:search "inCircle" .
     *     ?res geo:predicate <http://p> .
     *     ?res geo:spatialCircleCenter "4#4" .
     *     ?res geo:spatialCircleRadius "190" . #km
     *     ?res geo:timeStart "5" .
     *     ?res geo:timeEnd "5" .
     *   }
     * } 
     * 
     * extends the radius such that the point's neighbors in the south-east,
     * sout-west, north-east, and north-west are included now as well.
     * @throws Exception
     */
    public void testInCircleQuery04() throws Exception {
       
       new TestHelper(
          "geo-circle04",
          "geo-circle04.rq", 
          "geo-grid101010.nt",
          "geo-circle04.srx").runTest();
       
    }
    
    /**
     *  Compared to queries circle04, the query
     *  
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * 
     * SELECT * WHERE {
     *   SERVICE geo:search {
     *     ?res geo:search "inCircle" .
     *     ?res geo:predicate <http://p> .
     *     ?res geo:spatialCircleCenter "4#4" .
     *     ?res geo:spatialCircleRadius "240000" . 
     *     ?res geo:spatialUnit "Meters" .
     *     ?res geo:timeStart "5" .
     *     ?res geo:timeEnd "5" .
     *   }
     * }
     * 
     * further extends the range such that the next set of eastern, western,
     * southern, and northern points are matched.
     */
    public void testInCircleQuery05() throws Exception {
       
       new TestHelper(
          "geo-circle05",
          "geo-circle05.rq", 
          "geo-grid101010.nt",
          "geo-circle05.srx").runTest();
       
    }
    
    /**
     * Query similar in spirit to circle04, but settled at the corner of our
     * gred (top left):
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * 
     * SELECT * WHERE {
     *   SERVICE geo:search {
     *     ?res geo:search "inCircle" .
     *     ?res geo:predicate <http://p> .
     *     ?res geo:spatialCircleCenter "1#1" .
     *     ?res geo:spatialCircleRadius "240" . 
     *     ?res geo:timeStart "5" .
     *     ?res geo:timeEnd "5" .
     *   }
     * }
     * 
     * @throws Exception
     */
    public void testInCircleQuery06a() throws Exception {
       
       new TestHelper(
          "geo-circle06a",
          "geo-circle06a.rq", 
          "geo-grid101010.nt",
          "geo-circle06a.srx").runTest();
       
    }

    /**
     * Query similar in spirit to circle06a, but top-right corner.
     * 
     * @throws Exception
     */
    public void testInCircleQuery06b() throws Exception {
       
       new TestHelper(
          "geo-circle06b",
          "geo-circle06b.rq", 
          "geo-grid101010.nt",
          "geo-circle06b.srx").runTest();
       
    }

    /**
     * Query similar in spirit to circle06a, but lower-left corner.
     * 
     * @throws Exception
     */
    public void testInCircleQuery06c() throws Exception {
       
       new TestHelper(
          "geo-circle06c",
          "geo-circle06c.rq", 
          "geo-grid101010.nt",
          "geo-circle06c.srx").runTest();
       
    }

    /**
     * Query similar in spirit to circle06a, but lower-right corner.
     * 
     * @throws Exception
     */
    public void testInCircleQuery06d() throws Exception {
       
       new TestHelper(
          "geo-circle06d",
          "geo-circle06d.rq", 
          "geo-grid101010.nt",
          "geo-circle06d.srx").runTest();
       
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
     *     ?res geo:spatialCircleRadius "240000" . 
     *     ?res geo:spatialUnit "Meters" .
     *     ?res geo:timeStart "5" .
     *     ?res geo:timeEnd "5" .
     *   }
     *   hint:Prior <http://www.bigdata.com/queryHints#maxParallel> "5" .
     *   hint:Prior <http://www.bigdata.com/queryHints#com.bigdata.relation.accesspath.BlockingBuffer.chunkOfChunksCapacity> "5" .
     *   hint:Prior <http://www.bigdata.com/queryHints#com.bigdata.relation.accesspath.IBuffer.chunkCapacity> "10" .
     *   hint:Prior <http://www.bigdata.com/queryHints#com.bigdata.bop.join.PipelineJoin.avgDataPointsPerThread> "3" .  
     * } 
     * 
     * , which is the same as circle05 just executed with parallel processing enabled.
     * 
     * @throws Exception
     */
    public void testInCircleQueryParallel01() throws Exception {
       
       final TestHelper helper = new TestHelper(
          "geo-circle-parallel01",
          "geo-circle-parallel01.rq", 
          "geo-grid101010.nt",
          "geo-circle-parallel01.srx");
       
       helper.runTest();
    }

    /**
     * Verify that location value is properly extracted.
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
     *     ?res geo:locationValue ?location .
     *   }
     * } 
     */    
    public void testDimensionValueExtracion01() throws Exception {
       
       new TestHelper(
          "geo-valueextr",
          "geo-valueextr01.rq", 
          "geo-grid101010.nt",
          "geo-valueextr01.srx").runTest();
       
    }
    
    /**
     * Verify that time value is properly extracted.
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
     *     ?res geo:timeValue ?time .
     *   }
     * } 
     */    
    public void testDimensionValueExtracion02() throws Exception {
       
       new TestHelper(
          "geo-valueextr",
          "geo-valueextr02.rq", 
          "geo-grid101010.nt",
          "geo-valueextr02.srx").runTest();
       
    }
    
    /**
     * Verify that location + time value is properly extracted.
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
     *     ?res geo:locationAndTimeValue ?locationAndTime .    
     *   }
     * } 
     */    
    public void testDimensionValueExtracion03() throws Exception {
       
       new TestHelper(
          "geo-valueextr",
          "geo-valueextr03.rq", 
          "geo-grid101010.nt",
          "geo-valueextr03.srx").runTest();
       
    }
    
    
    /**
     * Verify that all dimension values are extracted properly
     * when respective output variables are present
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
     *     ?res geo:locationValue ?location .
     *     ?res geo:timeValue ?time .
     *     ?res geo:locationAndTimeValue ?locationAndTime .    
     *   }
     * } 
     */    
    public void testDimensionValueExtracion04() throws Exception {
       
       new TestHelper(
          "geo-valueextr",
          "geo-valueextr04.rq", 
          "geo-grid101010.nt",
          "geo-valueextr04.srx").runTest();
       
    }
    
    /**
     * Verify that a circle query with illegal center point latitude
     * value fails.
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * 
     * SERVICE geo:search {
     *   ?res geo:search "inCircle" .
     *   ?res geo:predicate <http://p> .
     *   ?res geo:spatialCircleCenter "-91#0.883" .
     *   ?res geo:spatialCircleRadius "10000000" . # spans all earth
     *   ?res geo:timeStart "0" .
     *   ?res geo:timeEnd "0" .
     * }
     */
    public void testCircleOutOfBounds01a() throws Exception {
       
       try {
          new TestHelper(
             "geo-circle-outofbounds1a",
             "geo-circle-outofbounds1a.rq", 
             "geo-small.nt",
             "geo-circle-outofbounds1a.srx").runTest();
       } catch (Exception e) {
          
          // error message is saying that "NortSouth" coordinates 
          // are out of range
          assertTrue(e.getMessage().contains("NorthSouth"));
          
          return; // we should follow this code path
       }
       
       throw new RuntimeException("Expected to run into exception. Test case failed.");
    }
    
    /**
     * Verify that a circle query with illegal center point longitude
     * value fails.
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * 
     * SERVICE geo:search {
     *   ?res geo:search "inCircle" .
     *   ?res geo:predicate <http://p> .
     *   ?res geo:spatialCircleCenter "27.9932#182" .
     *   ?res geo:spatialCircleRadius "10000000" . # spans all earth
     *   ?res geo:timeStart "0" .
     *   ?res geo:timeEnd "0" .
     * }
     */
    public void testCircleOutOfBounds01b() throws Exception {
       
       try {

          new TestHelper(
                "geo-circle-outofbounds1b",
                "geo-circle-outofbounds1b.rq", 
                "geo-small.nt",
                "geo-circle-outofbounds1b.srx").runTest();
       } catch (Exception e) {
          
          // error message is saying that "EastWest" coordinates 
          // are out of range
          assertTrue(e.getMessage().contains("EastWest"));
          
          return; // we should follow this code path

       }
       
       throw new RuntimeException("Expected to run into exception. Test case failed.");

    }

    /**
     * 
     * Verify that a query that spans over the max lat + lon
     * values does not fail, but yields all data in the database.
     * 
     * PREFIX geo: <http://www.bigdata.com/rdf/geospatial#>
     * 
     * SERVICE geo:search {
     *   ?res geo:search "inCircle" .
     *   ?res geo:predicate <http://p> .
     *   ?res geo:spatialCircleCenter "27.9932#39.928" .
     *   ?res geo:spatialCircleRadius "10000000" . # spans all earth
     *   ?res geo:timeStart "0" .
     *   ?res geo:timeEnd "0" .
     * }
     */
    public void testCircleOutOfBounds02() throws Exception {
       
       new TestHelper(
             "geo-circle-outofbounds2",
             "geo-circle-outofbounds2.rq", 
             "geo-small.nt",
             "geo-circle-outofbounds2.srx").runTest();
    }
    
    
    /**
     * Assert failing in case of invalid rectangle coordinates (in upper left).
     * 
     * @throws Exception
     */
    public void testRectangleOutOfBounds01a() throws Exception {
       
       try {
          new TestHelper(
             "geo-rectangle-outofbounds1a",
             "geo-rectangle-outofbounds1a.rq", 
             "geo-small.nt",
             "geo-rectangle-outofbounds1a.srx").runTest();
       } catch (Exception e) {
          
          // error message is saying that "NortSouth" coordinates 
          // are out of range
          assertTrue(e.getMessage().contains("NorthSouth"));
          
          return; // we should follow this code path
       }
       
       throw new RuntimeException("Expected to run into exception. Test case failed.");
    }
    
    /**
     * Assert failing in case of invalid rectangle coordinates (in lower right).
     * 
     * @throws Exception
     */
    public void testRectangleOutOfBounds01b() throws Exception {
       
       try {
          new TestHelper(
             "geo-rectangle-outofbounds1b",
             "geo-rectangle-outofbounds1b.rq", 
             "geo-small.nt",
             "geo-rectangle-outofbounds1b.srx").runTest();
       } catch (Exception e) {
          
          // error message is saying that "EathWest" coordinates 
          // are out of range
          assertTrue(e.getMessage().contains("EastWest"));
          
          return; // we should follow this code path
       }
       
       
       throw new RuntimeException("Expected to run into exception. Test case failed.");
    }
    
    /**
     * Real world test against geo coordinates of few cities.
     * 
     * @throws Exception
     */
    public void testRealWordCircle01() throws Exception {
       
       new TestHelper(
             "geo-realworld-circle01",
             "geo-realworld-circle01.rq", 
             "geo-realworld-cities.nt",
             "geo-realworld-circle01.srx").runTest();
    }

    /**
     * Real world test against geo coordinates of few cities.
     * 
     * @throws Exception
     */
    public void testRealWordCircle02() throws Exception {
       
       new TestHelper(
             "geo-realworld-circle02",
             "geo-realworld-circle02.rq", 
             "geo-realworld-cities.nt",
             "geo-realworld-circle02.srx").runTest();
    }
    
    /**
     * Real world test against geo coordinates of few cities,
     * including distance calculation.
     * 
     * @throws Exception
     */
    public void testRealWordCircleWithDistance01() throws Exception {
       
       new TestHelper(
             "geo-realworld-circlewithdistance01",
             "geo-realworld-circlewithdistance01.rq", 
             "geo-realworld-cities.nt",
             "geo-realworld-circlewithdistance01.srx").runTest();
    }
    
    /**
     * Real world test against geo coordinates of few cities,
     * including distance calculation and ORDER BY over distances.
     * 
     * @throws Exception
     */
    public void testRealWordCircleWithDistance02() throws Exception {
       
       new TestHelper(
             "geo-realworld-circlewithdistance02",
             "geo-realworld-circlewithdistance02.rq", 
             "geo-realworld-cities.nt",
             "geo-realworld-circlewithdistance02.srx").runTest();
    }

    /**
     * Real world test against geo coordinates of few cities.
     * 
     * @throws Exception
     */
    public void testRealWordRectangle01() throws Exception {
       
       new TestHelper(
             "geo-realworld-rectangle01",
             "geo-realworld-rectangle01.rq", 
             "geo-realworld-cities.nt",
             "geo-realworld-rectangle01.srx").runTest();       
    }

    /**
     * Rectangle test q query on grid ranging from [-179;180] longitude
     * and [-5;5] latitude and fixed "0" timestamp, aiming
     * covering edge cases overlapping the -180 and +180
     * longitude values.
     * 
     * @throws Exception
     */
    public void testGrid180x5vRectangle01() throws Exception {
        new TestHelper(
            "geo-grid180-rectangle01",
            "geo-grid180-rectangle01.rq", 
            "geo-grid180x5.n3",
            "geo-grid180-rectangle01.srx").runTest();     
    }

    
    /**
     * Rectangle test query on grid ranging from [-179;180] longitude
     * and [-5;5] latitude and fixed "0" timestamp, aiming
     * covering edge cases overlapping the -180 and +180
     * longitude values.
     * 
     * @throws Exception
     */
    public void testGrid180x5vRectangle02() throws Exception {
        new TestHelper(
            "geo-grid180-rectangle02",
            "geo-grid180-rectangle02.rq", 
            "geo-grid180x5.n3",
            "geo-grid180-rectangle02.srx").runTest();     
    }

    
    /**
     * Rectangle test query on grid ranging from [-179;180] longitude
     * and [-5;5] latitude and fixed "0" timestamp, aiming
     * covering edge cases overlapping the -180 and +180
     * longitude values.
     * 
     * @throws Exception
     */
    public void testGrid180x5vRectangle03() throws Exception {
        new TestHelper(
            "geo-grid180-rectangle03",
            "geo-grid180-rectangle03.rq", 
            "geo-grid180x5.n3",
            "geo-grid180-rectangle03.srx").runTest();     
    }
    
    /**
     * Circle test q query on grid ranging from [-179;180] longitude
     * and [-5;5] latitude and fixed "0" timestamp, aiming
     * covering edge cases overlapping the -180 and +180
     * longitude values.
     * 
     * @throws Exception
     */
    public void testGrid180x5vCircle01() throws Exception {
        new TestHelper(
            "geo-grid180-circle01",
            "geo-grid180-circle01.rq", 
            "geo-grid180x5.n3",
            "geo-grid180-circle01.srx").runTest();     
    }

    
    /**
     * Circle test query on grid ranging from [-179;180] longitude
     * and [-5;5] latitude and fixed "0" timestamp, aiming
     * covering edge cases overlapping the -180 and +180
     * longitude values.
     * 
     * @throws Exception
     */
    public void testGrid180x5vCircle02() throws Exception {
        new TestHelper(
            "geo-grid180-circle02",
            "geo-grid180-circle02.rq", 
            "geo-grid180x5.n3",
            "geo-grid180-circle02.srx").runTest();     
    }

    
    /**
     * Circle test q query on grid ranging from [-179;180] longitude
     * and [-5;5] latitude and fixed "0" timestamp, aiming
     * covering edge cases overlapping the -180 and +180
     * longitude values.
     * 
     * @throws Exception
     */
    public void testGrid180x5vCircle03() throws Exception {
        new TestHelper(
            "geo-grid180-circle03",
            "geo-grid180-circle03.rq", 
            "geo-grid180x5.n3",
            "geo-grid180-circle03.srx").runTest();     
    }

    
    /**
     * Circle test q query on grid ranging from [-179;180] longitude
     * and [-5;5] latitude and fixed "0" timestamp, aiming
     * covering edge cases overlapping the -180 and +180
     * longitude values.
     * 
     * @throws Exception
     */
    public void testGrid180x5vCircle04() throws Exception {
        new TestHelper(
            "geo-grid180-circle04",
            "geo-grid180-circle04.rq", 
            "geo-grid180x5.n3",
            "geo-grid180-circle04.srx").runTest();     
    }

    /**
     * BLZG-1718: exception if search predicate is unknown
     * 
     * @throws Exception
     */
    public void testUnknwonPredicate() throws Exception {
        new TestHelper(
                "geo-unknown-predicate",
                "geo-unknown-predicate.rq", 
                "geo-unknown-predicate.nt",
                "geo-unknown-predicate.srx").runTest();     
    }
    
    /**
     * BLZG-1719: test rectangle for real-world example
     * 
     * @throws Exception
     */
    public void testRectangleRealworld() throws Exception {
        new TestHelper(
                "geo-rectangle-realworld",
                "geo-rectangle-realworld.rq", 
                "geo-rectangle-realworld.n3",
                "geo-rectangle-realworld.srx").runTest();     
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
                + "{ \"uri\": \"http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral\", "
                + "\"fields\": [ "
                + "{ \"valueType\": \"DOUBLE\", \"multiplier\": \"100000\", \"serviceMapping\": \"LATITUDE\" }, "
                + "{ \"valueType\": \"DOUBLE\", \"multiplier\": \"100000\", \"serviceMapping\": \"LONGITUDE\" }, "
                + "{ \"valueType\": \"LONG\", \"serviceMapping\" : \"TIME\"  } "
                + "]}}");
        
        properties.setProperty(
                com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL_DEFAULT_DATATYPE,
                "http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral");
        
        return properties;

    }
}
