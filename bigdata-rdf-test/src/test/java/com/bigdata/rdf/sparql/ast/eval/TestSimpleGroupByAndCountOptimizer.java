/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2013.  All rights reserved.

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
package com.bigdata.rdf.sparql.ast.eval;

import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.join.DistinctTermScanOp;
import com.bigdata.bop.join.FastRangeCountOp;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSimpleGroupByAndCountOptimizer;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * SPARQL level test suite for the {@link ASTSimpleGroupByAndCountOptimizer}.
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/1059"> GROUP BY optimization
 *      using distinct-term-scan and fast-range-count</a>
 *      
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class TestSimpleGroupByAndCountOptimizer extends
      AbstractDataDrivenSPARQLTestCase {

   public TestSimpleGroupByAndCountOptimizer() {
   }

   public TestSimpleGroupByAndCountOptimizer(String name) {
      super(name);
   }

   public static Test suite() {

      final TestSuite suite = new TestSuite(
            TestSimpleGroupByAndCountOptimizer.class.getSimpleName());

      suite.addTestSuite(TestTriplesModeAPs.class);
      suite.addTestSuite(TestQuadsModeAPs.class);

      return suite;
   }

   /**
    * Triples mode test suite.
    */
   public static class TestTriplesModeAPs extends
         TestSimpleGroupByAndCountOptimizer {

      @Override
      public Properties getProperties() {

         final Properties properties = new Properties(super.getProperties());

         // turn off quads.
         properties.setProperty(AbstractTripleStore.Options.QUADS, "false");

         // turn on triples
         properties.setProperty(AbstractTripleStore.Options.TRIPLES_MODE,
               "true");

         return properties;

      }

      /**
       * Optimization applies to pattern:
       * 
       * <pre>
       * SELECT  (COUNT(*) as ?count) ?z 
       * WHERE {  ?x rdf:type ?z  } GROUP BY ?z
       * </pre>
       */
      public void test_simpleGroupByAndCount_01() throws Exception {

         final TestHelper h = new TestHelper(
               "simpleGroupByAndCount_triples_01", // testURI,
               "simpleGroupByAndCount_triples_01.rq",// queryFileURL
               "simpleGroupByAndCount_triples.ttl",// dataFileURL
               "simpleGroupByAndCount_triples_01.srx"// resultFileURL
         );

         h.runTest();

         // Verify that the DistinctTermScanOp and FastRangeCountOp
         // are both used in the query plan.
         assertEquals(
               1,
               BOpUtility.toList(h.getASTContainer().getQueryPlan(),
                     DistinctTermScanOp.class).size());
         assertEquals(
               1,
               BOpUtility.toList(h.getASTContainer().getQueryPlan(),
                     FastRangeCountOp.class).size());
      }

      /**
       * Optimization applies to pattern:
       * 
       * <pre>
       * SELECT  (COUNT(?x) as ?count) ?z 
       * WHERE {  ?x rdf:type ?z  } GROUP BY ?z
       * </pre>
       */
      public void test_simpleGroupByAndCount_02() throws Exception {

         final TestHelper h = new TestHelper(
               "simpleGroupByAndCount_triples_02", // testURI,
               "simpleGroupByAndCount_triples_02.rq",// queryFileURL
               "simpleGroupByAndCount_triples.ttl",// dataFileURL
               "simpleGroupByAndCount_triples_02.srx"// resultFileURL
         );

         h.runTest();

         // Verify that the DistinctTermScanOp and FastRangeCountOp
         // are both used in the query plan.
         assertEquals(
               1,
               BOpUtility.toList(h.getASTContainer().getQueryPlan(),
                     DistinctTermScanOp.class).size());
         assertEquals(
               1,
               BOpUtility.toList(h.getASTContainer().getQueryPlan(),
                     FastRangeCountOp.class).size());
      }

      /**
       * Optimization applies to pattern:
       * 
       * <pre>
       * SELECT  (COUNT(?z) as ?count) ?z 
       * WHERE {  ?x rdf:type ?z  } GROUP BY ?z
       * </pre>
       */
      public void test_simpleGroupByAndCount_03() throws Exception {

         final TestHelper h = new TestHelper(
               "simpleGroupByAndCount_triples_03", // testURI,
               "simpleGroupByAndCount_triples_03.rq",// queryFileURL
               "simpleGroupByAndCount_triples.ttl",// dataFileURL
               "simpleGroupByAndCount_triples_03.srx"// resultFileURL
         );

         h.runTest();

         // Verify that the FastRangeCountOp is used in the query plan
         assertEquals(
               1,
               BOpUtility.toList(h.getASTContainer().getQueryPlan(),
                     FastRangeCountOp.class).size());
      }

      /**
       * Optimization only partially applies to pattern:
       * 
       * <pre>
       * SELECT  (COUNT(*) as ?count) ?z 
       * WHERE {  ?z rdf:type ?x  } GROUP BY ?z
       * </pre>
       * 
       * Fast range count is used, but no distinct term scan. The reason is that
       * there is no PSO index.
       */
      public void test_simpleGroupByAndCount_04() throws Exception {

         final TestHelper h = new TestHelper(
               "simpleGroupByAndCount_triples_04", // testURI,
               "simpleGroupByAndCount_triples_04.rq",// queryFileURL
               "simpleGroupByAndCount_triples.ttl",// dataFileURL
               "simpleGroupByAndCount_triples_04.srx"// resultFileURL
         );

         h.runTest();

         // Verify that there is neither a FastRangeCountOp nor a
         // DistinctTermScanOp used in the query plan
         assertEquals(
               1,
               BOpUtility.toList(h.getASTContainer().getQueryPlan(),
                     FastRangeCountOp.class).size());
         assertEquals(
               0,
               BOpUtility.toList(h.getASTContainer().getQueryPlan(),
                     DistinctTermScanOp.class).size());
      }

      /**
       * The optimization must *not* be applied in the presence of delete
       * markers. Delete markers are present in the case of isolatable
       * indices. The query below, which is amenable to optimization in 
       * principle, cannot be optimized when this mode is used.
       * 
       * <pre>
       * SELECT  (COUNT(*) as ?count) ?z 
       * WHERE {  ?z rdf:type ?x  } GROUP BY ?z
       * </pre>
       */
      public void test_simpleGroupByAndCount_delete_markers() throws Exception {

         final TestHelper h = new TestHelper(
               "simpleGroupByAndCount_triples_04", // testURI,
               "simpleGroupByAndCount_triples_04.rq",// queryFileURL
               "simpleGroupByAndCount_triples.ttl",// dataFileURL
               "simpleGroupByAndCount_triples_04.srx"// resultFileURL
         );
         enableDeleteMarkersInIndes();
         h.runTest();

         // Verify that there is neither a FastRangeCountOp nor a
         // DistinctTermScanOp used in the query plan
         assertEquals(
               0,
               BOpUtility.toList(h.getASTContainer().getQueryPlan(),
                     FastRangeCountOp.class).size());
         assertEquals(
               0,
               BOpUtility.toList(h.getASTContainer().getQueryPlan(),
                     DistinctTermScanOp.class).size());
      }

   }

   /**
    * Quads mode test suite.
    */
   public static class TestQuadsModeAPs extends
         TestSimpleGroupByAndCountOptimizer {

      /**
       * Optimization applies as in triple mode for fully unbound triple
       * patterns, e.g.:
       * 
       * <pre>
       * SELECT  (COUNT(*) as ?count) ?z 
       * WHERE { GRAPH ?g { ?x rdf:type ?z } } GROUP BY ?z
       * </pre>
       */
      public void test_distinctTermScan_quads_01() throws Exception {

         final TestHelper h = new TestHelper("distinctTermScan_quads_01", // testURI,
               "simpleGroupByAndCount_quads_01.rq",// queryFileURL
               "simpleGroupByAndCount_quads.trig",// dataFileURL
               "simpleGroupByAndCount_quads_01.srx"// resultFileURL
         );

         h.runTest();

         // Verify that the DistinctTermScanOp and FastRangeCountOp
         // are both used in the query plan.
         assertEquals(
               1,
               BOpUtility.toList(h.getASTContainer().getQueryPlan(),
                     DistinctTermScanOp.class).size());
         assertEquals(
               1,
               BOpUtility.toList(h.getASTContainer().getQueryPlan(),
                     FastRangeCountOp.class).size());
      }

      /**
       * The query
       * 
       * <pre>
       * SELECT  (COUNT(*) as ?count) ?z 
       * WHERE { GRAPH <http://www.bigdata.com/mygraph> { ?x rdf:type ?z } } GROUP BY ?z
       * </pre>
       * 
       * requires either an CPOS or a PCOS index for the distinct term scan
       * optimization, none of which is present. Hence, only the fast range
       * count optimization can be applied.
       */
      public void test_distinctTermScan_quads_02() throws Exception {

         final TestHelper h = new TestHelper("distinctTermScan_quads_02", // testURI,
               "simpleGroupByAndCount_quads_02.rq",// queryFileURL
               "simpleGroupByAndCount_quads.trig",// dataFileURL
               "simpleGroupByAndCount_quads_02.srx"// resultFileURL
         );

         h.runTest();

         // Verify that the DistinctTermScanOp and FastRangeCountOp
         // are both used in the query plan.
         assertEquals(
               0,
               BOpUtility.toList(h.getASTContainer().getQueryPlan(),
                     DistinctTermScanOp.class).size());
         assertEquals(
               1,
               BOpUtility.toList(h.getASTContainer().getQueryPlan(),
                     FastRangeCountOp.class).size());
      }

      /**
       * The query
       * 
       * <pre>
       * SELECT  (COUNT(*) as ?count) ?s
       * WHERE { GRAPH <http://www.bigdata.com/mygraph> { ?s ?p ?o } } 
       * GROUP BY ?s
       * </pre>
       * 
       * can be optimized using the CSPO index.
       */
      public void test_distinctTermScan_quads_03() throws Exception {

         final TestHelper h = new TestHelper("distinctTermScan_quads_03", // testURI,
               "simpleGroupByAndCount_quads_03.rq",// queryFileURL
               "simpleGroupByAndCount_quads.trig",// dataFileURL
               "simpleGroupByAndCount_quads_03.srx"// resultFileURL
         );

         h.runTest();

         // Verify that the DistinctTermScanOp and FastRangeCountOp
         // are both used in the query plan.
         assertEquals(
               1,
               BOpUtility.toList(h.getASTContainer().getQueryPlan(),
                     DistinctTermScanOp.class).size());
         assertEquals(
               1,
               BOpUtility.toList(h.getASTContainer().getQueryPlan(),
                     FastRangeCountOp.class).size());
      }
      
      /**
       * The optimization must *not* be applied in the presence of delete
       * markers. Delete markers are present in the case of isolatable
       * indices. The query below, which is amenable to optimization in 
       * principle, cannot be optimized when this mode is used.
       * 
       * <pre>
       * SELECT  (COUNT(*) as ?count) ?s
       * WHERE { GRAPH <http://www.bigdata.com/mygraph> { ?s ?p ?o } } 
       * GROUP BY ?s
       * </pre>
       */
      public void test_distinctTermScan_quads_delete_markers() throws Exception {

         final TestHelper h = new TestHelper("distinctTermScan_quads_03", // testURI,
               "simpleGroupByAndCount_quads_03.rq",// queryFileURL
               "simpleGroupByAndCount_quads.trig",// dataFileURL
               "simpleGroupByAndCount_quads_03.srx"// resultFileURL
         );
         enableDeleteMarkersInIndes();
         h.runTest();

         // Verify that the DistinctTermScanOp and FastRangeCountOp
         // are both used in the query plan.
         assertEquals(
               0,
               BOpUtility.toList(h.getASTContainer().getQueryPlan(),
                     DistinctTermScanOp.class).size());
         assertEquals(
               0,
               BOpUtility.toList(h.getASTContainer().getQueryPlan(),
                     FastRangeCountOp.class).size());
      }

   }
}
