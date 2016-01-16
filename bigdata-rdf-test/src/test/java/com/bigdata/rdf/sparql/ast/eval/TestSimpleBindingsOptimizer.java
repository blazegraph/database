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

import java.util.List;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.Var;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * SPARQL level test suite for the {@link ASTSimpleBindingsOptimizer}.
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/653">Slow query with bind</a>
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class TestSimpleBindingsOptimizer extends
		AbstractDataDrivenSPARQLTestCase {

	public TestSimpleBindingsOptimizer() {
	}

	public TestSimpleBindingsOptimizer(String name) {
		super(name);
	}
	
    public static Test suite()
    {

        final TestSuite suite = new TestSuite(TestSimpleBindingsOptimizer.class.getSimpleName());

        suite.addTestSuite(TestQuadsModeAPs.class);

        suite.addTestSuite(TestTriplesModeAPs.class);
        
        return suite;
    }

    /**
     * Triples mode test suite.
     */
	public static class TestTriplesModeAPs extends TestSimpleBindingsOptimizer {

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
		 * <pre>
		 * SELECT * WHERE {
		 *   BIND ( <http://bigdata.com#Person> as ?type )
		 *   ?personId rdf:type ?type 
		 * }
		 * </pre>
		 */
		public void test_simpleBindingsOptimizer_triples_01() throws Exception {

			final TestHelper h = new TestHelper(
			      "simpleBindingsOptimizer_triples_01", // testURI,
					"simpleBindingsOptimizer_01.rq",// queryFileURL
					"simpleBindingsOptimizer_01.ttl",// dataFileURL
					"simpleBindingsOptimizer_01.srx"// resultFileURL
			);
			
			h.runTest();

         List<SPOPredicate> spoPredicates =
               BOpUtility.toList(
                  h.getASTContainer().getQueryPlan(), SPOPredicate.class);
            assertEquals(1,spoPredicates.size());
            assertPredicateUsesConstant_01(spoPredicates.get(0));
		}
		
      /**
       * Complex patterns like 
       * <pre>
       * SELECT * WHERE {
       *   BIND ( 2*?val as ?doubleVal )
       *   :Datapoint :value ?val 
       * }
       * 
       * cannot be replaced, as they're evaluated at runtime.
       * </pre>
       */
      public void test_simpleBindingsOptimizer_triples_02() throws Exception {

         final TestHelper h = new TestHelper(
               "simpleBindingsOptimizer_triples_02", // testURI,
               "simpleBindingsOptimizer_02.rq",// queryFileURL
               "simpleBindingsOptimizer_02.ttl",// dataFileURL
               "simpleBindingsOptimizer_02.srx"// resultFileURL
         );

         h.runTest();

         List<SPOPredicate> spoPredicates = BOpUtility.toList(h
               .getASTContainer().getQueryPlan(), SPOPredicate.class);
         assertEquals(1, spoPredicates.size());
         
         BOp var = spoPredicates.get(0).get(2);
         assertTrue(var instanceof Var);
      }

	}


	public static class TestQuadsModeAPs extends TestSimpleBindingsOptimizer {

	   
	   /**
       * <pre>
       * SELECT * WHERE {
       *   BIND ( <http://bigdata.com#Person> as ?type )
       *   ?personId rdf:type ?type 
       * }
       * </pre>
       */
      public void test_simpleBindingsOptimizer_quads_01() throws Exception {
         final TestHelper h = new TestHelper(
               "test_simpleBindingsOptimizer_quads_01", // testURI,
               "simpleBindingsOptimizer_01.rq",// queryFileURL
               "simpleBindingsOptimizer_01.trig",// dataFileURL
               "simpleBindingsOptimizer_01.srx"// resultFileURL
         );
         
         h.runTest();

         List<SPOPredicate> spoPredicates =
            BOpUtility.toList(
               h.getASTContainer().getQueryPlan(), SPOPredicate.class);
         assertEquals(1,spoPredicates.size());
         assertPredicateUsesConstant_01(spoPredicates.get(0));
      }
	}
	
	/**
	 * Make sure the passed predicate uses constant http://bigdata.com#Person
	 * in its third position (with shadowed variable ?type)
	 */
	public void assertPredicateUsesConstant_01(SPOPredicate pred) {
      Constant c = (Constant)pred.get(2);
      TermId tId = (TermId)c.get();

      assertEquals("type",c.getVar().getName());
      assertEquals("http://bigdata.com#Person", tId.getValue().toString());	   
	}
	
   /**
    * Complex patterns like 
    * <pre>
    * SELECT * WHERE {
    *   BIND ( 2*?val as ?doubleVal )
    *   :Datapoint :value ?val 
    * }
    * 
    * cannot be replaced, as they're evaluated at runtime.
    * </pre>
    */
   public void test_simpleBindingsOptimizer_triples_02() throws Exception {

      final TestHelper h = new TestHelper(
            "simpleBindingsOptimizer_triples_02", // testURI,
            "simpleBindingsOptimizer_02.rq",// queryFileURL
            "simpleBindingsOptimizer_02.trig",// dataFileURL
            "simpleBindingsOptimizer_02.srx"// resultFileURL
      );

      h.runTest();

      List<SPOPredicate> spoPredicates = BOpUtility.toList(h
            .getASTContainer().getQueryPlan(), SPOPredicate.class);
      assertEquals(1, spoPredicates.size());
      
      BOp var = spoPredicates.get(0).get(2);
      assertTrue(var instanceof Var);
   }
	
}
