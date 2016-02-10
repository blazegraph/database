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

package com.bigdata.rdf.sail.webapp;

import java.util.Properties;

import junit.framework.Test;

import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.RemoveOp;

/**
 * Proxied test suite for the <code>HASSTMT</code> REST API method.
 * 
 * @param <S>
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1109" >hasStatements can
 *      overestimate and ignores includeInferred (REST API) </a>
 * 
 *      FIXME *** Cover the quads mode APs.
 * 
 * TODO Should test GET as well as POST (this requires that we configured the
 * client differently).
 */
public class Test_REST_HASSTMT<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	public Test_REST_HASSTMT() {

	}

	public Test_REST_HASSTMT(final String name) {

		super(name);

	}

	public static Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(Test_REST_HASSTMT.class,
                "test.*", TestMode.quads
//                , TestMode.sids
//                , TestMode.triples
                );
       
	}

   public void test_HASSTMT_s() throws Exception {

      doInsertbyURL("POST", packagePath + "test_estcard.ttl");

      assertTrue(m_repo.hasStatement(new URIImpl(
            "http://www.bigdata.com/Mike"),// s
            null,// p
            null,// o
            false// includeInferred
            ));

      assertFalse(m_repo.hasStatement(new URIImpl(
            "http://www.bigdata.com/Fred"),// s
            null,// p
            null,// o
            false// includeInferred
            ));

   }

   public void test_HASSTMT_p() throws Exception {

      doInsertbyURL("POST", packagePath + "test_estcard.ttl");

      assertTrue(m_repo.hasStatement(null,// s
            RDF.TYPE,// p
            null,// o
            false// includeInferred
            // c
            ));

      assertFalse(m_repo.hasStatement(null,// s
            RDF.BAG,// p
            null,// o
            false// includeInferred
            // c
            ));

   }

   public void test_HASSTMT_o() throws Exception {

      doInsertbyURL("POST", packagePath + "test_estcard.ttl");

      assertTrue(m_repo.hasStatement(null,// s
            null,// p
            new LiteralImpl("Mike"),// o
            false // includeInferred
            // c
            ));

      assertFalse(m_repo.hasStatement(null,// s
            null,// p
            new LiteralImpl("Fred"),// o
            false // includeInferred
            // c
            ));

   }

   public void test_HASSTMT_so() throws Exception {

      doInsertbyURL("POST", packagePath + "test_estcard.ttl");

      assertTrue( m_repo.hasStatement(new URIImpl(
            "http://www.bigdata.com/Mike"),// s,
            RDF.TYPE,// p
            null,// o
            false // includeInferred
            // null // c
            ));

      assertFalse( m_repo.hasStatement(new URIImpl(
            "http://www.bigdata.com/Fred"),// s,
            RDF.TYPE,// p
            null,// o
            false // includeInferred
            // null // c
            ));

      assertFalse( m_repo.hasStatement(new URIImpl(
            "http://www.bigdata.com/Mike"),// s,
            RDF.BAG,// p
            null,// o
            false // includeInferred
            // null // c
            ));

   }

   public void test_HASSTMT_po() throws Exception {

      doInsertbyURL("POST", packagePath + "test_estcard.ttl");

      assertTrue( m_repo.hasStatement(null,// s,
            RDFS.LABEL,// p
            new LiteralImpl("Mike"),// o
            false // includeInferred
            // null // c
            ));

      assertFalse( m_repo.hasStatement(null,// s,
            RDFS.LABEL,// p
            new LiteralImpl("Fred"),// o
            false // includeInferred
            // null // c
            ));

      assertFalse( m_repo.hasStatement(null,// s,
            RDF.BAG,// p
            new LiteralImpl("Mike"),// o
            false // includeInferred
            // null // c
            ));

   }

   public void test_HASSTMT_sp() throws Exception {

      doInsertbyURL("POST", packagePath + "test_estcard.ttl");

      assertTrue(m_repo.hasStatement(new URIImpl(
            "http://www.bigdata.com/Mike"),// s
            RDFS.LABEL,// p
            null,// o
            false// includeInferred
            ));

      assertFalse(m_repo.hasStatement(new URIImpl(
            "http://www.bigdata.com/Mike"),// s
            RDF.BAG,// p
            null,// o
            false// includeInferred
            ));

   }

   /**
    * Adds test coverage using read/write tx and verifies that the behavior is
    * correct after we delete statements (that is, it is not relying on a fast
    * range count for read/write tx namespaces).
    * 
    * @see <a href="http://trac.bigdata.com/ticket/1109" >hasStatements can
    *      overestimate and ignores includeInferred (REST API) </a>
    */
   static public class ReadWriteTx<S extends IIndexManager> extends
         Test_REST_HASSTMT<S> {
    
      public static Test suite() {

         return ProxySuiteHelper.suiteWhenStandalone(Test_REST_HASSTMT.ReadWriteTx.class,
                   "test.*", TestMode.triples
//                 , TestMode.sids
//                 , TestMode.triples
                   );
          
      }

      @Override
      public Properties getProperties() {
         
         final Properties p = new Properties(super.getProperties());

         p.setProperty(BigdataSail.Options.ISOLATABLE_INDICES, "true");
         
         return p;
         
      }

      /**
       * Test the HASSTMT method when statements have been added, committed, and
       * then removed from a namespace that supports fully isolated read/write
       * transactions.
       */
      public void test_HASSTMT_readWriteTx() throws Exception {

         // Insert statements.
         doInsertbyURL("POST", packagePath + "test_estcard.ttl");

         /*
          * Since we have inserted data and not yet deleted anything, the fast
          * and exact range counts will be identical.
          */
         assertTrue(m_repo.hasStatement(null,// s
               RDFS.LABEL,// p
               null,// o
               false// includeInferred
               ));

         /*
          * Now delete all triples with rdfs:label as the predicate (there are
          * two). The fast range count should be unchanged since it counts the
          * deleted tuple in the index. The deleted statements should not be
          * reported by HASSTMT.
          */

         final long mutationCount = m_repo.remove(new RemoveOp(null/* s */,
               RDFS.LABEL/* p */, null/* o */));

         assertEquals(2, mutationCount);

         assertFalse(m_repo.hasStatement(null,// s
               RDFS.LABEL,// p
               null,// o
               false// includeInferred
               ));

      }

   }

   /**
    * Test suite for the semantics of includeInferred (this requires setting up
    * a namespace with truth maintenance).
    */
   static public class TruthMaintenance<S extends IIndexManager> extends
         Test_REST_HASSTMT<S> {

      public static Test suite() {

         return ProxySuiteHelper.suiteWhenStandalone(
               Test_REST_HASSTMT.TruthMaintenance.class, "test.*", TestMode.triples
         // , TestMode.sids
         // , TestMode.triples
               );

      }

      /**
       * Overlay the properties configuration for triples plus incremental truth
       * maintenance.
       * <p>
       * Note: You still only want to run this test suite when the test mode is
       * triples or sids. It will fail if you attempt to run with quads since
       * inference is not supported for quads.
       */
      @Override
      public Properties getProperties() {

         final Properties p = TestMode.triplesPlusTruthMaintenance
               .getProperties(super.getProperties());
         
         return p;

      }

      /**
       * Test the HASSTMT method for correct handling of the includeInferred
       * parameter.
       */
      public void test_HASSTMT_includeInferred() throws Exception {

         // Insert statements.
         doInsertbyURL("POST", packagePath + "test_estcard.ttl");

         /*
          * Without inferences.
          */
         assertFalse(m_repo.hasStatement(OWL.EQUIVALENTCLASS,
               RDFS.SUBPROPERTYOF, RDFS.SUBCLASSOF, false// includeInferred
               ));

         /*
          * With inferences.
          */
         assertTrue(m_repo.hasStatement(OWL.EQUIVALENTCLASS,
               RDFS.SUBPROPERTYOF, RDFS.SUBCLASSOF, true// includeInferred
               ));

         /*
          * Now delete all triples. Since we are testing an axiom, it should
          * still be there.
          */

         /*
          * TODO There appears to be a bug where removeAll() is also removing
          * axioms from the statement indices. This is a problem at the
          * Sail/AbstractTripleStore layer. We can restore the removeAll()
          * call once it is addressed (maybe as a 2nd test case).
          * 
          * See #1176 removeAll() removes axioms also (Truth Maintenance)
          */
//         final long mutationCount = m_repo.remove(new RemoveOp(null/* s */,
//               null/* p */, null/* o */));

         long mutationCount = 0;
         
         mutationCount += m_repo.remove(new RemoveOp(null/* s */,
               RDF.TYPE/* p */, null/* o */));
         mutationCount += m_repo.remove(new RemoveOp(null/* s */,
               RDFS.LABEL/* p */, null/* o */));
         mutationCount += m_repo.remove(new RemoveOp(null/* s */,
               FOAF.KNOWS/* p */, null/* o */));

         /*
          * There are 7 told triples. 194 inferences with the current rule set.
          * So the number of deleted statements is significantly more than the 7
          * told statements.
          */ 
         assertTrue(mutationCount > 7);
         
         /*
          * Without inferences.
          */
         assertFalse(m_repo.hasStatement(OWL.EQUIVALENTCLASS,
               RDFS.SUBPROPERTYOF, RDFS.SUBCLASSOF, false// includeInferred
               ));

         /*
          * With inferences.
          */
         assertTrue(m_repo.hasStatement(OWL.EQUIVALENTCLASS,
               RDFS.SUBPROPERTYOF, RDFS.SUBCLASSOF, true// includeInferred
               ));

      }

   }

}
