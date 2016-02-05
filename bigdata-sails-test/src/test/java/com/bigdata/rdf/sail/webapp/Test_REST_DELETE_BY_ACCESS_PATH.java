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

import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import junit.framework.Test;

import com.bigdata.journal.IIndexManager;

/**
 * Proxied test suite for the DELETE_BY_ACCESS_PATH method.
 * 
 * @param <S>
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1177"> Resource... contexts not
 *      encoded/decoded according to openrdf semantics (REST API) </a>
 * 
 *      FIXME (***) Add tests for quads mode that target #1177
 */
public class Test_REST_DELETE_BY_ACCESS_PATH<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	public Test_REST_DELETE_BY_ACCESS_PATH() {

	}

	public Test_REST_DELETE_BY_ACCESS_PATH(final String name) {

		super(name);

	}

	public static Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(Test_REST_DELETE_BY_ACCESS_PATH.class,
                "test.*", TestMode.quads
//                , TestMode.sids
//                , TestMode.triples
                );
       
	}

   /**
    * Delete everything matching an access path description.
    */
   public void test_DELETE_accessPath_delete_all() throws Exception {

      doInsertbyURL("POST", packagePath + "test_delete_by_access_path.ttl");

      final long mutationResult = doDeleteWithAccessPath(//
            // requestPath,//
            null,// s
            null,// p
            null);

      assertEquals(7, mutationResult);

   }

   /**
    * Delete everything with a specific subject.
    */
   public void test_DELETE_accessPath_delete_s() throws Exception {

      doInsertbyURL("POST", packagePath + "test_delete_by_access_path.ttl");

      final long mutationResult = doDeleteWithAccessPath(//
            // requestPath,//
            new URIImpl("http://www.bigdata.com/Mike"),// s
            null,// p
            null);

      assertEquals(3, mutationResult);

   }

   /**
    * Delete everything with a specific predicate.
    */
   public void test_DELETE_accessPath_delete_p() throws Exception {

      doInsertbyURL("POST", packagePath + "test_delete_by_access_path.ttl");

      final long mutationResult = doDeleteWithAccessPath(//
            // requestPath,//
            null,// s
            new URIImpl("http://www.w3.org/2000/01/rdf-schema#label"),// p
            null// o
      );

      assertEquals(2, mutationResult);

   }

   /**
    * Delete everything with a specific object (a URI).
    */
   public void test_DELETE_accessPath_delete_o_URI() throws Exception {

      doInsertbyURL("POST", packagePath + "test_delete_by_access_path.ttl");

      final long mutationResult = doDeleteWithAccessPath(//
            // requestPath,//
            null,// s
            null,// p
            new URIImpl("http://xmlns.com/foaf/0.1/Person")// o
      );

      assertEquals(3, mutationResult);

   }

   /**
    * Delete everything with a specific object (a Literal).
    */
   public void test_DELETE_accessPath_delete_o_Literal() throws Exception {

      doInsertbyURL("POST", packagePath + "test_delete_by_access_path.ttl");

      final long mutationResult = doDeleteWithAccessPath(//
            // requestPath,//
            null,// s
            null,// p
            new URIImpl("http://www.bigdata.com/Bryan")// o
      );

      assertEquals(1, mutationResult);

   }

   /**
    * Delete everything with a specific predicate and object (a URI).
    */
   public void test_DELETE_accessPath_delete_p_o_URI() throws Exception {

      doInsertbyURL("POST", packagePath + "test_delete_by_access_path.ttl");

      final long mutationResult = doDeleteWithAccessPath(//
            // requestPath,//
            null,// s
            RDF.TYPE,// p
            new URIImpl("http://xmlns.com/foaf/0.1/Person")// o
      );

      assertEquals(3, mutationResult);

   }

   /**
    * Delete everything with a specific predicate and object (a Literal).
    */
   public void test_DELETE_accessPath_delete_p_o_Literal() throws Exception {

      doInsertbyURL("POST", packagePath + "test_delete_by_access_path.ttl");

      final long mutationResult = doDeleteWithAccessPath(//
            // requestPath,//
            null,// s
            RDFS.LABEL,// p
            new LiteralImpl("Bryan")// o
      );

      assertEquals(1, mutationResult);

   }

   /**
    * Delete using an access path which does not match anything.
    */
   public void test_DELETE_accessPath_delete_NothingMatched() throws Exception {

      doInsertbyURL("POST", packagePath + "test_delete_by_access_path.ttl");

      final long mutationResult = doDeleteWithAccessPath(//
            // requestPath,//
            null,// s
            null,// p
            new URIImpl("http://xmlns.com/foaf/0.1/XXX")// o
      );

      assertEquals(0, mutationResult);

   }

   /**
    * Delete everything in a named graph (context).
    */
   public void test_DELETE_accessPath_delete_c() throws Exception {

      if (TestMode.quads != getTestMode())
         return;

      doInsertbyURL("POST", packagePath + "test_delete_by_access_path.trig");

      final URI base = new URIImpl("http://www.bigdata.com/");
      final URI c1 = new URIImpl("http://www.bigdata.com/c1");
      final URI c2 = new URIImpl("http://www.bigdata.com/c2");
      
      // This is the named graph that we will delete.
      assertEquals(3,m_repo.rangeCount(null,// s,
            null,// p
            null,// o
            base// c
            ));
      
      // These named graphs will not be deleted.
      assertEquals(2,m_repo.rangeCount(null,// s,
            null,// p
            null,// o
            c1// c
            ));
      assertEquals(2,m_repo.rangeCount(null,// s,
            null,// p
            null,// o
            c2// c
            ));
      
      // Delete the named graph (and only that graph)
      final long mutationResult = doDeleteWithAccessPath(//
            // requestPath,//
            null,// s
            null,// p
            null,// o
            base // c
      );
      assertEquals(3, mutationResult); // verify #of stmts modified.

      // range count each named graph again.
      final long rangeCount_base = m_repo.rangeCount(null,// s,
            null,// p
            null,// o
            base// c
            );

      final long rangeCount_c1 = m_repo.rangeCount(null,// s,
            null,// p
            null,// o
            c1// c
            );

      final long rangeCount_c2 = m_repo.rangeCount(null,// s,
            null,// p
            null,// o
            c2// c
            );

      // This is what we deleted out of the quad store.
      assertEquals(0,rangeCount_base);

      // These should be unchanged.
      assertEquals(2, rangeCount_c1);

      // These should be unchanged.
      assertEquals(2, rangeCount_c2);

   }

   /**
    * Delete everything in a different named graph (context).
    */
   public void test_DELETE_accessPath_delete_c1() throws Exception {

      if (TestMode.quads != getTestMode())
         return;

      doInsertbyURL("POST", packagePath + "test_delete_by_access_path.trig");

      final long mutationResult = doDeleteWithAccessPath(//
            // requestPath,//
            null,// s
            null,// p
            null,// o
            new URIImpl("http://www.bigdata.com/c1") // c
      );

      assertEquals(2, mutationResult);

   }

   /**
    * Delete everything in a two named graphs (context) while the data in another
    * named graph is not deleted.
    */
   public void test_DELETE_accessPath_delete_multiple_contexts() throws Exception {

      if (TestMode.quads != getTestMode())
         return;

      doInsertbyURL("POST", packagePath + "test_delete_by_access_path.trig");

      final URI base = new URIImpl("http://www.bigdata.com/");
      final URI c1 = new URIImpl("http://www.bigdata.com/c1");
      final URI c2 = new URIImpl("http://www.bigdata.com/c2");
      
      // This named graph will not be deleted.
      assertEquals(3, m_repo.rangeCount(null,// s,
            null,// p
            null,// o
            base// c
            ));
      
      // These named graphs will be deleted.
      assertEquals(2, m_repo.rangeCount(null,// s,
            null,// p
            null,// o
            c1// c
            ));
      assertEquals(2,m_repo.rangeCount(null,// s,
            null,// p
            null,// o
            c2// c
            ));

      final long mutationResult = doDeleteWithAccessPath(//
            // requestPath,//
            null,// s
            null,// p
            null,// o
            c1, c2 // c
      );

      // should have removed 2 statements from each of two named graphs for a
      // total of 4 statements removed.
      assertEquals(4, mutationResult);

      // range count each named graph.
      final long rangeCount_base = m_repo.rangeCount(null,// s,
            null,// p
            null,// o
            base// c
            );

      final long rangeCount_c1 = m_repo.rangeCount(null,// s,
            null,// p
            null,// o
            c1// c
            );

      final long rangeCount_c2 = m_repo.rangeCount(null,// s,
            null,// p
            null,// o
            c2// c
            );

      // Not deleted.
      assertEquals(3,rangeCount_base);

      // These should be deleted
      assertEquals(0, rangeCount_c1);

      // These should be unchanged.
      assertEquals(0, rangeCount_c2);

   }

   /**
    * Delete using an access path with the context position bound.
    */
   public void test_DELETE_accessPath_delete_c_nothingMatched()
         throws Exception {

      if (TestMode.quads != getTestMode())
         return;

      doInsertbyURL("POST", packagePath + "test_delete_by_access_path.trig");

      final long mutationResult = doDeleteWithAccessPath(//
            // requestPath,//
            null,// s
            null,// p
            null,// o
            new URIImpl("http://xmlns.com/foaf/0.1/XXX") // c
      );

      assertEquals(0, mutationResult);

   }

}
