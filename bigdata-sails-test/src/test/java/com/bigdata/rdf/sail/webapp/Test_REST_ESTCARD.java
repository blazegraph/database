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

import java.util.Collection;
import java.util.Properties;

import junit.framework.Test;

import org.openrdf.model.Resource;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryConnection;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.RemoveOp;

/**
 * Proxied test suite for the ESTCARD method (estimated cardinality aka fast
 * range counts) and related operations at the {@link RepositoryConnection} that
 * tunnel through to the same REST API method (getContexts(), size()).
 * 
 * @param <S>
 * 
 * TODO Should test GET as well as POST (this requires that we configured the
 * client differently).
 */
public class Test_REST_ESTCARD<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	public Test_REST_ESTCARD() {

	}

	public Test_REST_ESTCARD(final String name) {

		super(name);

	}

	public static Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(Test_REST_ESTCARD.class,
                "test.*", TestMode.quads
//                , TestMode.sids
//                , TestMode.triples
                );
       
	}

	/**
	 * Test the ESTCARD method (fast range count).
	 */
	public void test_ESTCARD() throws Exception {

		doInsertbyURL("POST", packagePath + "test_estcard.ttl");

		/*
		 * Note: In this case, it should work out that the exact size and the
		 * fast range count are the same. However, we want the FAST RANGE COUNT
		 * here since that is what we are testing.
		 */
		final long rangeCount = m_repo.size();

		assertEquals(7, rangeCount);

	}

	public void test_ESTCARD_s() throws Exception {

		doInsertbyURL("POST", packagePath + "test_estcard.ttl");

		final long rangeCount = m_repo.rangeCount(new URIImpl(
				"http://www.bigdata.com/Mike"),// s
				null,// p
				null// o
				);

		assertEquals(3, rangeCount);

	}

	public void test_ESTCARD_p() throws Exception {

		doInsertbyURL("POST", packagePath + "test_estcard.ttl");

		final long rangeCount = m_repo.rangeCount(null,// s
				RDF.TYPE,// p
				null// o
				// null // c
				);
		assertEquals(3, rangeCount);

	}

	public void test_ESTCARD_p2() throws Exception {

		doInsertbyURL("POST", packagePath + "test_estcard.ttl");

		final long rangeCount = m_repo.rangeCount(null,// s
				RDFS.LABEL,// p
				null// o
				// null // c
				);

		assertEquals(2, rangeCount);

	}

	public void test_ESTCARD_o() throws Exception {

		doInsertbyURL("POST", packagePath + "test_estcard.ttl");

		final long rangeCount = m_repo.rangeCount(null,// s
				null,// p
				new LiteralImpl("Mike")// o
				// null // c
				);

		assertEquals(1, rangeCount);

	}

	public void test_ESTCARD_so() throws Exception {

		doInsertbyURL("POST", packagePath + "test_estcard.ttl");

		final long rangeCount = m_repo.rangeCount(new URIImpl(
				"http://www.bigdata.com/Mike"),// s,
				RDF.TYPE,// p
				null// ,// o
				// null // c
				);

		assertEquals(1, rangeCount);

	}

	/**
	 * Test the ESTCARD method (fast range count).
	 */
	public void test_ESTCARD_quads_01() throws Exception {

		if (TestMode.quads != getTestMode())
			return;

		doInsertbyURL("POST", packagePath + "test_estcard.trig");

		final long rangeCount = m_repo.rangeCount(null,// s,
				null,// p
				null// o
				// null // c
				);
		assertEquals(7, rangeCount);

	}

	public void test_ESTCARD_quads_02() throws Exception {

		if (TestMode.quads != getTestMode())
			return;

		doInsertbyURL("POST", packagePath + "test_estcard.trig");

		final long rangeCount = m_repo.rangeCount(null,// s,
				null,// p
				null,// o
				new URIImpl("http://www.bigdata.com/")// c
				);

		assertEquals(3, rangeCount);

	}

	public void test_ESTCARD_quads_03() throws Exception {

		if (TestMode.quads != getTestMode())
			return;

		doInsertbyURL("POST", packagePath + "test_estcard.trig");

		final long rangeCount = m_repo.rangeCount(null,// s,
				null,// p
				null,// o
				new URIImpl("http://www.bigdata.com/c1")// c
				);

		assertEquals(2, rangeCount);

	}

	public void test_ESTCARD_quads_04() throws Exception {

		if (TestMode.quads != getTestMode())
			return;

		doInsertbyURL("POST", packagePath + "test_estcard.trig");

		final long rangeCount = m_repo.rangeCount(new URIImpl(
				"http://www.bigdata.com/Mike"),// s,
				null,// p
				null,// o
				new URIImpl("http://www.bigdata.com/c1")// c
				);

		assertEquals(1, rangeCount);

	}

	/**
	 * Test the CONTEXTS method.
	 */
	public void test_CONTEXTS() throws Exception {

		if (getTestMode() != TestMode.quads)
			return;

		doInsertbyURL("POST", packagePath + "test_estcard.trig");

		final Collection<Resource> contexts = m_repo.getContexts();

		assertEquals(3, contexts.size());

	}

	/**
	 * 
    * @see <a href="http://trac.bigdata.com/ticket/1127"> Extend ESTCARD method
    *      for exact range counts </a>
	 */
   static public class ReadWriteTx<S extends IIndexManager> extends
         Test_REST_ESTCARD<S> {
	 
	   public static Test suite() {

	      return ProxySuiteHelper.suiteWhenStandalone(Test_REST_ESTCARD.ReadWriteTx.class,
	                "test.*", TestMode.quads
//	                , TestMode.sids
//	                , TestMode.triples
	                );
	       
	   }

	   @Override
	   public Properties getProperties() {
	      
	      final Properties p = new Properties(super.getProperties());

	      p.setProperty(BigdataSail.Options.ISOLATABLE_INDICES, "true");
	      
	      return p;
	      
	   }

      /**
       * Test the ESTCARD method when statements have been added, committed, and
       * then removed from a namespace that supports fully isolated read/write
       * transactions.
       */
	   public void test_ESTCARD_readWriteTx() throws Exception {

	      // Insert statements.
	      doInsertbyURL("POST", packagePath + "test_estcard.ttl");

         /*
          * Since we have inserted data and not yet deleted anything, the fast
          * and exact range counts will be identical.
          */
         final long exactRangeCount1 = m_repo.rangeCount(true/* exact */,
               null/* s */, null/* p */, null/* o */);
         final long fastRangeCount1 = m_repo.rangeCount(false/* exact */,
               null/* s */, null/* p */, null/* o */);

         assertEquals(7, exactRangeCount1);
         assertEquals(7, fastRangeCount1);

         /*
          * Now delete all triples with rdfs:label as the predicate (there are
          * two). The fast range count should be unchanged since it counts the
          * deleted tuple in the index. The exact range count should reflect the
          * removed statement.
          */

         final long mutationCount = m_repo.remove(new RemoveOp(null/* s */,
               RDFS.LABEL/* p */, null/* o */));

         assertEquals(2, mutationCount);

         final long exactRangeCount2 = m_repo.rangeCount(true/* exact */,
               null/* s */, null/* p */, null/* o */);
         final long fastRangeCount2 = m_repo.rangeCount(false/* exact */,
               null/* s */, null/* p */, null/* o */);

         assertEquals(5, exactRangeCount2);
         assertEquals(7, fastRangeCount2);

      }

   }

}
