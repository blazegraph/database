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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Test;

import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;
import com.bigdata.rdf.store.BD;

/**
 * Proxied test suite.
 *
 * @param <S>
 * 
 * TODO Should test GET as well as POST (this requires that we configured the
 * client differently).
 */
public class Test_REST_DESCRIBE<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	public Test_REST_DESCRIBE() {

	}

	public Test_REST_DESCRIBE(final String name) {

		super(name);

	}

	public static Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(Test_REST_DESCRIBE.class,
                "test.*", TestMode.quads
//                , TestMode.sids
//                , TestMode.triples
                );
       
	}

	public void test_GET_DESCRIBE_RDFXML() throws Exception {
		doDescribeTest("GET", RDFFormat.RDFXML);
	}

	public void test_GET_DESCRIBE_NTRIPLES() throws Exception {
		doDescribeTest("GET", RDFFormat.NTRIPLES);
	}

	public void test_GET_DESCRIBE_N3() throws Exception {
		doDescribeTest("GET", RDFFormat.N3);
	}

	public void test_GET_DESCRIBE_TURTLE() throws Exception {
		doDescribeTest("GET", RDFFormat.TURTLE);
	}

	public void test_GET_DESCRIBE_TRIG() throws Exception {
		doDescribeTest("GET", RDFFormat.TRIG);
	}

	public void test_GET_DESCRIBE_TRIX() throws Exception {
		doDescribeTest("GET", RDFFormat.TRIX);
	}

	public void test_POST_DESCRIBE_RDFXML() throws Exception {
		doDescribeTest("POST", RDFFormat.RDFXML);
	}

	public void test_POST_DESCRIBE_NTRIPLES() throws Exception {
		doDescribeTest("POST", RDFFormat.NTRIPLES);
	}

	public void test_POST_DESCRIBE_N3() throws Exception {
		doDescribeTest("POST", RDFFormat.N3);
	}

	public void test_POST_DESCRIBE_TURTLE() throws Exception {
		doDescribeTest("POST", RDFFormat.TURTLE);
	}

	public void test_POST_DESCRIBE_TRIG() throws Exception {
		doDescribeTest("POST", RDFFormat.TRIG);
	}

	public void test_POST_DESCRIBE_TRIX() throws Exception {
		doDescribeTest("POST", RDFFormat.TRIX);
	}

   /**
    * I am not quite sure about the origin of this test. It appears to have been
    * related to the conversion from apache http to jetty for the client API. I
    * think that it was probably developed by Martyn. bbt. 
    * 
    * @see <a href="http://trac.bigdata.com/ticket/967"> Replace Apache Http
    *      Components with jetty http client (was Roll forward the apache http
    *      components dependency to 4.3.3) </a>
    * 
    * @throws Exception
    */
   public void test976_describeStress() throws Exception {
      
      doStressDescribeTest("GET", RDFFormat.RDFXML, 100 /** tasks **/, 50 /** threads **/, 500 /** statements **/);
      
   }

   protected void doStressDescribeTest(final String method,
         final RDFFormat format, final int tasks, final int threads,
         final int statements) throws Exception {

      final URI person = new URIImpl(BD.NAMESPACE + "Person");
      final URI likes = new URIImpl(BD.NAMESPACE + "likes");
      final URI rdf = new URIImpl(BD.NAMESPACE + "RDF");
      final URI rdfs = new URIImpl(BD.NAMESPACE + "RDFS");

      {
         // create a large number of mikes and bryans
         final Graph g = new GraphImpl();
         for (int n = 0; n < statements; n++) {
            final URI miken = new URIImpl(BD.NAMESPACE + "Mike#" + n);
            final URI bryann = new URIImpl(BD.NAMESPACE + "Bryan#" + n);
            final Literal nameMiken = new LiteralImpl("Mike#" + n);
            final Literal nameBryann = new LiteralImpl("Bryan#" + n);
            g.add(miken, RDF.TYPE, person);
            g.add(miken, likes, rdf);
            g.add(miken, RDFS.LABEL, nameMiken);
            g.add(bryann, RDF.TYPE, person);
            g.add(bryann, likes, rdfs);
            g.add(bryann, RDFS.LABEL, nameBryann);
         }

         m_repo.add(new AddOp(g));

      }

      // Run the DESCRIBE query and verify the results (non-empty).
      {

         final String queryStr = "prefix bd: <" + BD.NAMESPACE + "> " + //
               "prefix rdf: <" + RDF.NAMESPACE + "> " + //
               "prefix rdfs: <" + RDFS.NAMESPACE + "> " + //
               "DESCRIBE ?x " + //
               "WHERE { " + //
               "  ?x rdf:type bd:Person . " + //
               "  ?x bd:likes bd:RDF " + //
               "}";

         final AtomicInteger errorCount = new AtomicInteger();
         final Callable<Void> task = new Callable<Void>() {

            @Override
            public Void call() throws Exception {
               try {

                  final Graph actual = asGraph(m_repo
                        .prepareGraphQuery(queryStr));

                  assertTrue(!actual.isEmpty());

                  return null;
               } catch (Exception e) {
                  log.warn("Call failure", e);

                  errorCount.incrementAndGet();

                  throw e;
               }
            }

         };

         final int threadCount = Thread.activeCount();

         final ExecutorService exec = Executors.newFixedThreadPool(threads);
         for (int r = 0; r < tasks; r++) {
            exec.submit(task);
         }
         exec.shutdown();
         exec.awaitTermination(2000, TimeUnit.SECONDS);
         // force shutdown
         exec.shutdownNow();

         int loops = 20;
         while (Thread.activeCount() > threadCount && --loops > 0) {
            Thread.sleep(500);
            if (log.isTraceEnabled())
               log.trace("Extra threads: "
                     + (Thread.activeCount() - threadCount));
         }

         if (log.isInfoEnabled())
            log.info("Return with extra threads: "
                  + (Thread.activeCount() - threadCount));

         assertTrue(errorCount.get() == 0);
      }

   }

}
