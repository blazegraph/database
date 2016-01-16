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
Portions of this code are:

Copyright Aduna (http://www.aduna-software.com/) 2001-2007

All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.
    * Neither the name of the copyright holder nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/
/*
 * Created on Mar 6, 2012
 */
package com.bigdata.rdf.sail.webapp;

import info.aduna.io.IOUtil;
import info.aduna.iteration.Iterations;
import info.aduna.text.StringUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import junit.framework.Test;
import junit.util.PropertyUtil;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.util.ModelUtil;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.Query;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultUtil;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.dawg.DAWGTestResultSetUtil;
import org.openrdf.query.impl.MutableTupleQueryResult;
import org.openrdf.query.impl.TupleQueryResultBuilder;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.StatementCollector;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepositoryConnection;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sail.webapp.client.HttpException;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Proxied test suite for SPARQL 1.1 Federated Query. In general, each test
 * loads some data into the KB and then issues a federated query. The core tests
 * are W3C tests and have the pattern <code>serviceXX</code>. There are also
 * tests developed for openrdf.
 * 
 * @see <a href="http://www.w3.org/2009/sparql/docs/tests/"> W3C Tests </a>
 * 
 * @see <a
 *      href="http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service/">
 *      Federated Query Test Suite </a>
 * 
 * @param <S>
 */
public class TestFederatedQuery<S extends IIndexManager> extends
        AbstractTestNanoSparqlClient<S> {
	
	public static final String TEST_RESOURCE_PATH = "/com/bigdata/rdf/sail/webapp/openrdf-service/";

    public TestFederatedQuery() {

    }

    public TestFederatedQuery(final String name) {

        super(name);

    }

    public static Test suite() {

       return ProxySuiteHelper.suiteWhenStandalone(TestFederatedQuery.class,
                 "test.*", TestMode.quads
//                 , TestMode.sids
//                 , TestMode.triples
                 );
        
    }

    @Override
    public void setUp() throws Exception {
        
        super.setUp();
        
        final Properties p = getProperties();
       
      /*
       * Note: I have seen some OOMs in the federated query test suite when
       * running it locally. These appear to come from extending the backing
       * buffer for a Transient store, so I am specifying a disk-backed mode
       * here.
       */
        p.setProperty(com.bigdata.journal.Options.BUFFER_MODE, BufferMode.DiskRW.toString());
        
        p.setProperty(com.bigdata.journal.Options.CREATE_TEMP_FILE, "true");
        
//        localRepository = m_repo;
        
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        
    }
    
    private String getNamespace(final int i) {
        
        if (i < 1)
            throw new IllegalArgumentException();
        
        return namespace + "_" + i;
        
    }
    
    /**
     * Get the repository url, initialized repositories are called
     * 
     * endpoint1 endpoint2 .. endpoint%MAX_ENDPOINTS%
     * 
     * @param i
     *            the index of the repository, starting with 1
     * @return
     */
    private String getRepositoryUrl(final int i) {

        return getRepositoryUrlBase() + i + "/sparql";

    }
    
    private String getRepositoryUrlBase() {
        
        return m_serviceURL + "/namespace/" + namespace + "_";

    }
    
    /**
    * Get the repository, initialized repositories are called
    * 
    * <pre>
    * endpoint1
    * endpoint2
    * ..
    * endpoint%MAX_ENDPOINTS%
    * </pre>
    * 
    * @param i
    *           the index of the repository, starting with 1
    * @return
    */
   private RemoteRepository getRepository(final int i) throws Exception {

      final String ns = getNamespace(i);

      try {
         boolean found = true;
         try {
            final Properties p = m_mgr.getRepositoryProperties(ns);
            assert p != null;
            found = true;
         } catch (HttpException ex) {
            if (ex.getStatusCode() == HttpServletResponse.SC_NOT_FOUND) {
               found = false;
            }
         }
         if (!found) {
            // Create the repository.
            final Properties p = PropertyUtil.flatCopy(getProperties());
            p.setProperty(RemoteRepositoryManager.OPTION_CREATE_KB_NAMESPACE,
                  ns);
            m_mgr.createRepository(ns, p);
         }
      } catch (Exception ex) {
         throw new RuntimeException(ex);
      }

      return m_mgr.getRepositoryForNamespace(ns);
        
    }
    
    /**
     * Prepare a particular test, and load the specified data.
     * 
     * Note: the repositories are cleared before loading data
     * 
     * @param localData
     *            a local data file that is added to local repository, use null
     *            if there is no local data
     * @param endpointData
     *            a list of endpoint data files, dataFile at index is loaded to
     *            endpoint%i%, use empty list for no remote data
     * @throws Exception
     */
    private void prepareTest(final String localData,
            final List<String> endpointData) throws Exception {

//        if (endpointData.size() > MAX_ENDPOINTS)
//            throw new RuntimeException("MAX_ENDPOINTs to low, "
//                    + endpointData.size()
//                    + " repositories needed. Adjust configuration");

        if (localData != null) {
            
            if (log.isInfoEnabled())
                log.info("Loading: " + localData + " into local repository");

            loadDataSet(m_repo, localData);
            
        }

        int i = 1; // endpoint id, start with 1
        for (String s : endpointData) {

            final RemoteRepository repo = getRepository(i);

            if (log.isInfoEnabled())
                log.info("Loading: " + s + " into " + getRepositoryUrl(i)
                        + " as " + repo);

            loadDataSet(repo, s);
            
            i++;
            
        }

    }
    
    /**
    * Load a dataset.
    * 
    * @param rep
    * @param datasetFile
    * @throws Exception
    */
    private void loadDataSet(final RemoteRepository rep, final String datasetFile)
            throws Exception {

		final URL datasetUri = TestFederatedQuery.class.getClass().getResource(
				TEST_RESOURCE_PATH + datasetFile);
		
		if(log.isInfoEnabled()) {
			log.info("datasetFile: " + datasetFile);
			log.info("datasetUri: " + datasetUri);
		}
      
       		rep.add(new AddOp(datasetUri.toExternalForm()));

//        final InputStream dataset = TestFederatedQuery.class
//                .getResourceAsStream(datasetFile);
//
//        if (dataset == null)
//            throw new IllegalArgumentException("Datasetfile not found: "
//                    + datasetFile);
//
//        try {
//
//            RepositoryConnection con = rep.getConnection();
//            try {
//                con.setAutoCommit(false);
//                // con.clear();
//                con.add(dataset, ""/* baseURI */,
//                        RDFFormat.forFileName(datasetFile));
//                con.commit();
//            } finally {
//                con.close();
//            }
//        } finally {
//            dataset.close();
//        }
        
    }

    public void testSimpleServiceQuery() throws Exception {

        // test setup
        final String EX_NS = "http://example.org/";
        final ValueFactory f = new ValueFactoryImpl();
        final URI bob = f.createURI(EX_NS, "bob");
        final URI alice = f.createURI(EX_NS, "alice");
        final URI william = f.createURI(EX_NS, "william");

        // clears the repository and adds new data
        prepareTest("simple-default-graph.ttl",
                Arrays.asList("simple.ttl"));
            
        final StringBuilder qb = new StringBuilder();
        qb.append(" SELECT * \n"); 
        qb.append(" WHERE { \n");
        qb.append("     SERVICE <" + getRepositoryUrl(1) + "> { \n");
        qb.append("             ?X <"   + FOAF.NAME + "> ?Y \n ");
        qb.append("     } \n ");
        qb.append("     ?X a <" + FOAF.PERSON + "> . \n");
        qb.append(" } \n");

        final BigdataSailRemoteRepositoryConnection conn = m_repo.getBigdataSailRemoteRepository().getConnection();
        
        try {

            final TupleQuery tq = conn.prepareTupleQuery(QueryLanguage.SPARQL,
                    qb.toString());

            final TupleQueryResult tqr = tq.evaluate();

            assertNotNull(tqr);
            assertTrue("No solutions.", tqr.hasNext());

            int count = 0;
            while (tqr.hasNext()) {
                final BindingSet bs = tqr.next();
                count++;

                final Value x = bs.getValue("X");
                final Value y = bs.getValue("Y");

                assertFalse(william.equals(x));

                assertTrue(bob.equals(x) || alice.equals(x));
                if (bob.equals(x)) {
                    f.createLiteral("Bob").equals(y);
                } else if (alice.equals(x)) {
                    f.createLiteral("Alice").equals(y);
                }
            }

            assertEquals(2, count);

        } finally {
            conn.close();
        }

    }

//  @Test
    public void test1() throws Exception{
        prepareTest("data01.ttl", Arrays.asList("data01endpoint.ttl"));
        execute("service01.rq", "service01.srx", false);          
    }

//  @Test
    public void test2() throws Exception {
//        if(false) {
//        final BigdataValueFactory f = (BigdataValueFactory) localSail.getValueFactory();
//        final BigdataValue[] values = new BigdataValue[] {
//                f.createURI("http://example.org/a"),
//                f.createURI("http://example.org/b"),
//        };
//        localSail.getDatabase().getLexiconRelation().addTerms(values, values.length, false/*readOnly*/);
//        }
        prepareTest(null, Arrays.asList("data02endpoint1.ttl", "data02endpoint2.ttl"));
        execute("service02.rq", "service02.srx", false);          
    }
    
//  @Test
    public void test3() throws Exception {      
        prepareTest(null, Arrays.asList("data03endpoint1.ttl", "data03endpoint2.ttl"));
        execute("service03.rq", "service03.srx", false);  
    }
    
    /*
     * FIXME This test has been disabled until we have resolution for the
     * question of whether or not the test is in error.
     */
////  @Test
//    public void test4() throws Exception {      
//        prepareTest("data04.ttl", Arrays.asList("data04endpoint.ttl"));
////        System.err.println(localSail.getDatabase().dumpStore());
//        execute("service04.rq", "service04.srx", false);
//    }
    
    // @Test
    public void test5() throws Exception {
        
        final URI serviceURI1 = new URIImpl(getRepositoryUrl(1));
        
        final URI serviceURI2 = new URIImpl(getRepositoryUrl(2));
        
        final URI serviceURI1_alias = new URIImpl(
                "http://localhost:18080/openrdf/repositories/endpoint1");
        
        final URI serviceURI2_alias = new URIImpl(
                "http://localhost:18080/openrdf/repositories/endpoint2");
        
        try {
            
            /*
             * Explicitly register aliases for the service URIs used in the
             * data05.ttl file.
             */

//            ServiceRegistry.getInstance().add(serviceURI1,
//                    new RemoteServiceFactoryImpl());
//
//            ServiceRegistry.getInstance().add(serviceURI2,
//                    new RemoteServiceFactoryImpl());

            ServiceRegistry.getInstance().addAlias(serviceURI1,
                    serviceURI1_alias);

            ServiceRegistry.getInstance().addAlias(serviceURI2,
                    serviceURI2_alias);

            prepareTest(
                    "data05.ttl",
                    Arrays.asList("data05endpoint1.ttl", "data05endpoint2.ttl"));

            execute("service05.rq", "service05.srx", false);

        } finally {
        
            // De-register those service aliases.
            ServiceRegistry.getInstance().remove(serviceURI1_alias);
            ServiceRegistry.getInstance().remove(serviceURI2_alias);
            
        }
        
    }
    
//  @Test
    public void test6() throws Exception { //     fail("FIXME RESTORE"); // FIXME RESTORE
        prepareTest(null, Arrays.asList("data06endpoint1.ttl"));
        execute("service06.rq", "service06.srx", false);          
    }
    
//  @Test
    public void test7() throws Exception { //     fail("FIXME RESTORE");// FIXME RESTORE
        // clears the repository and adds new data + execute
        prepareTest("data07.ttl", Collections.<String>emptyList());
        execute("service07.rq", "service07.srx", false);          
    }
    
//  @Test
    public void test8() throws Exception {
        /* test where the SERVICE expression is to be evaluated as ASK request */
        prepareTest("data08.ttl", Arrays.asList("data08endpoint.ttl"));
        execute("service08.rq", "service08.srx", false);          
    }   
    
//  @Test
    public void test9() throws Exception {
        /* test where the service endpoint is bound at runtime through BIND */
        prepareTest(null, Arrays.asList("data09endpoint.ttl"));
        execute("service09.rq", "service09.srx", false);          
    }
    
    /**
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/510">
     *      Blank nodes in SERVICE graph patterns </a>
     */
//  @Test
    public void test10() throws Exception {
        /* test how we deal with blank node */
        prepareTest("data10.ttl", Arrays.asList("data10endpoint.ttl"));
        execute("service10.rq", "service10.srx", false);          
    }

    /**
     * A variant of {@link #test10()} in which the blank node within the SERVICE
     * graph pattern is replaced by a variable. This query runs fine.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/510">
     * Blank nodes in SERVICE graph patterns </a>
     */
    public void test10b() throws Exception {
        /* test how we deal with blank node */
        prepareTest("data10.ttl", Arrays.asList("data10endpoint.ttl"));
        execute("service10b.rq", "service10.srx", false);          
    }
    
//  @Test
    public void test11() throws Exception {
        /* test vectored join with more intermediate results */
        // clears the repository and adds new data + execute
        prepareTest("data11.ttl", Arrays.asList("data11endpoint.ttl"));
        execute("service11.rq", "service11.srx", false);      
    }
    
    /**
     * This is a manual test to see the Fallback in action. Query asks DBpedia,
     * which does not support BINDINGS
     * <p>
     * Note: Bigdata is not doing automatic fallback. You have to explicitly
     * make a note of services which do not support SPARQL 1.1. Note: I have
     * taken this query out of CI. We are not doing fallback, so there is no
     * reason to issue the query. Also, the virtuoso cluster for dbpedia falls
     * over a lot ;-) causing this query to fail. For example:
     * <pre>
     * [junit] Caused by: java.io.IOException: Status Code=500, Status
     * Line=HTTP/1.1 500 SPARQL Request Failed, Response=Virtuoso 08C01 Error
     * CL...: Cluster could not connect to host 2 22202 error 111
     * </pre>
     */
//    public void test12() throws Exception {
//        final URI serviceURI = new URIImpl("http://dbpedia.org/sparql");
//        ServiceRegistry.getInstance().add(serviceURI,
//                new RemoteServiceFactoryImpl(false/* isSparql11 */));
//        try {
//            /* test vectored join with more intermediate results */
//            // clears the repository and adds new data + execute
//            prepareTest(PREFIX + "data12.ttl", Collections.<String> emptyList());
//            execute(PREFIX + "service12.rq", PREFIX + "service12.srx", false);
//        } finally {
//            ServiceRegistry.getInstance().remove(serviceURI);
//        }
//    }

    /*
     * FIXME I have disabled this test. For now, just workaround the problem by
     * having correlated joins.  It is not too much to ask!
     */
//    /**
//     * This test is failing due to an uncorrelated join between two SERVICE
//     * calls. Those SERVICE calls do not share any variables. The join is a full
//     * cross product. The problem is not that we fail to do the cross product.
//     * It is that we are doing it twice -- once for each empty solution flowing
//     * into the 2nd SERVICE call.
//     * 
//     * @see #test13b()
//     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/509">
//     *      Uncorrelated SERVICE JOINs. </a>
//     */
//    public void test13() throws Exception {
//        /* test for bug SES-899: cross product is required */
//        prepareTest(null, Arrays.asList("data13.ttl"));
//        execute("service13.rq", "service13.srx", false);              
//    }

    /**
     * Variant of {@link #test13()} which demonstrates a workaround for the
     * uncorrelated SERVICE joins by lifting the SERVICE calls into named
     * subqueries.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/509">
     *      Uncorrelated SERVICE JOINs. </a>
     */
    public void test13b() throws Exception {
        /* test for bug SES-899: cross product is required */
        prepareTest(null, Arrays.asList("data13.ttl"));
        execute("service13b.rq", "service13.srx", false);              
    }
    
    public void testEmptyServiceBlock() throws Exception {
        /* test for bug SES-900: nullpointer for empty service block */
        prepareTest(null, Arrays.asList("data13.ttl"));
        execute("service14.rq", "service14.srx", false);  
   }
    
    /**
     * Execute a testcase, both queryFile and expectedResultFile must be files 
     * located on the class path.
     * 
     * @param queryFile
     * @param expectedResultFile
     * @param checkOrder
     * @throws Exception
     */
    private void execute(final String queryFile,
            final String expectedResultFile, final boolean checkOrder)
            throws Exception {
        
        final BigdataSailRemoteRepositoryConnection conn = m_repo
               .getBigdataSailRemoteRepository().getConnection();
       
        try {
           
           final String baseURI = getRepositoryUrlBase();
           
           String queryString = readQueryString(queryFile);

           /*
            * Replace the constants in the query strings with the actual service
            * end point.
            */
           queryString = queryString.replace(
                   "http://localhost:18080/openrdf/repositories/endpoint",
                   baseURI);

           /*
            * Figure out what kind of query this is.
            */
         final QueryType queryType;
         {
            final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
                  .getResourceLocator().locate(namespace, ITx.READ_COMMITTED);

            final ASTContainer astContainer = new Bigdata2ASTSPARQLParser()
                  .parseQuery2(queryString, baseURI);

            queryType = astContainer.getOriginalAST().getQueryType();
         }

         switch (queryType) {
         case ASK: {
            // TODO implement if needed
            throw new RuntimeException("Not yet supported (boolean) "
                  + queryString);
         }
         case CONSTRUCT:
         case DESCRIBE: {

            final Query query = conn.prepareGraphQuery(QueryLanguage.SPARQL,
                  queryString);

            final GraphQueryResult gqr = ((GraphQuery) query).evaluate();

            final Set<Statement> queryResult = Iterations.asSet(gqr);

            final Set<Statement> expectedResult = readExpectedGraphQueryResult(expectedResultFile);

            compareGraphs(queryResult, expectedResult);

            break;
         }
         case SELECT: {
            final Query query = conn.prepareTupleQuery(QueryLanguage.SPARQL,
                  queryString);

            final TupleQueryResult queryResult = ((TupleQuery) query)
                  .evaluate();

            final TupleQueryResult expectedResult = readExpectedTupleQueryResult(expectedResultFile);

            compareTupleQueryResults(queryResult, expectedResult, checkOrder);
            break;
         }
         default:
            throw new RuntimeException("Unexpected query type: " + queryString);
         }

      } finally {
         conn.close();
      }
   }
    
    /**
     * Read the query string from the specified resource
     * 
     * @param queryResource
     * @return
     * @throws RepositoryException
     * @throws IOException
     */
    private String readQueryString(final String queryResource) throws RepositoryException, IOException {
		final InputStream stream = TestFederatedQuery.class
				.getResourceAsStream(TEST_RESOURCE_PATH + queryResource);
        try {
            return IOUtil.readString(new InputStreamReader(stream, "UTF-8"));
        } finally {
            stream.close();
        }
    }
    
    /**
     * Read the expected tuple query result from the specified resource
     * 
     * @param queryResource
     * @return
     * @throws RepositoryException
     * @throws IOException
     */
    private TupleQueryResult readExpectedTupleQueryResult(final String resultFile)    throws Exception
    {
       final TupleQueryResultFormat tqrFormat = QueryResultIO.getParserFormatForFileName(resultFile);
    
        if (tqrFormat != null) {
           final InputStream in = TestFederatedQuery.class.getResourceAsStream(TEST_RESOURCE_PATH + resultFile);
            try {
               final TupleQueryResultParser parser = QueryResultIO.createParser(tqrFormat);
                parser.setValueFactory(ValueFactoryImpl.getInstance());
    
                final TupleQueryResultBuilder qrBuilder = new TupleQueryResultBuilder();
                parser.setTupleQueryResultHandler(qrBuilder);
    
                parser.parse(in);
                return qrBuilder.getQueryResult();
            }
            finally {
                in.close();
            }
        }
        else {
           final Set<Statement> resultGraph = readExpectedGraphQueryResult(resultFile);
            return DAWGTestResultSetUtil.toTupleQueryResult(resultGraph);
        }
    }
    
    /**
     * Read the expected graph query result from the specified resource
     * 
     * @param resultFile
     * @return
     * @throws Exception
     */
    private Set<Statement> readExpectedGraphQueryResult(String resultFile) throws Exception
    {
        final RDFFormat rdfFormat = Rio.getParserFormatForFileName(resultFile);
    
        if (rdfFormat != null) {
            final RDFParser parser = Rio.createParser(rdfFormat);
            parser.setDatatypeHandling(DatatypeHandling.IGNORE);
            parser.setPreserveBNodeIDs(true);
            parser.setValueFactory(ValueFactoryImpl.getInstance());
    
            final Set<Statement> result = new LinkedHashSet<Statement>();
            parser.setRDFHandler(new StatementCollector(result));
    
			final InputStream in = TestFederatedQuery.class
					.getResourceAsStream(TEST_RESOURCE_PATH + resultFile);
            try {
                parser.parse(in, null);     // TODO check
            }
            finally {
                in.close();
            }
    
            return result;
        }
        else {
            throw new RuntimeException("Unable to determine file type of results file");
        }
    }

    /**
     * Compare two tuple query results
     * 
     * @param queryResult
     * @param expectedResult
     * @param checkOrder
     * @throws Exception
     * 
     * FIXME Use the code from {@link AbstractDataDrivenSPARQLTestCase}
     */
    private void compareTupleQueryResults(TupleQueryResult queryResult, TupleQueryResult expectedResult, boolean checkOrder)
        throws Exception
    {
        // Create MutableTupleQueryResult to be able to re-iterate over the
        // results
        MutableTupleQueryResult queryResultTable = new MutableTupleQueryResult(queryResult);
        MutableTupleQueryResult expectedResultTable = new MutableTupleQueryResult(expectedResult);
    
        boolean resultsEqual;
        
        resultsEqual = QueryResultUtil.equals(queryResultTable, expectedResultTable);
        
        if (checkOrder) {
            // also check the order in which solutions occur.
            queryResultTable.beforeFirst();
            expectedResultTable.beforeFirst();

            while (queryResultTable.hasNext()) {
                BindingSet bs = queryResultTable.next();
                BindingSet expectedBs = expectedResultTable.next();
                
                if (! bs.equals(expectedBs)) {
                    resultsEqual = false;
                    break;
                }
            }
        }
        
    
        if (!resultsEqual) {
            queryResultTable.beforeFirst();
            expectedResultTable.beforeFirst();
    
            /*
             * StringBuilder message = new StringBuilder(128);
             * message.append("\n============ "); message.append(getName());
             * message.append(" =======================\n");
             * message.append("Expected result: \n"); while
             * (expectedResultTable.hasNext()) {
             * message.append(expectedResultTable.next()); message.append("\n"); }
             * message.append("============="); StringUtil.appendN('=',
             * getName().length(), message);
             * message.append("========================\n"); message.append("Query
             * result: \n"); while (queryResultTable.hasNext()) {
             * message.append(queryResultTable.next()); message.append("\n"); }
             * message.append("============="); StringUtil.appendN('=',
             * getName().length(), message);
             * message.append("========================\n");
             */
    
            List<BindingSet> queryBindings = Iterations.asList(queryResultTable);
            
            List<BindingSet> expectedBindings = Iterations.asList(expectedResultTable);
    
            List<BindingSet> missingBindings = new ArrayList<BindingSet>(expectedBindings);
            missingBindings.removeAll(queryBindings);
    
            List<BindingSet> unexpectedBindings = new ArrayList<BindingSet>(queryBindings);
            unexpectedBindings.removeAll(expectedBindings);
    
            StringBuilder message = new StringBuilder(128);
            message.append("\n============ ");
            message.append(getName());
            message.append(" =======================\n");
    
            if (!missingBindings.isEmpty()) {
    
                message.append("Missing bindings: \n");
                for (BindingSet bs : missingBindings) {
                    message.append(bs);
                    message.append("\n");
                }
    
                message.append("=============");
                StringUtil.appendN('=', getName().length(), message);
                message.append("========================\n");
            }
    
            if (!unexpectedBindings.isEmpty()) {
                message.append("Unexpected bindings: \n");
                for (BindingSet bs : unexpectedBindings) {
                    message.append(bs);
                    message.append("\n");
                }
    
                message.append("=============");
                StringUtil.appendN('=', getName().length(), message);
                message.append("========================\n");
            }
            
            if (checkOrder && missingBindings.isEmpty() && unexpectedBindings.isEmpty()) {
                message.append("Results are not in expected order.\n");
                message.append(" =======================\n");
                message.append("query result: \n");
                for (BindingSet bs: queryBindings) {
                    message.append(bs);
                    message.append("\n");
                }
                message.append(" =======================\n");
                message.append("expected result: \n");
                for (BindingSet bs: expectedBindings) {
                    message.append(bs);
                    message.append("\n");
                }
                message.append(" =======================\n");
    
                System.out.print(message.toString());
            }
    
            log.error(message.toString());
            fail(message.toString());
        }
        /* debugging only: print out result when test succeeds 
        else {
            queryResultTable.beforeFirst();
    
            List<BindingSet> queryBindings = Iterations.asList(queryResultTable);
            StringBuilder message = new StringBuilder(128);
    
            message.append("\n============ ");
            message.append(getName());
            message.append(" =======================\n");
    
            message.append(" =======================\n");
            message.append("query result: \n");
            for (BindingSet bs: queryBindings) {
                message.append(bs);
                message.append("\n");
            }
            
            System.out.print(message.toString());
        }
        */
    }
    
    /**
     * Compare two graphs
     * 
     * @param queryResult
     * @param expectedResult
     * @throws Exception
     */
    private void compareGraphs(Set<Statement> queryResult, Set<Statement> expectedResult)
        throws Exception
    {
        if (!ModelUtil.equals(expectedResult, queryResult)) {
            // Don't use RepositoryUtil.difference, it reports incorrect diffs
            /*
             * Collection<? extends Statement> unexpectedStatements =
             * RepositoryUtil.difference(queryResult, expectedResult); Collection<?
             * extends Statement> missingStatements =
             * RepositoryUtil.difference(expectedResult, queryResult);
             * StringBuilder message = new StringBuilder(128);
             * message.append("\n=======Diff: "); message.append(getName());
             * message.append("========================\n"); if
             * (!unexpectedStatements.isEmpty()) { message.append("Unexpected
             * statements in result: \n"); for (Statement st :
             * unexpectedStatements) { message.append(st.toString());
             * message.append("\n"); } message.append("============="); for (int i =
             * 0; i < getName().length(); i++) { message.append("="); }
             * message.append("========================\n"); } if
             * (!missingStatements.isEmpty()) { message.append("Statements missing
             * in result: \n"); for (Statement st : missingStatements) {
             * message.append(st.toString()); message.append("\n"); }
             * message.append("============="); for (int i = 0; i <
             * getName().length(); i++) { message.append("="); }
             * message.append("========================\n"); }
             */
            StringBuilder message = new StringBuilder(128);
            message.append("\n============ ");
            message.append(getName());
            message.append(" =======================\n");
            message.append("Expected result: \n");
            for (Statement st : expectedResult) {
                message.append(st.toString());
                message.append("\n");
            }
            message.append("=============");
            StringUtil.appendN('=', getName().length(), message);
            message.append("========================\n");
    
            message.append("Query result: \n");
            for (Statement st : queryResult) {
                message.append(st.toString());
                message.append("\n");
            }
            message.append("=============");
            StringUtil.appendN('=', getName().length(), message);
            message.append("========================\n");
    
            log.error(message.toString());
            fail(message.toString());
        }
    }
    
}
