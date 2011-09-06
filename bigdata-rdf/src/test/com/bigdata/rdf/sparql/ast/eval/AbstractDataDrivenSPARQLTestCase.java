/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
 * Created on Sep 5, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import info.aduna.iteration.Iterations;
import info.aduna.text.StringUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.model.util.ModelUtil;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryResultUtil;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.dawg.DAWGTestResultSetUtil;
import org.openrdf.query.impl.MutableTupleQueryResult;
import org.openrdf.query.impl.TupleQueryResultBuilder;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.BooleanQueryResultParserRegistry;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.rio.helpers.StatementCollector;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.QueryRoot;

/**
 * Abstract base class for data driven test suites.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: AbstractDataDrivenSPARQLTestCase.java 5130 2011-09-05 16:54:20Z
 *          thompsonbry $
 * 
 *          TODO Add tests for triples-only and sids modes.
 * 
 *          TODO Support manifest driven test suite.
 */
public class AbstractDataDrivenSPARQLTestCase extends
        AbstractASTEvaluationTestCase {

    private static final Logger log = Logger
            .getLogger(AbstractDataDrivenSPARQLTestCase.class);
    
    /**
     * 
     */
    public AbstractDataDrivenSPARQLTestCase() {
    }

    /**
     * @param name
     */
    public AbstractDataDrivenSPARQLTestCase(String name) {
        super(name);
    }

    /**
     * Data-driven unit tests for SPARQL queries.
     * <p>
     * Note: This class was derived from the openrdf SPARQLQueryTest file (Aduna
     * BSD style license).
     */
    public class TestHelper {

        private final String testURI;
        
        private final String queryFileURL;
        private final String dataFileURL;
        private final String resultFileURL;

        private final boolean laxCardinality = false;
        private final boolean checkOrder = false;
        
        private final String queryStr;

        private final QueryRoot queryRoot;
        
        private final AST2BOpContext context;
        
        private final QueryRoot optimizedQuery;
        
        private final PipelineOp queryPlan;

        /**
         * 
         * @param testURI
         * @throws Exception
         * 
         */
        public TestHelper(final String testURI) throws Exception {
            
            this(testURI, testURI + ".rq", testURI + ".trig", testURI + ".srx");
            
        }

        /**
         * 
         * @param testURI
         * @param queryFileURL
         * @param dataFileURL
         * @param resultFileURL
         * @throws Exception
         */
        public TestHelper(final String testURI, final String queryFileURL,
                final String dataFileURL, final String resultFileURL)
                throws Exception {

            if (log.isInfoEnabled())
                log.info("testURI:\n" + testURI);

            this.testURI = testURI;
            this.queryFileURL = queryFileURL;
            this.dataFileURL = dataFileURL;
            this.resultFileURL = resultFileURL;

            this.queryStr = getResourceAsString(queryFileURL);

            if (log.isInfoEnabled())
                log.info("query:\n" + queryStr);

            final long nparsed = loadData(dataFileURL);

            if (log.isInfoEnabled())
                log.info("Loaded " + nparsed + " statements from "
                        + dataFileURL);

            queryRoot = new Bigdata2ASTSPARQLParser(store).parseQuery2(
                    queryStr, null/* baseUri */);

            if (log.isInfoEnabled())
                log.info("AST:\n" + queryRoot);

            this.context = new AST2BOpContext(queryRoot, store);

            /*
             * Run the query optimizer first so we have access to the rewritten
             * query plan.
             */
            this.optimizedQuery = context.optimizedQuery = (QueryRoot) context.optimizers
                    .optimize(context, queryRoot, null/* bindingSet[] */);

            if (log.isInfoEnabled())
                log.info("optimizedQuery:\n" + optimizedQuery);

            queryPlan = AST2BOpUtility.convert(context);

            if (log.isInfoEnabled())
                log.info("queryPlan:\n" + queryPlan);

        }

        /**
         * Run a data-driven SPARQL test.
         * 
         * @param resource
         *            The base name of the resource. It will locate the query
         *            (.rq), the data (.trig), and the expected resources (.srx)
         *            using this base name.
         * 
         * @throws Exception
         *             if the resources can not be found, if the query does not
         *             produce the expected results, etc.
         */
        public void runTest() throws Exception {

            final AST2BOpContext context = new AST2BOpContext(queryRoot, store);

            switch (queryRoot.getQueryType()) {
            case SELECT: {

                final TupleQueryResult expectedResult = readExpectedTupleQueryResult();

                final IVariable<?>[] projected = queryRoot.getProjection()
                        .getProjectionVars();

                final TupleQueryResult queryResult = ASTEvalHelper
                        .evaluateTupleQuery(store, queryPlan,
                                new QueryBindingSet(), context.queryEngine,
                                projected);

                compareTupleQueryResults(queryResult, expectedResult);

                break;
            }
            case DESCRIBE:
            case CONSTRUCT: {
                
                final Set<Statement> expectedResult = readExpectedGraphQueryResult();

                final GraphQueryResult gqr = ASTEvalHelper.evaluateGraphQuery(
                        store, //
                        queryPlan, //
                        new QueryBindingSet(),//
                        context.queryEngine, //
                        queryRoot.getProjection().getProjectionVars(),
                        queryRoot.getPrefixDecls(), //
                        queryRoot.getConstruct()//
                        );

                final Set<Statement> queryResult = Iterations.asSet(gqr);

                compareGraphs(queryResult, expectedResult);
                
                break;
                
            }
            case ASK: {
                
                final boolean queryResult = ASTEvalHelper.evaluateBooleanQuery(
                        store, queryPlan, new QueryBindingSet(),
                        context.queryEngine);
                
                final boolean expectedResult = readExpectedBooleanQueryResult();
                
                assertEquals(expectedResult, queryResult);
                
                break;
                
            }
            default:
                throw new RuntimeException("Unknown query type: "
                        + queryRoot.getQueryType());
            }

        }

        public TupleQueryResult readExpectedTupleQueryResult() throws Exception {

            final TupleQueryResultFormat tqrFormat = QueryResultIO
                    .getParserFormatForFileName(resultFileURL);

            if (tqrFormat != null) {
                
                final InputStream in = getResourceAsStream(resultFileURL);
                
                try {
                
                    final TupleQueryResultParser parser = QueryResultIO
                            .createParser(tqrFormat);
                    
                    parser.setValueFactory(store.getValueFactory());

                    final TupleQueryResultBuilder qrBuilder = new TupleQueryResultBuilder();
                    
                    parser.setTupleQueryResultHandler(qrBuilder);

                    parser.parse(in);
                    
                    return qrBuilder.getQueryResult();
                    
                } finally {
                    
                    in.close();
                    
                }

            } else {

                final Set<Statement> resultGraph = readExpectedGraphQueryResult();

                return DAWGTestResultSetUtil.toTupleQueryResult(resultGraph);

            }

        }

        public boolean readExpectedBooleanQueryResult() throws Exception {
            
            final BooleanQueryResultFormat bqrFormat = BooleanQueryResultParserRegistry
                    .getInstance().getFileFormatForFileName(resultFileURL);

            if (bqrFormat != null) {

                final InputStream in = getResourceAsStream(resultFileURL);
                
                try {
                
                    return QueryResultIO.parse(in, bqrFormat);
                    
                } finally {
                    
                    in.close();
                    
                }

            } else {

                final Set<Statement> resultGraph = readExpectedGraphQueryResult();
                
                return DAWGTestResultSetUtil.toBooleanQueryResult(resultGraph);
                
            }

        }

        public Set<Statement> readExpectedGraphQueryResult() throws Exception {

            final RDFFormat rdfFormat = Rio
                    .getParserFormatForFileName(resultFileURL);

            if (rdfFormat != null) {

                final RDFParser parser = Rio.createParser(rdfFormat);
                parser.setDatatypeHandling(DatatypeHandling.IGNORE);
                parser.setPreserveBNodeIDs(true);
                parser.setValueFactory(store.getValueFactory());

                final Set<Statement> result = new LinkedHashSet<Statement>();
                
                parser.setRDFHandler(new StatementCollector(result));

                final InputStream in = getResourceAsStream(resultFileURL);
                
                try {
                
                    parser.parse(in, resultFileURL);
                    
                } finally {
                    
                    in.close();
                    
                }

                return result;
                
            } else {

                throw new RuntimeException(
                        "Unable to determine file type of results file: "
                                + resultFileURL);

            }
            
        }

        private void compareTupleQueryResults(
                final TupleQueryResult queryResult,
                final TupleQueryResult expectedResult) throws Exception {
            
            /*
             * Create MutableTupleQueryResult to be able to re-iterate over the
             * results.
             */
            
            final MutableTupleQueryResult queryResultTable = new MutableTupleQueryResult(queryResult);
            
            final MutableTupleQueryResult expectedResultTable = new MutableTupleQueryResult(expectedResult);

            boolean resultsEqual;
            if (laxCardinality) {
                resultsEqual = QueryResultUtil.isSubset(queryResultTable, expectedResultTable);
            }
            else {
                resultsEqual = QueryResultUtil.equals(queryResultTable, expectedResultTable);
                
                if (checkOrder) {
                    // also check the order in which solutions occur.
                    queryResultTable.beforeFirst();
                    expectedResultTable.beforeFirst();

                    while (queryResultTable.hasNext()) {
                        final BindingSet bs = queryResultTable.next();
                        final BindingSet expectedBs = expectedResultTable.next();
                        
                        if (! bs.equals(expectedBs)) {
                            resultsEqual = false;
                            break;
                        }
                    }
                }
            }

            if (!resultsEqual) {
                queryResultTable.beforeFirst();
                expectedResultTable.beforeFirst();

// Note: code block shows the expected and actual results.
//                
//                if(false) {
//                    StringBuilder message = new StringBuilder(2048);
//                    message.append("\n============ ");
//                    message.append(getName());
//                    message.append(" =======================\n");
//                    message.append("Expected result: \n");
//                    while (expectedResultTable.hasNext()) {
//                        message.append(expectedResultTable.next());
//                        message.append("\n");
//                    }
//                    message.append("=============");
//                    StringUtil.appendN('=', getName().length(), message);
//                    message.append("========================\n");
//                    message.append("Query result: \n");
//                    while (queryResultTable.hasNext()) {
//                        message.append(queryResultTable.next());
//                        message.append("\n");
//                    }
//                    message.append("=============");
//                    StringUtil.appendN('=', getName().length(), message);
//                    message.append("========================\n");
//                    System.err.println(message);
//                }

                final List<BindingSet> queryBindings = Iterations.asList(queryResultTable);
                final List<BindingSet> expectedBindings = Iterations.asList(expectedResultTable);

                final List<BindingSet> missingBindings = new ArrayList<BindingSet>(expectedBindings);
                missingBindings.removeAll(queryBindings);

                final List<BindingSet> unexpectedBindings = new ArrayList<BindingSet>(queryBindings);
                unexpectedBindings.removeAll(expectedBindings);

                final StringBuilder message = new StringBuilder(2048);
                message.append("\n");
                message.append(testURI);
                message.append("\n");
                message.append(getName());
                message.append("\n===================================\n");

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

//                    RepositoryConnection con = ((DatasetRepository)dataRep).getDelegate().getConnection();
//                    System.err.println(con.getClass());
//                    try {
                    message.append("\n===================================\n");
                    message.append("\nqueryRoot:\n");
                    message.append(BOpUtility.toString(queryRoot));
                    message.append("\noptimizedQuery:\n");
                    message.append(BOpUtility.toString(optimizedQuery));
                    message.append("\nqueryPlan:\n");
                    message.append(BOpUtility.toString(queryPlan));
                    message.append("\n===================================\n");
                    message.append("database dump:\n");
                    message.append(store.dumpStore());
//                        RepositoryResult<Statement> stmts = con.getStatements(null, null, null, false);
//                        while (stmts.hasNext()) {
//                            message.append(stmts.next());
//                            message.append("\n");
//                        }
//                    } finally {
//                        con.close();
//                    }
                    
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
            
        protected InputStream getResourceAsStream(final String resource) {

            // try the classpath
            InputStream is = getClass().getResourceAsStream(resource);

            if (is == null) {

                // Searching for the resource from the root of the class
                // returned
                // by getClass() (relative to the class' package) failed.
                // Next try searching for the desired resource from the root
                // of the jar; that is, search the jar file for an exact match
                // of the input string.
                is = getClass().getClassLoader().getResourceAsStream(resource);

            }

            if (is == null) {

                final File file = new File(resource);

                if (file.exists()) {

                    try {

                        is = new FileInputStream(resource);

                    } catch (FileNotFoundException e) {

                        throw new RuntimeException(e);

                    }

                }

            }

            if (is == null) {

                try {

                    is = new URL(resource).openStream();

                } catch (MalformedURLException e) {

                    /*
                     * Ignore. we will handle the problem below if this was not
                     * a URL.
                     */

                } catch (IOException e) {

                    throw new RuntimeException(e);

                }

            }

            if (is == null)
                throw new RuntimeException("Not found: " + resource);

            return is;

        }

        /**
         * Return the contents of the resource.
         * 
         * @param resource
         *            The resource.
         * 
         * @return It's contents.
         */
        protected String getResourceAsString(final String resource) {

            final StringBuilder sb = new StringBuilder();

            final InputStream is = getResourceAsStream(resource);

            if (is == null)
                throw new RuntimeException("Not found: " + resource);

            try {

                final LineNumberReader r = new LineNumberReader(
                        new InputStreamReader(is));

                String s;
                while ((s = r.readLine()) != null) {

                    sb.append(s);

                    sb.append("\n");

                }

                return sb.toString();

            } catch (IOException e) {

                throw new RuntimeException(e);

            } finally {

                try {

                    if (is != null)
                        is.close();

                } catch (IOException e) {

                    throw new RuntimeException(e);

                }

            }

        }

        /**
         * Load some RDF data.
         * 
         * @param resource
         *            The resource whose data will be loaded.
         * 
         * @return The #of statements parsed from the source. If there are
         *         duplicate told statements, then there may be fewer statements
         *         written onto the KB.
         */
        protected long loadData(final String resource) {

            final RDFFormat rdfFormat = RDFFormat.forFileName(resource);

            final RDFParserFactory rdfParserFactory = RDFParserRegistry
                    .getInstance().get(rdfFormat);

            final RDFParser rdfParser = rdfParserFactory.getParser();

            rdfParser.setValueFactory(store.getValueFactory());

            rdfParser.setVerifyData(true);

            rdfParser.setStopAtFirstError(true);

            rdfParser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

            final AddStatementHandler handler = new AddStatementHandler();

            rdfParser.setRDFHandler(handler);

            /*
             * Run the parser, which will cause statements to be inserted.
             */

            final InputStream is = getResourceAsStream(resource);

            try {

                rdfParser.parse(is, baseURI);

                return handler.close();

            } catch (Exception e) {

                throw new RuntimeException(e);

            } finally {

                try {

                    is.close();

                } catch (IOException e) {

                    throw new RuntimeException(e);

                }

            }

        }

        /**
         * Helper class adds statements to the sail as they are visited by a
         * parser.
         */
        private class AddStatementHandler extends RDFHandlerBase {

            private final StatementBuffer<Statement> buffer;

            private long n = 0L;

            public AddStatementHandler() {

                buffer = new StatementBuffer<Statement>(store, 100/* capacity */);

            }

            public void handleStatement(final Statement stmt)
                    throws RDFHandlerException {

                if (log.isDebugEnabled())
                    log.debug(stmt.toString());

                buffer.add(stmt.getSubject(), stmt.getPredicate(),
                        stmt.getObject(), stmt.getContext(),
                        StatementEnum.Explicit);

                n++;

            }

            /**
             * 
             * @return The #of statements visited by the parser.
             */
            public long close() {

                buffer.flush();

                return n;

            }

        }

    }

}
