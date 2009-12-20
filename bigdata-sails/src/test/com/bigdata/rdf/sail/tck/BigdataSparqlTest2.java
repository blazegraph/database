package com.bigdata.rdf.sail.tck;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.impl.DatasetImpl;

public class BigdataSparqlTest2 extends TestCase {
    
    protected static final Logger log = Logger.getLogger(BigdataSparqlTest2.class);
    
    static final private String failure = 
        "graph: file:/C:/DOCUME~1/mike/LOCALS~1/Temp/sparql5850/testcases-dawg/data-r2/optional/complex-data-1.ttl\n" +
        "graph: file:/C:/DOCUME~1/mike/LOCALS~1/Temp/sparql5850/testcases-dawg/data-r2/optional/complex-data-2.ttl\n" +
        "queryFile: file:/C:/DOCUME~1/mike/LOCALS~1/Temp/sparql5850/testcases-dawg/data-r2/optional/q-opt-complex-3.rq\n" +
        "resultFile: file:/C:/DOCUME~1/mike/LOCALS~1/Temp/sparql5850/testcases-dawg/data-r2/optional/result-opt-complex-3.ttl";
    
    static final private String testURI =
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-12";
    
    static final private Collection<String> testURIs = Arrays.asList(new String[] {
/*
//      busted with EvalStrategy1
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#nested-opt-2",
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#filter-scope-1",
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#join-scope-1",
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional/manifest#dawg-optional-complex-4",
        
//      busted with EvalStrategy2 with LeftJoin enabled
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-12",
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#nested-opt-1",
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#opt-filter-1",
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#opt-filter-2",
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional/manifest#dawg-optional-complex-3",
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional-filter/manifest#dawg-optional-filter-001",
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional-filter/manifest#dawg-optional-filter-004",
*/        
//      Dataset crap
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/dataset/manifest#dawg-dataset-01"
        
    });
    
    protected static BigdataSparqlTest getSingleTest(String testURI) throws Exception {
        
        BigdataSparqlTest test = null;
        TestSuite suite = (TestSuite) BigdataSparqlTest.suite();
        Enumeration e1 = suite.tests();
        while (e1.hasMoreElements()) {
            suite = (TestSuite) e1.nextElement();
            Enumeration e2 = suite.tests();
            while (e2.hasMoreElements()) {
                 test = (BigdataSparqlTest) e2.nextElement();
                 if (testURI.equals(test.getTestURI())) {
                     return test;
                 }
            }
        }
        
        throw new RuntimeException("could not find a test with that URI");
        
    }
    
    public static Test suite() throws Exception {

        TestSuite suite = new TestSuite();
/*
 * more efficient, but in the wrong order
        BigdataSparqlTest test = null;
        Enumeration e1 = ((TestSuite) BigdataSparqlTest.suite()).tests();
        while (e1.hasMoreElements()) {
            Enumeration e2 = ((TestSuite) e1.nextElement()).tests();
            while (e2.hasMoreElements()) {
                 test = (BigdataSparqlTest) e2.nextElement();
                 log.info(test.getTestURI());
                 if (testURIs.contains(test.getTestURI())) {
                     suite.addTest(test);
                 }
            }
        }
 */
        for (String s : testURIs) {
            suite.addTest(getSingleTest(s));
        }
        
        return suite;
        
    }

/*    
    public void testSingleQuery() throws Exception {
        
        String queryFile = null;
        String resultFile = null;
        DatasetImpl dataset = new DatasetImpl();
        String line = null;
        BufferedReader reader = new BufferedReader(new StringReader(failure));
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("graph: ")) {
                dataset.addDefaultGraph(new URIImpl(line.substring("graph: ".length())));
            } else if (line.startsWith("queryFile: ")) {
                queryFile = line.substring("queryFile: ".length());
            } else if (line.startsWith("resultFile: ")) {
                resultFile = line.substring("resultFile: ".length());
            } else {
                throw new RuntimeException("unexpected line: " + line);
            }
        }
        
        assert(queryFile != null && resultFile != null && 
                dataset.getDefaultGraphs().size() > 0);
        
        log.info(queryFile);
        log.info(resultFile);
        for (URI graph : dataset.getDefaultGraphs()) {
            log.info(graph);
        }
        
        BigdataSparqlTest test = 
            // new BigdataSparqlTest("", "", queryFile, resultFile, dataset);
            getSingleTest(testURI);
        
        try {
            
            test.setUp();
            log.info("query:\n" + test.getQueryString());
            
            test.runTest();
            
        } catch(Exception ex) {
            
            ex.printStackTrace();
            test.tearDown();
            
        }
        
    }
*/
}
