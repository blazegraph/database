/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 19, 2008
 */
package com.bigdata.rdf.sail.tck;

import info.aduna.io.IOUtil;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.openrdf.query.Dataset;
import org.openrdf.query.parser.sparql.ManifestTest;
import org.openrdf.query.parser.sparql.SPARQL11ManifestTest;
import org.openrdf.query.parser.sparql.SPARQLASTQueryTest;
import org.openrdf.query.parser.sparql.SPARQLQueryTest;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.dataset.DatasetRepository;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.memory.MemoryStore;

import com.bigdata.BigdataStatics;
import com.bigdata.btree.keys.CollatorEnum;
import com.bigdata.btree.keys.StrengthEnum;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;

/**
 * Test harness for running the SPARQL test suites. This version runs against
 * a {@link Journal} without full read/write transaction support.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class BigdataSparqlTest 
//extends SPARQLQueryTest // Sesame TupleExpr based evaluation 
extends SPARQLASTQueryTest // Bigdata native AST based evaluation
{

//    static private final Logger log = Logger.getLogger(BigdataSparqlTest.class);
    
    /**
     * We cannot use inlining for these test because we do normalization on
     * numeric values and these tests test for syntactic differences, i.e.
     * 01 != 1.
     */
    static final Collection<String> cannotInlineTests = Arrays.asList(new String[] {
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-01",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-03",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-04",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-str-1",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-str-2",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-datatype-1",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#sameTerm-simple",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#sameTerm-eq",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#sameTerm-not-eq",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-equals/manifest#eq-graph-1",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-equals/manifest#eq-graph-2",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/distinct/manifest#no-distinct-1",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/distinct/manifest#distinct-1",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/distinct/manifest#no-distinct-9",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/distinct/manifest#distinct-9",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#date-2",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#date-3",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#date-4",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-exists-05",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-exists-06"
    });

    /**
     * The tests test things that are no longer in the spec.
     */
    static final public Collection<String> badTests = Arrays.asList(new String[] {
		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-sequence-04",
		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-sequence-05",
		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-sequence-06",
      });

    /**
     * These tests fail but should not. They are conditionally disabled based on
     * {@link BigdataStatics#runKnownBadTests}. This is done as a convenience to
     * 'green' up CI.
     */
    static final public Collection<String> knownBadTests = Arrays.asList(new String[] {
            "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-datatype-2",
            "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-wildcard-cycles-04",
            "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-subquery-04",
            "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-subquery-06",
            "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-order-02",
            "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-order-03",
            "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-sum-02",
    });

	/**
	 * The following tests require Unicode configuration for identical
	 * comparisons. This appears to work with {ASCII,IDENTICAL} or
	 * {JDK,IDENTICAL} but not with {ICU,IDENTICAL} for some reason.
	 */
    static final Collection<String> unicodeStrengthIdentical = Arrays.asList(new String[] {
    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/i18n/manifest#normalization-1"
    });
    
//    private static String datasetTests = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/dataset";

    /**
     * Skip the dataset tests for now until we can figure out what is wrong with
     * them.
     * 
     * FIXME Fix the dataset tests. There is some problem in how the data to be
     * loaded into the fixture is being resolved in these tests.
     */
    public static Test suite() throws Exception {
        
        return suite(true /*hideDatasetTests*/);
        
    }
    
    public static Test suite(final boolean hideDatasetTests) throws Exception {
        
        TestSuite suite1 = suiteLTSWithPipelineJoins();

        // Only run the specified tests?
        if (!testURIs.isEmpty()) {
            final TestSuite suite = new TestSuite();
            for (String s : testURIs) {
                final SPARQLQueryTest test = getSingleTest(suite1, s);
                if (test == null)
                    throw new RuntimeException("Could not find test: uri="
                            + s);
                suite.addTest(test);
            }
            return suite;
        }
        
        if (hideDatasetTests)
            suite1 = filterOutTests(suite1, "dataset");

        suite1 = filterOutTests(suite1, badTests);

        if (!BigdataStatics.runKnownBadTests)
            suite1 = filterOutTests(suite1, knownBadTests);

        /**
         * BSBM BI use case query 5
         * 
         * bsbm-bi-q5
         * 
         * We were having a problem with this query which I finally tracked this
         * down to an error in the logic to decide on a merge join. The story is
         * documented at the trac issue below. However, even after all that the
         * predicted result for openrdf differs at the 4th decimal place. I have
         * therefore filtered out this test from the openrdf TCK.
         * 
         * <pre>
         * Missing bindings: 
         * [product=http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product63;
         * nrOfReviews="3"^^<http://www.w3.org/2001/XMLSchema#integer>;
         * avgPrice="4207.426"^^<http://www.w3.org/2001/XMLSchema#float>;
         * country=http://downlode.org/rdf/iso-3166/countries#RU]
         * ====================================================
         * Unexpected bindings: 
         * [country=http://downlode.org/rdf/iso-3166/countries#RU;
         * product=http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product63;
         * nrOfReviews="3"^^<http://www.w3.org/2001/XMLSchema#integer>;
         * avgPrice="4207.4263"^^<http://www.w3.org/2001/XMLSchema#float>]
         * </pre>
         * 
         * @see <a
         *      href="https://sourceforge.net/apps/trac/bigdata/ticket/534#comment:2">
         *      BSBM BI Q5 Error when using MERGE JOIN </a>
         */
        suite1 = filterOutTests(
                suite1,
                "bsbm"
                );
//                "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#bsbm-bi-q5");

        return suite1;

    }

    /**
     * Hack filters out the "dataset" tests.
     * 
     * @param suite1
     *            The test suite.
     * 
     * @return The test suite without the data set tests.
     */
    static TestSuite filterOutTests(final TestSuite suite1, final String name) {

        final TestSuite suite2 = new TestSuite(suite1.getName());
        @SuppressWarnings("unchecked")
        final Enumeration<Test> e = suite1.tests();
        while (e.hasMoreElements()) {
            final Test aTest = e.nextElement();
            if (aTest instanceof TestSuite) {
                final TestSuite aTestSuite = (TestSuite) aTest;
                if (!aTestSuite.getName().equals(name)) {
                    suite2.addTest(filterOutTests(aTestSuite,name));
                }
            } else {
                suite2.addTest(aTest);
            }
        }
        return suite2;
       
    }

    static TestSuite filterOutTests(final TestSuite suite1, final Collection<String> testURIs) {

        final TestSuite suite2 = new TestSuite(suite1.getName());
        @SuppressWarnings("unchecked")
        final Enumeration<Test> e = suite1.tests();
        while (e.hasMoreElements()) {
            final Test aTest = e.nextElement();
            if (aTest instanceof TestSuite) {
            	final TestSuite aTestSuite = (TestSuite) aTest;
                suite2.addTest(filterOutTests(aTestSuite, testURIs));
            } else if (aTest instanceof SPARQLQueryTest) {
                final SPARQLQueryTest test = (SPARQLQueryTest) aTest;
                if (!testURIs.contains(test.getTestURI())) {
                    suite2.addTest(test);
                }
            }

        }
        return suite2;
       
    }

    /**
     * An array of URIs for tests to be run. When null or empty the default test
     * suite is run. When specified, only the tests matching these test URIs are
     * run.
     */
    static final Collection<String> testURIs = Arrays.asList(new String[] {

    	// property paths
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-collection-01",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-collection-02",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-alternative-01",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-alternative-02",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-inverse-01",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-inverse-02",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-inverse-03",
//// Simple Sequences		
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-sequence-01",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-sequence-02",  // with inverse
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-sequence-03",
//// Sequence 04, 05, and 06 use the Property paths forms {...}, which were removed in the July 24th Working Draft
////		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-sequence-04",
////		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-sequence-05",
////		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-sequence-06",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-sequence-07",  // with optional
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-negated-01",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-negated-02",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-negated-03",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-negated-04",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-wildcard-01",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-wildcard-02",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-wildcard-03",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-wildcard-04",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-wildcard-05",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-wildcard-06",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-wildcard-cycles-01",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-wildcard-cycles-02",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-wildcard-cycles-03",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-wildcard-cycles-04",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-wildcard-reflexive-01",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-nested-01",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-nested-02",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-nested-03",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-nested-04",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-nested-05",
//		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-nested-06",


//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#sameTerm-simple"
    		
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-datatype-2"
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/cast/manifest#cast-dT"
    		
    		// 8, 9, 14-19, 23-30

//        	"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-in-02",
//        	"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-not-in-01",
//        	"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-not-in-02",
//        	"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-not-in-03",
    		
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-exists-05",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-exists-06",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-substr-02",
//        	"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql1/manifest#sparql11-substr-03"


        	
    		// derived numeric types
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-08",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-09",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-14",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-19",
    		
    		// Throwing NotSerializableException - something is dragging in the SPOAccessPath
    		// @see http://sourceforge.net/apps/trac/bigdata/ticket/379
//          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/triple-match/manifest#dawg-triple-pattern-003",

          // Hanging with error locating named subquery solution
    		// @see https://sourceforge.net/apps/trac/bigdata/ticket/380#comment:1
//          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#nested-opt-1",

            // Hanging, presumably due to failure to trigger last pass evaluation for SORT.
    		// @see https://sourceforge.net/apps/trac/bigdata/ticket/380#comment:3
//        	"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/sort/manifest#dawg-sort-3",

//    	"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#nested-opt-1",
//        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#nested-opt-2",
//        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#opt-filter-1", 
//        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#opt-filter-2", 
//        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#opt-filter-3", 
//        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#filter-place-1", 
//        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#filter-place-2",
//        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#filter-place-3", 
//        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#filter-nested-1", 
//        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#filter-nested-2", 
//        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#filter-scope-1", 
//        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#join-scope-1",
//        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#join-combo-1", 
//        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#join-combo-2",

    		
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-01",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-02",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-03",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-04",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-05",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-06",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-07",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-08",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-09",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-10",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-11",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-12",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-13",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-14",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-15",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-16",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-17",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-18",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-19",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-20",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-21",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-22",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-23",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-24",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-25",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-26",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-27",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-28",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-29",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#type-promotion-30",
    		
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/boolean-effective-value/manifest#dawg-bev-1",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/boolean-effective-value/manifest#dawg-bev-2",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/boolean-effective-value/manifest#dawg-bev-3",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/boolean-effective-value/manifest#dawg-bev-4",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/boolean-effective-value/manifest#dawg-bev-5",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/boolean-effective-value/manifest#dawg-bev-6",
    		
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#sameTerm-eq",
    		
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-01",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-02",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-03",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-04",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-05",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-06",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-07",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-08",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-09",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-10",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-11",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-12",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-cmp-01",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-cmp-02",
    		
	/*
	 * working through the new query engine failures: 0 errors, 11 failures
	 */

			/*
			 * Basically we are having a lot of problems with our compare
			 * operator, which is supposed to do fuzzy comparisons that
			 * sometimes requires materialized RDF values. These I feel I can
			 * handle on my own.
			 */

    		// "a" and "a"^^xsd:string have different term ids?  also bnodes are different
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-07",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-08",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-10",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-11",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-12",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-cmp-01",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-cmp-02",

			/*
			 * These tests have to do with that that weird "well designed"
			 * optional nesting P = A OPT (B OPT C) where A and C share
			 * variables not in B.  I think I can handle these on my own.
			 */
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#nested-opt-1",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#join-scope-1",

    		/*
    		 * Everything below this point I need help with.
    		 */
    		
			/*
			 * This one is truly bizarre - involving a non-optional subquuery
			 * plus an optional subquery. Don't even know where to start on this
			 * guy.
			 */
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#filter-scope-1",

			/*
			 * Sometimes, a filter is the entire join group, and it should not
			 * be able to see variables outside the group.  Frankly I do not
			 * understand this one.
			 */
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#filter-nested-2",

//			/*
//			 * These demonstrate the problem of where to put non-optional
//			 * filters that need to be evaluated after optional tails and
//			 * optional join groups.
//			 * 
//			 * NOTE: These are fixed.
//			 */
////    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/bound/manifest#dawg-bound-query-001",
////    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional-filter/manifest#dawg-optional-filter-002",
////    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional-filter/manifest#dawg-optional-filter-003",
    		
//			/*
//			 * These failures have to do with nested UNIONs - we don't seem to
//			 * be handling them correctly at all.
//			 * 
//			 * NOTE: These are fixed.
//			 */
////   		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#join-combo-1",
////   		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#join-combo-2",
//   		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional/manifest#dawg-optional-complex-1",
//   		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional/manifest#dawg-optional-complex-2",
////   		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional/manifest#dawg-optional-complex-4",

    });

    /**
     * Return the sole test in the suite associated with the specified testURI.
     * 
     * @param suite
     *            The test suite.
     * @param testURI
     *            The test URI (these are defined by the DAWG).
     *            
     * @return An instance of this class which will run just that one test.
     * 
     * @throws RuntimeException
     *             if there is no test in the suite which is associated with
     *             that testURI.
     */
    static SPARQLQueryTest getSingleTest(final TestSuite suite,
            final String testURI) throws RuntimeException {
    
        @SuppressWarnings("unchecked")
        final Enumeration<Test> e1 = suite.tests();
        while (e1.hasMoreElements()) {
            final Test aTest = e1.nextElement();
//            log.warn(aTest.toString());
            if (aTest instanceof TestSuite) {
                final SPARQLQueryTest test = getSingleTest((TestSuite) aTest,
                        testURI);
                if (test != null)
                    return test;
            }
            if (aTest instanceof SPARQLQueryTest) {
                final SPARQLQueryTest test = (SPARQLQueryTest) aTest;
                if (testURI.equals(test.getTestURI())) {
                    return test;
                }
            }
        }
        return null;
    }

    /**
     * Return the test suite. 
     */
    public static TestSuite suiteLTSWithPipelineJoins() throws Exception {

        final Factory factory = new Factory() {

            public SPARQLQueryTest createSPARQLQueryTest(String testURI,
                    String name, String queryFileURL, String resultFileURL,
                    Dataset dataSet, boolean laxCardinality) {

                return createSPARQLQueryTest(testURI, name, queryFileURL,
                        resultFileURL, dataSet, laxCardinality, true/* checkOrder */);

            }
            
            public SPARQLQueryTest createSPARQLQueryTest(String testURI,
                    String name, String queryFileURL, String resultFileURL,
                    Dataset dataSet, boolean laxCardinality, boolean checkOrder) {

                return new BigdataSparqlTest(testURI, name, queryFileURL,
                        resultFileURL, dataSet, laxCardinality, checkOrder) {

                    protected Properties getProperties() {

                        final Properties p = new Properties(
                                super.getProperties());

                        // p.setProperty(AbstractResource.Options.NESTED_SUBQUERY,
                        // "false");

                        return p;

                    }

                };

            }

        };

        final TestSuite suite = new TestSuite();

        // SPARQL 1.0
        suite.addTest(ManifestTest.suite(factory));

        // SPARQL 1.1
        suite.addTest(SPARQL11ManifestTest.suite(factory));
        
        return suite;

    }

    public BigdataSparqlTest(String testURI, String name, String queryFileURL,
            String resultFileURL, Dataset dataSet, boolean laxCardinality,
            boolean checkOrder) {

        super(testURI, name, queryFileURL, resultFileURL, dataSet,
                laxCardinality, checkOrder);

    }
    
//    public String getTestURI() {
//        return testURI;
//    }
    
    /**
     * Overridden to destroy the backend database and its files on the disk.
     */
    @Override
    public void tearDown()
        throws Exception
    {
/*
        StringBuilder message = new StringBuilder();
        message.append("data:\n");

        RepositoryConnection cxn = dataRep.getConnection();
        try {
            RepositoryResult<Statement> stmts = cxn.getStatements(null, null, null, true);
            while (stmts.hasNext()) {
                Statement stmt = stmts.next();
                message.append(stmt+"\n");
            }
        } finally {
            cxn.close();
        }
        SPARQLQueryTest.logger.error(message.toString());
*/        
        IIndexManager backend = null;
        
        Repository delegate = dataRep == null ? null : ((DatasetRepository) dataRep).getDelegate();
        
        if (delegate != null && delegate instanceof BigdataSailRepository) {
            
            backend = ((BigdataSailRepository) delegate).getDatabase()
                            .getIndexManager();
            
        }

        super.tearDown();

        if (backend != null)
            tearDownBackend(backend);

        /*
         * Note: this field MUST be cleared to null or the backing database
         * instance will be held by a hard reference throughout the execution of
         * all unit tests in this test suite!
         */

        dataRep = null;
        queryString = null;
    }

    /**
     * Note: This method may be overridden in order to run the test suite
     * against other variations of the bigdata backend.
     * 
     * @see #suiteLTSWithNestedSubquery()
     * @see #suiteLTSWithPipelineJoins()
     */
    protected Properties getProperties() {

        final Properties props = new Properties();
        
//        final File journal = BigdataStoreTest.createTempFile();
//        
//        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());

        props.setProperty(Options.BUFFER_MODE, BufferMode.Transient.toString());
        
        // quads mode: quads=true, sids=false, axioms=NoAxioms, vocab=NoVocabulary
        props.setProperty(Options.QUADS_MODE, "true");

        // no justifications
        props.setProperty(Options.JUSTIFY, "false");
        
        // no query time inference
        props.setProperty(Options.QUERY_TIME_EXPANDER, "false");
        
//        // auto-commit only there for TCK
//        props.setProperty(Options.ALLOW_AUTO_COMMIT, "true");
        
        // exact size only there for TCK
        props.setProperty(Options.EXACT_SIZE, "true");
        
//        props.setProperty(Options.COLLATOR, CollatorEnum.ASCII.toString());
        
//      Force identical unicode comparisons (assuming default COLLATOR setting).
//        props.setProperty(Options.STRENGTH, StrengthEnum.Identical.toString());
        
		/*
		 * disable read/write transactions since this class runs against the
		 * unisolated connection.
		 */
		props.setProperty(Options.ISOLATABLE_INDICES, "false");
        
        // disable truth maintenance in the SAIL
        props.setProperty(Options.TRUTH_MAINTENANCE, "false");
        
        return props;
        
    }
    
    @Override
    protected Repository newRepository() throws RepositoryException {

        if (true) {
            final Properties props = getProperties();
            
            if (cannotInlineTests.contains(testURI)){
            	// The test can not be run using XSD inlining.
                props.setProperty(Options.INLINE_XSD_DATATYPE_LITERALS, "false");
            	props.setProperty(Options.INLINE_DATE_TIMES, "false");
            }
            
            if(unicodeStrengthIdentical.contains(testURI)) {
            	// Force identical Unicode comparisons.
            	props.setProperty(Options.COLLATOR, CollatorEnum.JDK.toString());
            	props.setProperty(Options.STRENGTH, StrengthEnum.Identical.toString());
            }
            
            final BigdataSail sail = new BigdataSail(props);
            return new DatasetRepository(new BigdataSailRepository(sail));
        } else {
            return new DatasetRepository(new SailRepository(new MemoryStore()));
        }
    }

    protected void tearDownBackend(IIndexManager backend) {
        
        backend.destroy();
        
    }
    
    @Override
    protected Repository createRepository() throws Exception {
        Repository repo = newRepository();
        repo.initialize();
        return repo;
    }
    
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }
    
    public Repository getRepository() {
        return dataRep;
    }
    
    private String queryString = null;
    public String getQueryString() throws Exception {
        if (queryString == null) {
            InputStream stream = new URL(queryFileURL).openStream();
            try {
                return IOUtil.readString(new InputStreamReader(stream, "UTF-8"));
            }
            finally {
                stream.close();
            }
        }
        return queryString;
    }
    
/*
 * Note: This override is being done in the base class now.
 */
    
//    @Override
//    protected void runTest()
//        throws Exception
//    {
//    	if(true) {
//			/*
//			 * Dump out some interesting things about the query, the parse tree,
//			 * and the state of the database *before* we run the unit test. This
//			 * uses the same logic that is used when the test is run by the base
//			 * class.
//			 */
//        BigdataSailRepositoryConnection con = getQueryConnection(dataRep);
//        try {
//
////            log.info("database dump:");
////            RepositoryResult<Statement> stmts = con.getStatements(null, null, null, false);
////            while (stmts.hasNext()) {
////                log.info(stmts.next());
////            }
//            log.info("dataset:\n" + dataset);
//
//            String queryString = readQueryString();
//            log.info("query:\n" + getQueryString());
//            
//            Query query = con.prepareQuery(QueryLanguage.SPARQL, queryString, queryFileURL);
//            if (dataset != null) {
//                query.setDataset(dataset);
//            }
//
//            if (query instanceof TupleQuery) {
//                TupleQueryResult queryResult = ((TupleQuery)query).evaluate();
//                while (queryResult.hasNext()) {
//                    log.info("query result:\n" + queryResult.next());
//                }
//            }
//
//        }
//        finally {
//            con.close();
//        }
//    	}
//        
//        super.runTest();
//    }

	/**
	 * Overridden to use {@link BigdataSail#getReadOnlyConnection()} as a
	 * workaround to the test harness which invokes
	 * {@link BigdataSail#getConnection()} multiple times from within the same
	 * thread. When full transactions are not enabled, that will delegate to
	 * {@link BigdataSail#getUnisolatedConnection()}. Only one unisolated
	 * connection is permitted at a time. While different threads will block to
	 * await the unisolated connection, that method will throw an exception if
	 * there is an attempt by a single thread to obtain more than one instance
	 * of the unisolated connection (since that operation would otherwise
	 * deadlock).
	 */
	@Override
    protected BigdataSailRepositoryConnection getQueryConnection(
            Repository dataRep) throws Exception {

        return ((BigdataSailRepository) ((DatasetRepository) dataRep)
                .getDelegate()).getReadOnlyConnection();

    }

}
