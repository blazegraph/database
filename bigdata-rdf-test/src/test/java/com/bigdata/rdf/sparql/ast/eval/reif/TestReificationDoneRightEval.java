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
 * Created on Sep 7, 2011
 */

package com.bigdata.rdf.sparql.ast.eval.reif;

import java.util.Properties;

import com.bigdata.BigdataStatics;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.impl.bnode.SidIV;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.decls.DCTermsVocabularyDecl;

/**
 * Reification Done Right test suite.
 * <p>
 * The basic extension is:
 * 
 * <pre>
 * BIND(<<?s,?p,?o>> as ?sid)
 * </pre>
 * 
 * This triple reference pattern associates the bindings on the subject,
 * predicate, and object positions of some triple pattern with the binding on a
 * variable for the triple. Except in the case where ?sid is already bound, the
 * triple pattern itself is processed normally and the bindings are concatenated
 * to form a representation of a triple, which is then bound on ?sid. When ?sid
 * is bound, it is decomposed into its subject, predicate, and object components
 * and those values are bound on the triple pattern.
 * <p>
 * When there are nested triple reference patterns, then they are just unwound
 * into simple triple reference patterns. A variables is created to provide the
 * association between each nested triple reference pattern and role played by
 * that triple reference pattern in the outer triple reference pattern.
 * <p>
 * We can handle this internally by allowing an optional named/positional role
 * for a {@link Predicate} which (when defined) becomes bound to the composition
 * of the (subject, predicate, and object) bindings for the predicate. It might
 * be easiest to handle this if we allowed the [c] position to be optional as
 * well and ignored it when in a triples only mode. The sid/triple binding would
 * always be "in scope" but it would only be interpreted when non-null (either a
 * constant or a variable).
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/526">
 *      Reification Done Right</a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestReificationDoneRightEval extends AbstractDataDrivenSPARQLTestCase {

//    private static final Logger log = Logger.getLogger(TestReificationDoneRight.class);
	
	public static final String TEST_RESOURCE_PREFIX = "/com/bigdata/rdf/sparql/ast/eval/reif/";
    
    /**
     * 
     */
    public TestReificationDoneRightEval() {
    }

    /**
     * @param name
     */
    public TestReificationDoneRightEval(String name) {
        super(name);
	}
    
	/**
	 * Bootstrap test. The data are explicitly entered into the KB by hand. This
	 * makes it possible to test evaluation without having to fix the RDF data
	 * loader. The query is based on <code>rdf-02</code>.
	 */
    public void test_reificationDoneRight_00() throws Exception {

		final BigdataValueFactory vf = store.getValueFactory();

		final BigdataURI SAP = vf.createURI("http://example.com/SAP");
		final BigdataURI bought = vf.createURI("http://example.com/bought");
		final BigdataURI sybase = vf.createURI("http://example.com/sybase");
		final BigdataURI dcSource = vf.createURI(DCTermsVocabularyDecl.NAMESPACE+"source");
		final BigdataURI dcCreated = vf.createURI(DCTermsVocabularyDecl.NAMESPACE+"created");
		final BigdataURI newsSybase = vf.createURI("http://example.com/news/us-sybase");
		final BigdataLiteral createdDate = vf.createLiteral("2011-04-05T12:00:00Z",XSD.DATETIME);
		final BigdataURI g1 = vf.createURI("http://example.com/g1");

		// Add/resolve the terms against the lexicon.
		final BigdataValue[] terms = new BigdataValue[] { SAP, bought, sybase,
				dcSource, dcCreated, newsSybase, createdDate, g1 };

		final BigdataURI context = store.isQuads() ? g1 : null;
		
		store.addTerms(terms);
		
		// ground statement.
		final BigdataStatement s0 = vf.createStatement(SAP, bought, sybase, 
				context, StatementEnum.Explicit);
		
		// Setup blank node with SidIV for that Statement.
		final BigdataBNode s1 = vf.createBNode("s1");
		s1.setStatementIdentifier(true);
		final ISPO spo = new SPO(s0);//SAP.getIV(), bought.getIV(), sybase.getIV(),
//				null/* NO CONTEXT */, StatementEnum.Explicit);
		s1.setIV(new SidIV<BigdataBNode>(spo));

		// metadata statements.

		final BigdataStatement mds1 = vf.createStatement(s1, dcSource,
				newsSybase, context, StatementEnum.Explicit);

		final BigdataStatement mds2 = vf.createStatement(s1, dcCreated,
				createdDate, context, StatementEnum.Explicit);

		final ISPO[] stmts = new ISPO[] { new SPO(s0), new SPO(mds1), new SPO(mds2) };

		store.addStatements(stmts, stmts.length);

		/*
		 * Now that we have populated the database, we can go ahead and compile
		 * the query. (If we compile the query first then it will not find any
		 * matching lexical items.)
		 */

		final TestHelper h = new TestHelper(TEST_RESOURCE_PREFIX + "rdr-00", // testURI,
				TEST_RESOURCE_PREFIX + "rdr-02.rq",// queryFileURL
				TEST_RESOURCE_PREFIX + "empty.ttl",// dataFileURL
				TEST_RESOURCE_PREFIX + "rdr-02.srx"// resultFileURL
		);

		h.runTest();

    }

    /**
     * Version of the above where the data are read from a file rather than being
     * built up by hand.
     */
    public void test_reificationDoneRight_00_loadDataFromFile() throws Exception {

    	new TestHelper(TEST_RESOURCE_PREFIX + "rdr-00-loadFromFile", // testURI,
				TEST_RESOURCE_PREFIX + "rdr-02.rq",// queryFileURL
				TEST_RESOURCE_PREFIX + "rdr-02.ttlx",// dataFileURL
				TEST_RESOURCE_PREFIX + "rdr-02.srx"// resultFileURL
		).runTest();

    }
    
	/**
	 * Bootstrap test. The data are explicitly entered into the KB by hand. This
	 * makes it possible to test evaluation without having to fix the RDF data
	 * loader. The query is based on <code>rdf-02a</code>.
	 */
    public void test_reificationDoneRight_00a() throws Exception {

		final BigdataValueFactory vf = store.getValueFactory();

		final BigdataURI SAP = vf.createURI("http://example.com/SAP");
		final BigdataURI bought = vf.createURI("http://example.com/bought");
		final BigdataURI sybase = vf.createURI("http://example.com/sybase");
		final BigdataURI dcSource = vf.createURI(DCTermsVocabularyDecl.NAMESPACE+"source");
		final BigdataURI dcCreated = vf.createURI(DCTermsVocabularyDecl.NAMESPACE+"created");
		final BigdataURI newsSybase = vf.createURI("http://example.com/news/us-sybase");
		final BigdataLiteral createdDate = vf.createLiteral("2011-04-05T12:00:00Z",XSD.DATETIME);
		final BigdataURI g1 = vf.createURI("http://example.com/g1");

		// Add/resolve the terms against the lexicon.
		final BigdataValue[] terms = new BigdataValue[] { SAP, bought, sybase,
				dcSource, dcCreated, newsSybase, createdDate, g1 };

		final BigdataURI context = store.isQuads() ? g1 : null;
		
		store.addTerms(terms);
		
		// ground statement.
		final BigdataStatement s0 = vf.createStatement(SAP, bought, sybase,
				context, StatementEnum.Explicit);
		
		// Setup blank node with SidIV for that Statement.
		final BigdataBNode s1 = vf.createBNode("s1");
		s1.setStatementIdentifier(true);
		final ISPO spo = new SPO(SAP.getIV(), bought.getIV(), sybase.getIV(),
				null/* NO CONTEXT */, StatementEnum.Explicit);
		s1.setIV(new SidIV<BigdataBNode>(spo));

		// metadata statements.

		final BigdataStatement mds1 = vf.createStatement(s1, dcSource,
				newsSybase, context, StatementEnum.Explicit);

		final BigdataStatement mds2 = vf.createStatement(s1, dcCreated,
				createdDate, context, StatementEnum.Explicit);

		final ISPO[] stmts = new ISPO[] { new SPO(s0), new SPO(mds1), new SPO(mds2) };

		store.addStatements(stmts, stmts.length);

		/*
		 * Now that we have populated the database, we can go ahead and compile
		 * the query. (If we compile the query first then it will not find any
		 * matching lexical items.)
		 */

		new TestHelper(TEST_RESOURCE_PREFIX + "rdr-00a", // testURI,
				TEST_RESOURCE_PREFIX + "rdr-02a.rq",// queryFileURL
				TEST_RESOURCE_PREFIX + "empty.ttl",// dataFileURL
				TEST_RESOURCE_PREFIX + "rdr-02a.srx"// resultFileURL
		).runTest();

    }
    
    /**
     * Version of the above where the data are read from a file rather than being
     * built up by hand.
     */
    public void test_reificationDoneRight_00a_loadFromFile() throws Exception {

		new TestHelper(TEST_RESOURCE_PREFIX + "rdr-00a-loadFromFile", // testURI,
				TEST_RESOURCE_PREFIX + "rdr-02a.rq",// queryFileURL
				TEST_RESOURCE_PREFIX + "rdr-02.ttlx",// dataFileURL
				TEST_RESOURCE_PREFIX + "rdr-02a.srx"// resultFileURL
		).runTest();

    }
    
    /**
	 * Simple query involving alice, bob, and an information extractor. For this
	 * version of the test the data are modeled in the source file using RDF
	 * reification.
	 * 
	 * <pre>
	 * select ?src where {
	 *   ?x foaf:name "Alice" .
	 *   ?y foaf:name "Bob" .
	 *   <<?x foaf:knows ?y>> dc:source ?src .
	 * }
	 * </pre>
	 */
	public void test_reificationDoneRight_01() throws Exception {

		new TestHelper(TEST_RESOURCE_PREFIX + "rdr-01", // testURI,
                TEST_RESOURCE_PREFIX + "rdr-01.rq",// queryFileURL
                TEST_RESOURCE_PREFIX + "rdr-01.ttl",// dataFileURL
                TEST_RESOURCE_PREFIX + "rdr-01.srx"// resultFileURL
                ).runTest();

	}

    /**
	 * Simple query involving alice, bob, and an information extractor. For this
	 * version of the test the data are modeled in the source file using the RDR
	 * syntax.
	 * 
	 * <pre>
	 * select ?src where {
	 *   ?x foaf:name "Alice" .
	 *   ?y foaf:name "Bob" .
	 *   <<?x foaf:knows ?y>> dc:source ?src .
	 * }
	 * </pre>
	 */
	public void test_reificationDoneRight_01_usingRDRData() throws Exception {

		new TestHelper(TEST_RESOURCE_PREFIX + "rdr-01-usingRDRData", // testURI,
                TEST_RESOURCE_PREFIX + "rdr-01.rq",// queryFileURL
                TEST_RESOURCE_PREFIX + "rdr-01.ttlx",// dataFileURL
                TEST_RESOURCE_PREFIX + "rdr-01.srx"// resultFileURL
                ).runTest();

	}

	/**
	 * Same data, but the query uses the BIND() syntax and pulls out some more
	 * information.
	 * 
	 * <pre>
	 * select ?who ?src ?conf where {
	 *   ?x foaf:name "Alice" .
	 *   ?y foaf:name ?who .
	 *   BIND( <<?x foaf:knows ?y>> as ?sid ) .
	 *   ?sid dc:source ?src .
	 *   ?sid rv:confidence ?src .
	 * }
	 * </pre>
	 */
	public void test_reificationDoneRight_01a() throws Exception {

		new TestHelper(TEST_RESOURCE_PREFIX + "rdr-01a", // testURI,
                TEST_RESOURCE_PREFIX + "rdr-01a.rq",// queryFileURL
                TEST_RESOURCE_PREFIX + "rdr-01.ttl",// dataFileURL
                TEST_RESOURCE_PREFIX + "rdr-01a.srx"// resultFileURL
                ).runTest();

	}

	/**
	 * Same data, but the query uses the BIND() syntax and pulls out some more
	 * information and RDR syntax for the data.
	 * 
	 * <pre>
	 * select ?who ?src ?conf where {
	 *   ?x foaf:name "Alice" .
	 *   ?y foaf:name ?who .
	 *   BIND( <<?x foaf:knows ?y>> as ?sid ) .
	 *   ?sid dc:source ?src .
	 *   ?sid rv:confidence ?src .
	 * }
	 * </pre>
	 */
	public void test_reificationDoneRight_01a_usingRDRData() throws Exception {

		new TestHelper(TEST_RESOURCE_PREFIX + "rdr-01a-usingRDRData", // testURI,
                TEST_RESOURCE_PREFIX + "rdr-01a.rq",// queryFileURL
                TEST_RESOURCE_PREFIX + "rdr-01.ttlx",// dataFileURL
                TEST_RESOURCE_PREFIX + "rdr-01a.srx"// resultFileURL
                ).runTest();

	}

	/**
	 * Simple query ("who bought sybase").
	 * 
	 * <pre>
	 * SELECT ?src ?who {
	 *    <<?who :bought :sybase>> dc:source ?src
	 * }
	 * </pre>
	 */
	public void test_reificationDoneRight_02() throws Exception {

		new TestHelper(TEST_RESOURCE_PREFIX + "rdr-02", // testURI,
                TEST_RESOURCE_PREFIX + "rdr-02.rq",// queryFileURL
                TEST_RESOURCE_PREFIX + "rdr-02.ttl",// dataFileURL
                TEST_RESOURCE_PREFIX + "rdr-02.srx"// resultFileURL
                ).runTest();

	}

	/**
	 * Simple query ("who bought sybase") using RDR syntax for the data.
	 * 
	 * <pre>
	 * SELECT ?src ?who {
	 *    <<?who :bought :sybase>> dc:source ?src
	 * }
	 * </pre>
	 */
	public void test_reificationDoneRight_02_usingRDRData() throws Exception {

		new TestHelper(TEST_RESOURCE_PREFIX + "rdr-02", // testURI,
                TEST_RESOURCE_PREFIX + "rdr-02.rq",// queryFileURL
                TEST_RESOURCE_PREFIX + "rdr-02.ttlx",// dataFileURL
                TEST_RESOURCE_PREFIX + "rdr-02.srx"// resultFileURL
                ).runTest();

	}

	/**
	 * Same data, but the query uses the BIND() syntax and pulls out some more
	 * information.
	 * 
	 * <pre>
	 * SELECT ?src ?who ?created {
	 *    BIND( <<?who :bought :sybase>> as ?sid ) .
	 *    ?sid dc:source ?src .
	 *    OPTIONAL {?sid dc:created ?created}
	 * }
	 * </pre>
	 */
	public void test_reificationDoneRight_02a() throws Exception {

		new TestHelper(TEST_RESOURCE_PREFIX + "rdr-02a", // testURI,
                TEST_RESOURCE_PREFIX + "rdr-02a.rq",// queryFileURL
                TEST_RESOURCE_PREFIX + "rdr-02a.ttl",// dataFileURL
                TEST_RESOURCE_PREFIX + "rdr-02a.srx"// resultFileURL
                ).runTest();

	}
	
	/**
	 * Same data, but the query uses the BIND() syntax and pulls out some more
	 * information and RDR syntax for the data.
	 * 
	 * <pre>
	 * SELECT ?src ?who ?created {
	 *    BIND( <<?who :bought :sybase>> as ?sid ) .
	 *    ?sid dc:source ?src .
	 *    OPTIONAL {?sid dc:created ?created}
	 * }
	 * </pre>
	 */
	public void test_reificationDoneRight_02a_usingRDRData() throws Exception {

		new TestHelper(TEST_RESOURCE_PREFIX + "rdr-02a", // testURI,
                TEST_RESOURCE_PREFIX + "rdr-02a.rq",// queryFileURL
                TEST_RESOURCE_PREFIX + "rdr-02a.ttlx",// dataFileURL
                TEST_RESOURCE_PREFIX + "rdr-02a.srx"// resultFileURL
                ).runTest();

	}
	
	/**
	 * <pre>
	 * prefix : <http://example.com/>
	 * SELECT ?a {
	 *    BIND( <<?a :b :c>> AS ?ignored )
	 * }
	 * </pre>
	 * 
	 * <pre>
	 * prefix : <http://example.com/> .
	 * :a1  :b  :c .
	 * :a2  :b  :c .
	 * <<:a2 :b :c>>  :d  :e .
	 * <<:a3 :b :c>>  :d  :e .
	 * </pre>
	 * 
	 * <pre>
	 * ?a
	 * ---
	 * :a1
	 * :a2
	 * </pre>
	 * 
	 * @throws Exception
	 */
	public void test_reificationDoneRight_03() throws Exception {

		new TestHelper(TEST_RESOURCE_PREFIX + "rdr-03", // testURI,
                TEST_RESOURCE_PREFIX + "rdr-03.rq",// queryFileURL
                TEST_RESOURCE_PREFIX + "rdr-03.ttl",// dataFileURL
                TEST_RESOURCE_PREFIX + "rdr-03.srx"// resultFileURL
                ).runTest();

	}

	/**
	 * <pre>
	 * prefix : <http://example.com/>
	 * SELECT ?a ?e {
	 *    BIND( <<?a :b :c>> AS ?sid ) .
	 *    ?sid :d ?e .
	 * }
	 * </pre>
	 * 
	 * <pre>
	 * prefix : <http://example.com/> .
	 * :a1  :b  :c .
	 * :a2  :b  :c .
	 * <<:a2 :b :c>>  :d  :e .
	 * <<:a3 :b :c>>  :d  :e .
	 * </pre>
	 * 
	 * <pre>
	 * ?a  ?e
	 * ------
	 * :a2 :e
	 * </pre>
	 * 
	 * @throws Exception
	 */
	public void test_reificationDoneRight_03a() throws Exception {

		new TestHelper(TEST_RESOURCE_PREFIX + "rdr-03a", // testURI,
                TEST_RESOURCE_PREFIX + "rdr-03a.rq",// queryFileURL
                TEST_RESOURCE_PREFIX + "rdr-03a.ttl",// dataFileURL
                TEST_RESOURCE_PREFIX + "rdr-03a.srx"// resultFileURL
                ).runTest();

	}

    /**
     * <pre>
     * </pre>
     * @see <a href="http://trac.blazegraph.com/ticket/815"> RDR query does too
     *      much work</a>
     */
    public void test_reificationDoneRight_04() throws Exception {

        new TestHelper(TEST_RESOURCE_PREFIX + "rdr-04", // testURI,
                TEST_RESOURCE_PREFIX + "rdr-04.rq",// queryFileURL
                TEST_RESOURCE_PREFIX + "rdr-04.ttlx",// dataFileURL
                TEST_RESOURCE_PREFIX + "rdr-04.srx"// resultFileURL
                ).runTest();

    }
    
    // TODO: this test case is actually failing, not sure whether it should
    // succeed or not
    /**
     * Test loading of RDR triples from file ttl file containing standard
     * reification that contains three unordered reified triples, with query
     * specified in Standard RDF Reification.
     * 
     * @throws Exception
     */
    public void test_reificationDoneRight_05a() throws Exception {

        if (!BigdataStatics.runKnownBadTests) // FIXME RDR TEST KNOWN TO FAIL.
            return;

        new TestHelper(TEST_RESOURCE_PREFIX + "rdr-05a", // testURI,
               TEST_RESOURCE_PREFIX + "rdr-05a.rq",// queryFileURL
               TEST_RESOURCE_PREFIX + "rdr-05.ttl",// dataFileURL
               TEST_RESOURCE_PREFIX + "rdr-05.srx"// resultFileURL
               ).runTest();

   }
    
    /**
     * Test loading of RDR triples from file ttl file containing standard
     * reification that contains three unordered reified triples, with query
     * specified in RDR.
     * 
     * @throws Exception
     */
    public void test_reificationDoneRight_05b() throws Exception {

       new TestHelper(TEST_RESOURCE_PREFIX + "rdr-05b", // testURI,
               TEST_RESOURCE_PREFIX + "rdr-05b.rq",// queryFileURL
               TEST_RESOURCE_PREFIX + "rdr-05.ttl",// dataFileURL
               TEST_RESOURCE_PREFIX + "rdr-05.srx"// resultFileURL
               ).runTest();

   }

    @Override
    public Properties getProperties() {

        // Note: clone to avoid modifying!!!
        final Properties properties = (Properties) super.getProperties().clone();

        // turn off quads.
        properties.setProperty(AbstractTripleStore.Options.QUADS, "false");
        
        properties.setProperty(AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "true");

        // TM not available with quads.
        properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE,"false");

//        // override the default vocabulary.
//        properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
//                NoVocabulary.class.getName());

        // turn off axioms.
        properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
                NoAxioms.class.getName());

        // no persistence.
        properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE,
                BufferMode.Transient.toString());
        
//        properties.setProperty(AbstractTripleStore.Options.STORE_BLANK_NODES, "true");

        return properties;

    }

}
