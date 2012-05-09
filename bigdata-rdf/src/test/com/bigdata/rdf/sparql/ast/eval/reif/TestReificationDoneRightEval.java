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
 * Created on Sep 7, 2011
 */

package com.bigdata.rdf.sparql.ast.eval.reif;

import com.bigdata.bop.ap.Predicate;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;

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
 * @version $Id: TestTCK.java 6261 2012-04-09 10:28:48Z thompsonbry $
 */
public class TestReificationDoneRightEval extends AbstractDataDrivenSPARQLTestCase {

//    private static final Logger log = Logger.getLogger(TestReificationDoneRight.class);
    
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
     * Simple query involving alice, bob, and an information extractor.
     */
	public void test_reificationDoneRight_01() throws Exception {

		new TestHelper("reif/rdr-01", // testURI,
                "reif/rdr-01.rq",// queryFileURL
                "reif/rdr-01.ttl",// dataFileURL
                "reif/rdr-01.srx"// resultFileURL
                ).runTest();

	}

	/**
	 * Same data, but the query uses the BIND() syntax and pulls out some
	 * more information.
	 */
	public void test_reificationDoneRight_01a() throws Exception {

		new TestHelper("reif/rdr-01a", // testURI,
                "reif/rdr-01a.rq",// queryFileURL
                "reif/rdr-01.ttl",// dataFileURL
                "reif/rdr-01.srx"// resultFileURL
                ).runTest();

	}

	/**
	 * Simple query ("who bought sybase").
	 */
	public void test_reificationDoneRight_02() throws Exception {

		new TestHelper("reif/rdr-02", // testURI,
                "reif/rdr-02.rq",// queryFileURL
                "reif/rdr-02.ttl",// dataFileURL
                "reif/rdr-02.srx"// resultFileURL
                ).runTest();

	}

	/**
	 * Same data, but the query uses the BIND() syntax and pulls out some
	 * more information.
	 */
	public void test_reificationDoneRight_02a() throws Exception {

		new TestHelper("reif/rdr-02a", // testURI,
                "reif/rdr-02a.rq",// queryFileURL
                "reif/rdr-02a.ttl",// dataFileURL
                "reif/rdr-02a.srx"// resultFileURL
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

		new TestHelper("reif/rdr-03", // testURI,
                "reif/rdr-03.rq",// queryFileURL
                "reif/rdr-03.ttl",// dataFileURL
                "reif/rdr-03.srx"// resultFileURL
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

		new TestHelper("reif/rdr-03a", // testURI,
                "reif/rdr-03a.rq",// queryFileURL
                "reif/rdr-03a.ttl",// dataFileURL
                "reif/rdr-03a.srx"// resultFileURL
                ).runTest();

	}

}
