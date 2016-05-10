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

import junit.framework.Test;

import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.IIndexManager;

/**
 * Proxied test suite.
 *
 * @param <S>
 */
public class Test_REST_DELETE_WITH_BODY<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	public Test_REST_DELETE_WITH_BODY() {

	}

	public Test_REST_DELETE_WITH_BODY(final String name) {

		super(name);

	}

	public static Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(Test_REST_DELETE_WITH_BODY.class,
                "test.*", TestMode.quads
//                , TestMode.sids
//                , TestMode.triples
                );
       
	}

	public void test_DELETE_withPOST_RDFXML() throws Exception {
		doDeleteWithPostTest(RDFFormat.RDFXML);
	}

	public void test_DELETE_withPOST_NTRIPLES() throws Exception {
		doDeleteWithPostTest(RDFFormat.NTRIPLES);
	}

	public void test_DELETE_withPOST_N3() throws Exception {
		doDeleteWithPostTest(RDFFormat.N3);
	}

	public void test_DELETE_withPOST_TURTLE() throws Exception {
		doDeleteWithPostTest(RDFFormat.TURTLE);
	}

	public void test_DELETE_withPOST_TRIG() throws Exception {
		doDeleteWithPostTest(RDFFormat.TRIG);
	}

	public void test_DELETE_withPOST_TRIX() throws Exception {
		doDeleteWithPostTest(RDFFormat.TRIX);
	}

}
