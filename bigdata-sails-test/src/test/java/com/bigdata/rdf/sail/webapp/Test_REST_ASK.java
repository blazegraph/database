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

import org.openrdf.query.resultio.BooleanQueryResultFormat;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.IPreparedBooleanQuery;

/**
 * Proxied test suite.
 *
 * @param <S>
 * 
 * TODO Should test GET as well as POST (this requires that we configured the
 * client differently).
 */
public class Test_REST_ASK<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	public Test_REST_ASK() {

	}

	public Test_REST_ASK(final String name) {

		super(name);

	}

	public static Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(Test_REST_ASK.class,
                "test.*", TestMode.quads
//                , TestMode.sids
//                , TestMode.triples
                );
       
	}

	/**
	 * "ASK" query with an empty KB and CONNEG for various known/accepted MIME
	 * Types.
	 */
	public void test_ASK() throws Exception {

		final String queryStr = "ASK where {?s ?p ?o}";

		// final RemoteRepository repo = new RemoteRepository(m_serviceURL);
		{
			final IPreparedBooleanQuery query = m_repo
					.prepareBooleanQuery(queryStr);
			assertEquals(false, query.evaluate());
		}
		{
			final IPreparedBooleanQuery query = m_repo
					.prepareBooleanQuery(queryStr);
			query.setHeader("Accept",
					BooleanQueryResultFormat.SPARQL.getDefaultMIMEType());
			assertEquals(false, query.evaluate());
		}
		{
			final IPreparedBooleanQuery query = m_repo
					.prepareBooleanQuery(queryStr);
			query.setHeader("Accept",
					BooleanQueryResultFormat.TEXT.getDefaultMIMEType());
			assertEquals(false, query.evaluate());
		}

		/**
       * Uncommented to test CONNEG for JSON (available with openrdf 2.7).
		 * 
		 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/588" >
		 *      JSON-LD </a>
		 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/714" >
		 *      Migrate to openrdf 2.7 </a>
		 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/704" >
		 *      ask does not return json </a>
		 */
		{
			final IPreparedBooleanQuery query = m_repo
					.prepareBooleanQuery(queryStr);
			query.setHeader("Accept", "application/sparql-results+json");
			assertEquals(false, query.evaluate());
		}

	}

	// /**
	// * "ASK" query using POST with an empty KB.
	// */
	// public void test_POST_ASK() throws Exception {
	//
	// final String queryStr = "ASK where {?s ?p ?o}";
	//
	// final QueryOptions opts = new QueryOptions();
	// opts.serviceURL = m_serviceURL;
	// opts.queryStr = queryStr;
	// opts.method = "POST";
	//
	// opts.acceptHeader = BooleanQueryResultFormat.SPARQL.getDefaultMIMEType();
	// assertEquals(false, askResults(doSparqlQuery(opts, requestPath)));
	//
	// opts.acceptHeader = BooleanQueryResultFormat.TEXT.getDefaultMIMEType();
	// assertEquals(false, askResults(doSparqlQuery(opts, requestPath)));
	//
	// }

}
