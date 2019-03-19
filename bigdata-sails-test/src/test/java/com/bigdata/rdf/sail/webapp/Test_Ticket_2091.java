/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2014.  All rights reserved.

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

import java.util.ArrayList;
import java.util.Collection;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import com.bigdata.bop.engine.NativeHeapStandloneChunkHandler;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepositoryConnection;
import com.bigdata.rdf.sparql.ast.QueryHints;

import junit.framework.Test;


/**
 * This test checks if constants in projection expressions are properly resolved.
 * @see https://jira.blazegraph.com/browse/BLZG-2091
 */
public class Test_Ticket_2091 extends AbstractProtocolTest {

	private String key;
	private String prevValue;

	public Test_Ticket_2091(String name)  {
		super(name);
		key = QueryHints.class.getName() + "." + QueryHints.QUERY_ENGINE_CHUNK_HANDLER;
		prevValue = System.getProperty(key);
	}

	@Override
	public void setUp() throws Exception {
		System.setProperty(key, NativeHeapStandloneChunkHandler.class.getName());
		super.setUp();
	}
	
	@Override
	public void tearDown() throws Exception {
		super.tearDown();
		if (prevValue != null) {
			System.setProperty(key, prevValue);
		} else {
			System.clearProperty(key);
		}
		
	}
	
	static public Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(Test_Ticket_2091.class,"test.*",
				TestMode.quads,
				TestMode.sids,
				TestMode.triples 
				);
	}
	
	private final static String QUERY = "PREFIX wd: <http://wd/>\n" + 
			"PREFIX wdt: <http://wdt/>\n" +
			"SELECT ?s ?host (IF(BOUND(?host), 0, 1) as ?p1)\r\n" + 
			"{\r\n" + 
			"	?s wdt:P31 wd:Q3917681\r\n" + 
			"	OPTIONAL {?s wdt:P17 ?host }\r\n" + 
			"} ";;

	/**
	 * Execute a query including constants in projection expression.
	 * The test succeeeds if the query successfully evaluates with 
	 * QUERY_ENGINE_CHUNK_HANDLER set to NativeHeapStandloneChunkHandler,
	 * as it requires results to be encoded in respect to namespace of value factory,
	 * which fails if constant has not been resolved against target DB value factory.
	 * @throws Exception
	 */
	public void test_simple() throws Exception {

		setMethodisPostUrlEncodedData();
		BigdataSailRemoteRepositoryConnection conn = m_repo.getBigdataSailRemoteRepository().getConnection();
		conn.prepareUpdate(QueryLanguage.SPARQL, "prefix wd: <http://wd/> \n" + 
				"prefix wdt: <http://wdt/> " +
				"INSERT DATA { wd:Q2 wdt:P31 wd:Q3917681 } ").execute();

		final TupleQueryResult result = 
				conn.prepareTupleQuery(QueryLanguage.SPARQL, QUERY).evaluate();
		
		Collection<Value> subjects = new ArrayList<>();
		try {
			while(result.hasNext()) {
				BindingSet bs = result.next();
				subjects.add(bs.getBinding("s").getValue());
			}
		} finally {
			result.close();
		}
		if(log.isInfoEnabled()) log.info(subjects);
		assertEquals(1, subjects.size());

	}


}
