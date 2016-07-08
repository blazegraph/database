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

import java.io.IOException;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepositoryConnection;

import junit.framework.Test;


/**
 * This test checks if literals are properly escaped while streaming potential solutions
 * into service calls (in RemoteSparqlXXQueryBuilder implementations).
 * @see https://jira.blazegraph.com/browse/BLZG-1951
 */
public class Test_Ticket_1951 extends AbstractProtocolTest {

	public Test_Ticket_1951(String name)  {
		super(name);
	}


	static public Test suite() {
		return ProxySuiteHelper.suiteWhenStandalone(Test_Ticket_1951.class,"test.*",
//				TestMode.quads
//				TestMode.sids,
				TestMode.triples
				);
	}
	
	/**
	 * Execute an ASK query including a SERVICE keyword which gets sent back to this server.
	 * The test checks if literal, which needs escaping, is properly streamed through a service call
	 * @param args
	 * @throws IOException
	 */
	public void test_simple() throws Exception {

		setMethodisPostUrlEncodedData();
		BigdataSailRemoteRepositoryConnection conn = m_repo.getBigdataSailRemoteRepository().getConnection();
		conn.prepareUpdate(QueryLanguage.SPARQL, "INSERT DATA { <a:a> <b:b> \"a\\nb\" }").execute();
		conn.commit();
		
		final String QUERY = "SELECT *\r\n" + 
			"WHERE {\r\n" + 
			"  ?s ?p ?o\r\n" + 
			"  SERVICE <> {\r\n" + 
			"    ?x ?y ?o\r\n" + 
			"  }\r\n" + 
			"}";

		final TupleQueryResult result = conn.prepareTupleQuery(QueryLanguage.SPARQL, QUERY).evaluate();
		
		try {
			while(result.hasNext()) {
				BindingSet bs = result.next();
				Value s = bs.getBinding("s").getValue();
				Value p = bs.getBinding("p").getValue();
				Value o = bs.getBinding("o").getValue();
				Value x = bs.getBinding("x").getValue();
				Value y = bs.getBinding("y").getValue();
				Value a = conn.getValueFactory().createURI("a:a");
				Value b = conn.getValueFactory().createURI("b:b");
				Value lit = conn.getValueFactory().createLiteral("a\nb");
				// expected:
				// ?s == ?x == <a>
				assertEquals(a, s);
				assertEquals(a, x);
				// ?p == ?y == <b>
				assertEquals(b, p);
				assertEquals(b, y);
				// ?o == "a\nb"
				assertEquals(lit, o);
			}
		} finally {
			result.close();
		}
		
	}


}
