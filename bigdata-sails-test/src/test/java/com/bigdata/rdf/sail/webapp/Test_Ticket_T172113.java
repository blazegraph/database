/*
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

import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.RemoveOp;

/**
 * ConcurrentModificationException on non-grouping query with aggregates in SELECT.
 * @See https://phabricator.wikimedia.org/T172113
 */
public class Test_Ticket_T172113<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	public Test_Ticket_T172113() {

	}

	public Test_Ticket_T172113(final String name) {

		super(name);

	}

	public static Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(Test_Ticket_T172113.class,
                "test.*", TestMode.triples
                );
       
	}

   /**
    * Test supposed to check if constants .. will be resolved
    */
   public void test_aggregatesWithoutGroupBy_T172113() throws Exception {
        // Clear DB per task description (Executing the query over the empty database)
        m_repo.remove(new RemoveOp(null, null, null));
        String query = "SELECT (COUNT(*) AS ?a) (COUNT(?x) AS ?b) (?b/?a AS ?r) { BIND(1 AS ?x)}";
        final TupleQuery tq = m_repo.getBigdataSailRemoteRepository().getConnection().prepareTupleQuery(QueryLanguage.SPARQL, query, null);
        final TupleQueryResult tqr = tq.evaluate();
        try {
            int count = 0;
            while (tqr.hasNext()) {
                System.out.println(tqr.next());
                count++;
             }
            assertEquals(1,count); // asserting successful execution of the query, as it was failing while parsing
        } finally {
            tqr.close();
        }
   }

}
