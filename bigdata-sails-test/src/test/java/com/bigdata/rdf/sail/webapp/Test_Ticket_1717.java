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

import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.RemoveOp;

/**
 * GETSTMTS test suite for not resolved numeric IVs in Bind.
 * 
 * @see <a href="http://jira.blazegraph.com/browse/BLZG-1717" > IV not resolved </a>
 */
public class Test_Ticket_1717<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	public Test_Ticket_1717() {

	}

	public Test_Ticket_1717(final String name) {

		super(name);

	}

	public static Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(Test_Ticket_1717.class,
                "test.*", TestMode.triplesPlusTruthMaintenance
                );
       
	}

   /**
    * Test supposed to check if constants in BIND expressions will be resolved
    */
   public void test_ticket_1717() throws Exception {

      {
        // Clear DB per task description (Executing the query over the empty database)
        m_repo.remove(new RemoveOp(null, null, null));
        String query = "SELECT ?z ?s1 {  ?s ?p ?o .   BIND(?o+1 AS ?z)  BIND(?z+1 AS ?z2) }";
        final TupleQuery tq = m_repo.getBigdataSailRemoteRepository().getConnection().prepareTupleQuery(QueryLanguage.SPARQL, query, null);
          final TupleQueryResult tqr = tq.evaluate();
          try {
              int count = 0;
              while (tqr.hasNext()) {
                  System.out.println(tqr.next());
                  count++;
               }
              assertEquals(0,count); // asserting successful execution of the query, as it was failing while parsing
          } finally {
               tqr.close();
          }
      }
   }

}
