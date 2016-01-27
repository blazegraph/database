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

import java.util.Arrays;

import junit.framework.Test;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;

/**
 * GETSTMTS test suite for includeInferred.
 * 
 * @see <a href="http://jira.blazegraph.com/browse/BLZG-1207" > getStatements()
 *      ignores includeInferred (REST API) </a>
 */
public class Test_Ticket_1207<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	private final Resource s = new URIImpl("http://test/s");
	private final URI p = new URIImpl("http://test/p");
	private final URI p1 = new URIImpl("http://test/p1");
	private final Value o = new URIImpl("http://test/o");

	public Test_Ticket_1207() {

	}

	public Test_Ticket_1207(final String name) {

		super(name);

	}

	public static Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(Test_Ticket_1207.class,
                "test.*", TestMode.triplesPlusTruthMaintenance
                );
       
	}

   /**
    * Test supposed to check if remote call for getStatements properly handle includeInferred flag
    */
   public void test_ticket_1207() throws Exception {

      final ValueFactoryImpl vf = ValueFactoryImpl.getInstance();
      final Statement[] a = new Statement[] { 
			vf.createStatement(s, p, o),
			vf.createStatement(p, RDFS.SUBPROPERTYOF, p1)
	  };

      final AddOp addOp = new AddOp(Arrays.asList(a));

      m_repo.add(addOp);

      final GraphQueryResult resultIncludeInferred = m_repo.getStatements(s, null, o, true);
      try {
         int count = 0;
         while (resultIncludeInferred.hasNext()) {
             resultIncludeInferred.next();
             count++;
         }
         assertEquals(2,count);
      } finally {
         resultIncludeInferred.close();
      }

      final GraphQueryResult resultDoNotIncludeInferred = m_repo.getStatements(s, null, o, false);
      try {
         int count = 0;
         while (resultDoNotIncludeInferred.hasNext()) {
        	 resultDoNotIncludeInferred.next();
             count++;
         }
         assertEquals(1,count);
      } finally {
    	  resultDoNotIncludeInferred.close();
      }

      {
          final TupleQuery tq = m_repo.getBigdataSailRemoteRepository().getConnection().prepareTupleQuery(QueryLanguage.SPARQL, "SELECT * {?s ?p ?o} LIMIT 100", null);
          tq.setBinding("s", s);
          tq.setIncludeInferred(true);
          final TupleQueryResult tqr = tq.evaluate();
          try {
              int count = 0;
              while (tqr.hasNext()) {
                  tqr.next();
                  count++;
               }
              assertEquals(2,count);
          } finally {
               tqr.close();
          }
      }
      {
          final TupleQuery tq = m_repo.getBigdataSailRemoteRepository().getConnection().prepareTupleQuery(QueryLanguage.SPARQL, "SELECT * {?s ?p ?o} LIMIT 100", null);
          tq.setBinding("s", s);
          tq.setIncludeInferred(false);
          final TupleQueryResult tqr = tq.evaluate();
          try {
              int count = 0;
              while (tqr.hasNext()) {
                  System.out.println(tqr.next());
                  count++;
               }
              assertEquals(1,count);
          } finally {
               tqr.close();
          }
      }
   }

}
