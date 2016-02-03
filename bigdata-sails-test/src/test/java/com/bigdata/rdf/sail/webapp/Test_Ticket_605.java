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

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResult;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;

/**
 * @see <a href="http://trac.bigdata.com/ticket/605" > Error in function
 *      :rangeCount </a>
 */
public class Test_Ticket_605<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	public Test_Ticket_605() {

	}

	public Test_Ticket_605(final String name) {

		super(name);

	}

	public static Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(Test_Ticket_605.class,
                "test.*", TestMode.quads
//                , TestMode.sids
//                , TestMode.triples
                );
       
	}

   /**
    * A Java version of the scala example.
    * 
    * @throws Exception
    * 
    * @see <a href="http://trac.bigdata.com/ticket/605" > Error in function
    *      :rangeCount </a>
    */
   public void test_ticket_605() throws Exception {

      final URI s = new URIImpl(":s");
      final URI p = new URIImpl(":p");
      final URI o = new URIImpl(":o");

      final Statement[] a = new Statement[] { ValueFactoryImpl.getInstance()
            .createStatement(s, p, o) };

      final AddOp addOp = new AddOp(Arrays.asList(a));

      m_repo.add(addOp);

      final TupleQueryResult result = m_repo.prepareTupleQuery(
            "SELECT * {?s ?p ?o} LIMIT 100").evaluate();
      try {
         while (result.hasNext()) {
            final BindingSet bset = result.next();
            if (log.isInfoEnabled()) {
               log.info(bset);
            }
            System.out.println(bset);
         }
      } finally {
         result.close();
      }

   }

}
