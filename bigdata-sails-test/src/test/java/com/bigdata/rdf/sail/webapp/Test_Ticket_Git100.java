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
import org.openrdf.query.Update;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.RemoveOp;

/**
 * Test suite for not resolved numeric IVs in DELETE+INSERT.
 * 
 * @see <a href="https://github.com/blazegraph/database/issues/100" > SPARQL Update produces "BigdataValue not available" exception on integers </a>
 */
public class Test_Ticket_Git100<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	public Test_Ticket_Git100() {

	}

	public Test_Ticket_Git100(final String name) {

		super(name);

	}

	public static Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(Test_Ticket_Git100.class,
                "test.*", TestMode.triplesPlusTruthMaintenance
                );
       
	}

   /**
    * Test supposed to check if constants in INSERT clauses will be resolved
    */
   public void test_ticket_GIT() throws Exception {

      {
        m_repo.remove(new RemoveOp(null, null, null));
        String query0 = 
        		"INSERT { <:foo1> <:bar> 0 . } WHERE {};" + 
        		"INSERT { <:foo2> <:bar> 0 . } WHERE {};";
        final Update tq = m_repo.getBigdataSailRemoteRepository().getConnection().prepareUpdate(QueryLanguage.SPARQL, query0, null);
        tq.execute();
      }
   }

}
