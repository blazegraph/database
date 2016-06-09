/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2011.  All rights reserved.

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

package com.bigdata.rdf.sail;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;

/**
 * Test suite for an issue where IV resolution of RDR statements was not completed
 * 
 * @see <a href="https://jira.blazegraph.com/browse/BLZG-1875">
 * Insert problem using subqueries and having clause
 */
public class TestTicket1875 extends QuadsTestCase {
	
    public TestTicket1875() {
	}

	public TestTicket1875(String arg0) {
		super(arg0);
	}

	/**
	 * Test succeeds if all updates executed without any exceptions
	 * @throws Exception
	 */
	public void testBug() throws Exception {

		final BigdataSail sail = getSail();
		try {
			final BigdataSailRepository repo = new BigdataSailRepository(sail);
			
			try {
				
				repo.initialize();
				String update1 =
						"insert data {  \r\n" + 
						"  <x:s>	<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.ft.com/ontology/thing/Thing> .\r\n" + 
						"  <<<x:s>	<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.ft.com/ontology/thing/Thing>  >> <http://www.ft.com/ontology/event/prov> <http://x>  \r\n" + 
						"}";
				executeUpdate(repo, update1);
	
				String update2 =
						"prefix rdf:          <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \r\n" + 
						"insert data {  \r\n" + 
						"  <x:s>	rdf:type <http://x/o> .\r\n" + 
						"  <<<x:s>	rdf:type <http://x/o>  >> <http://x/pr> <http://x/or>  \r\n" + 
						"}";
				executeUpdate(new BigdataSailRepository(sail), update2);
	
				String update3 =
						"prefix bbb: <http://x/> \r\n" + 
						"insert data {   \r\n" + 
						"  <x:a> rdf:type bbb:B .\r\n" + 
						"  << <x:a> rdf:type bbb:B >><x:pr> <x:pr> .\r\n" + 
						"}";
				executeUpdate(new BigdataSailRepository(sail), update3);

			} finally {
				repo.shutDown();
			}
			
		} finally {
			sail.__tearDownUnitTest();
		}
	}

	private void executeUpdate(final BigdataSailRepository repo, final String update)
			throws UpdateExecutionException, RepositoryException, MalformedQueryException {
		final BigdataSailRepositoryConnection conn = repo.getConnection();
		try {
			Update preparedUpdate = conn.prepareUpdate(QueryLanguage.SPARQL, update);
			preparedUpdate.execute();
			// no exception should occur on execution, overwise test will fail
		} finally {
			conn.close();
		}
	}

}
