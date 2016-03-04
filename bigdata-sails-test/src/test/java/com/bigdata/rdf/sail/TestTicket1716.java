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
 * Test suite for an issue where IV resolution of invalid numeric literal fails
 * 
 * @see <a href="https://jira.blazegraph.com/browse/BLZG-1716">
 * SPARQL Update parser fails on invalid numeric literals
 */
public class TestTicket1716 extends QuadsTestCase {
	
    public TestTicket1716() {
	}

	public TestTicket1716(String arg0) {
		super(arg0);
	}

	public void testBug() throws Exception {

		final BigdataSail sail = getSail();
		try {
			executeQuery(new BigdataSailRepository(sail));
		} finally {
			sail.__tearDownUnitTest();
		}
	}

	private void executeQuery(final BigdataSailRepository repo)
			throws UpdateExecutionException, RepositoryException, MalformedQueryException {
		try {
			repo.initialize();
			final BigdataSailRepositoryConnection conn = repo.getConnection();
			try {
				String update = "insert {" + 
						"<http://dbpedia.org/resource/Jules_Verne> <http://dbpedia.org/property/period> \"\"^^<http://www.w3.org/2001/XMLSchema#int>\r\n" + 
						"} where {}"; 
				Update preparedUpdate = conn.prepareUpdate(QueryLanguage.SPARQL, update);
				preparedUpdate.execute();
				// no exception should occur on execution, overwise test will fail
			} finally {
				conn.close();
			}
		} finally {
			repo.shutDown();
		}
	}

}
