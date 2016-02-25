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

import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * Test suite for an issue where IV resolution of having clause fails
 * 
 * @see <a href="https://jira.blazegraph.com/browse/BLZG-1753">
 * Insert problem using subqueries and having clause
 */
public class TestTicket1753 extends QuadsTestCase {
	
    public TestTicket1753() {
	}

	public TestTicket1753(String arg0) {
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
				String update = "insert\r\n" + 
						"{ <http://dbpedia.org/resource/Jules_Verne> <http://ll.com.br/related> ?ss}\r\n" + 
						"where { {select distinct ?ss where { \r\n" + 
						"	?b <http://ll.com.br/author> ?ss . ?ss <http://purl.org/dc/terms/subject> ?x . \r\n" + 
						"	filter(?ss != <http://dbpedia.org/resource/Jules_Verne>)\r\n" + 
						"	{select (count(distinct ?s) as ?c) ?x where\r\n" + 
						"	{ <http://dbpedia.org/resource/Jules_Verne> <http://purl.org/dc/terms/subject> ?x . \r\n" +
						"	?s <http://purl.org/dc/terms/subject> ?x . \r\n" + 
						"	?b <http://ll.com.br/author> ?s . \r\n" + 
						"	filter( !contains(str(?x),\"University\") \r\n" + 
						"		&& !contains(str(?x),\"People\") \r\n" + 
						"		&& !contains(str(?x),\"Convert\") \r\n" + 
						"		&& !contains(str(?x),\"people\") )} \r\n" + 
						"	group by ?x having(?c < 500) } \r\n" + 
						"}order by asc(?c) limit 20} }"; 
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
