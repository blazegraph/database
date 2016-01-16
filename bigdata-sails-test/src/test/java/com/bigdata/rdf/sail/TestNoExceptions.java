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
/*
 * Created on Oct 1, 2013
 */

package com.bigdata.rdf.sail;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.sparql.ast.ParseException;
import com.bigdata.rdf.sail.sparql.ast.TokenMgrError;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * This test suite is for trac items where the failure mode is a 500 error caused
 * by a software error, often in the static optimizer.
 * 
 * The tests each consist of a test query in a file in this package.
 * The typical test succeeds if the optimizers run on this query without a disaster.
 * This test suite does NOT have either of the following objectives:
 * - that the static optimizer is correct in the sense that the optimized query has the same meaning as the original query
 * or
 * - an optimizer in the sense that the optimized query is likely to be faster than the original query.
 * 
 * The very limited goal is that no uncaught exceptions are thrown!
 * 
 */
public class TestNoExceptions extends
        QuadsTestCase {

    /**
     * 
     */
    public TestNoExceptions() {
    }

    /**
     * @param name
     */
    public TestNoExceptions(String name) {
        super(name);
    }

    public AbstractBigdataSailTestCase getOurDelegate() {

        if (getDelegate() == null) {

            String testClass = System.getProperty("testClass");
            if (testClass != null) {
            	return super.getOurDelegate();

            }
            setDelegate(new com.bigdata.rdf.sail.TestBigdataSailWithQuads());
        }
        return (AbstractBigdataSailTestCase) super.getDelegate();
    }

	/**
	 * Please set your database properties here, except for your journal file,
	 * please DO NOT SPECIFY A JOURNAL FILE.
	 */
	@Override
	public Properties getProperties() {

		final Properties props = super.getProperties();

		/*
		 * For example, here is a set of five properties that turns off
		 * inference, truth maintenance, and the free text index.
		 */
		props.setProperty(BigdataSail.Options.AXIOMS_CLASS,
				NoAxioms.class.getName());
		props.setProperty(BigdataSail.Options.VOCABULARY_CLASS,
				NoVocabulary.class.getName());
		props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
		props.setProperty(BigdataSail.Options.JUSTIFY, "false");
//		props.setProperty(BigdataSail.Options.INLINE_DATE_TIMES, "true");
//		props.setProperty(BigdataSail.Options.ISOLATABLE_INDICES, "true");
//		props.setProperty(BigdataSail.Options.EXACT_SIZE, "true");
//		props.setProperty(BigdataSail.Options.ALLOW_SESAME_QUERY_EVALUATION,
//				"false");
		props.setProperty(BigdataSail.Options.STATEMENT_IDENTIFIERS, "false");

		return props;

	}
    /**
     * Unit test for WITH {subquery} AS "name" and INCLUDE. The WITH must be in
     * the top-level query. 
     * 
     * This is specifically for Trac 746 which crashed out during optimize.
     * So the test simply runs that far, and does not verify anything
     * other than the ability to optimize without an exception
     * @throws IOException 
     */
    public void test_namedSubquery746() throws Exception,
            TokenMgrError, ParseException, IOException {
        optimizeQuery("ticket746");

    }
    
/**
 * <pre>
SELECT *
{  { SELECT * { ?s ?p ?o } LIMIT 1 }
   FILTER ( ?s = &lt;eg:a&gt; )
}
 </pre>
 * @throws MalformedQueryException
 * @throws TokenMgrError
 * @throws ParseException
 * @throws IOException
 */
    public void test_filterSubselect737() throws Exception,
            TokenMgrError, ParseException, IOException {
        optimizeQuery("filterSubselect737");

    }
    

/**
 * <pre>
SELECT *
WHERE {

   { FILTER ( false ) }
    UNION
    {
    {  SELECT ?Subject_A 
      WHERE {
        { SELECT $j__5 ?Subject_A
          {
          } ORDER BY $j__5
        }
      } GROUP BY ?Subject_A
    }
   }
  OPTIONAL {
    {  SELECT ?Subject_A 
      WHERE {
        { SELECT $j__8 ?Subject_A
          {
         
          }  ORDER BY $j_8
        }
      } GROUP BY ?Subject_A
    }
  }
}
 </pre>
 * @throws MalformedQueryException
 * @throws TokenMgrError
 * @throws ParseException
 * @throws IOException
 */
    public void test_nestedSubselectsWithUnion737() throws Exception,
            TokenMgrError, ParseException, IOException {
        optimizeQuery("nestedSubselectsWithUnion737");

    }

	void optimizeQuery(final String queryfile) throws Exception {
		final String sparql = IOUtils.toString(getClass().getResourceAsStream(queryfile+".rq"));
		// try with Bigdata:
		final BigdataSail sail = getSail();
		try {
			executeQuery(new BigdataSailRepository(sail),sparql);
		} finally {
			sail.__tearDownUnitTest();
		}

	}

	private void executeQuery(final SailRepository repo, final String query)
			throws RepositoryException, MalformedQueryException,
			QueryEvaluationException, RDFParseException, IOException,
			RDFHandlerException {
		try {
			repo.initialize();
			final RepositoryConnection conn = repo.getConnection();
			conn.setAutoCommit(false);
			try {
				final ValueFactory vf = conn.getValueFactory();
				conn.commit();
				TupleQuery tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
				TupleQueryResult tqr = tq.evaluate();
				tqr.close();
			} finally {
				conn.close();
			}
		} finally {
			repo.shutDown();
		}
	}
    
}
