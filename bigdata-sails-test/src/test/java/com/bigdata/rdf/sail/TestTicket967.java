package com.bigdata.rdf.sail;

import java.io.IOException;
import java.util.Properties;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.memory.MemoryStore;

/**
 * 
 * Ticket 967 is an EOF exception on high load of HttpClient requests
 */
public class TestTicket967 extends QuadsTestCase {

	public TestTicket967() {
	}

	public TestTicket967(String arg0) {
		super(arg0);
	}

	public void testBug() throws Exception {
		// try with Sesame MemoryStore:
		executeTest(new SailRepository(new MemoryStore()));

		// try with Bigdata:
		try {
			executeTest(new BigdataSailRepository(getSail()));
		} finally {
			getSail().__tearDownUnitTest();
		}
	}

	private void executeTest(final SailRepository repo)
			throws RepositoryException, MalformedQueryException,
			QueryEvaluationException, RDFParseException, RDFHandlerException,
			IOException {
		try {
			repo.initialize();
			final RepositoryConnection conn = repo.getConnection();
			try {
				conn.setAutoCommit(false);
				final ValueFactory vf = conn.getValueFactory();
		        final URI uri = vf.createURI("os:/elem/example");
		        // run a query which looks for a statement and then adds it if it is not found.
		        addDuringQueryExec(conn, uri, RDF.TYPE, vf.createURI("os:class/Clazz"));
		        // now try to export the statements.
	        	final RepositoryResult<Statement> stats = conn.getStatements(null, null, null, false);
		        try {
		        	// materialize the newly added statement.
		        	stats.next();
		        } catch (RuntimeException e) {
		        	fail(e.getLocalizedMessage(), e); // With Bigdata this fails
		        } finally {
		        	stats.close();
		        }
		        conn.rollback(); // discard the result (or commit, but do something to avoid a logged warning from Sesame).
			} finally {
				conn.close();
			}
		} finally {
			repo.shutDown();
		}
	}
	
	private void addDuringQueryExec(final RepositoryConnection conn,
			final Resource subj, final URI pred, final Value obj,
			final Resource... ctx) throws RepositoryException,
			MalformedQueryException, QueryEvaluationException {
		final TupleQuery tq = conn.prepareTupleQuery(QueryLanguage.SPARQL,
        		"select distinct ?s ?p ?o where{?s ?p ?t . ?t <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?o }"
        		);
        tq.setBinding("s", subj);
        tq.setBinding("p", pred);
        tq.setBinding("o", obj);
        final TupleQueryResult tqr = tq.evaluate();
        try {
            if (!tqr.hasNext()) {
                conn.add(subj, pred, obj, ctx);
            }
        } finally {
            tqr.close();
        }
    }
}
