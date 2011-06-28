package alex;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.rdfxml.RDFXMLParser;

import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;

/**
* @author Alexander De Leon
*/
public class LoadPdb {

	private static final Logger LOG = Logger.getLogger(LoadPdb.class);

	public static void main(String[] args) throws Exception {
		File file = new File("tmp-1Y26.jnl");

		Properties properties = new Properties();
		properties.setProperty(BigdataSail.Options.FILE, file.getAbsolutePath());

		BigdataSail sail = new BigdataSail(properties);
		Repository repo = new BigdataSailRepository(sail);
		repo.initialize();

        if(false) {
        sail.getDatabase().getDataLoader().loadData(
                "contrib/src/problems/alex/1Y26.rdf",
                new File("contrib/src/problems/alex/1Y26.rdf").toURI()
                        .toString(), RDFFormat.RDFXML);
        sail.getDatabase().commit();
        }
        
//		loadSomeDataFromADocument(repo, "contrib/src/problems/alex/1Y26.rdf");
		// readSomeData(repo);
		executeSelectQuery(repo, "SELECT * WHERE { ?s ?p ?o }");
	}

    
	public static void loadSomeDataFromADocument(Repository repo, String documentPath) throws Exception {
		int counter = 0;
		File file = new File(documentPath);
		StatementCollector collector = new StatementCollector();
		InputStream in = new FileInputStream(file);
		try {
			RDFParser parser = new RDFXMLParser();
			parser.setRDFHandler(collector);
			parser.parse(in, file.toURI().toString());
		} finally {
			in.close();
		}

		RepositoryConnection cxn = repo.getConnection();
		cxn.setAutoCommit(false);

		Statement stmt = null;
		try {
			for (Iterator<Statement> i = collector.getStatements().iterator(); i.hasNext();) {
				stmt = i.next();
				cxn.add(stmt);
				counter++;
			}
			LOG.info("Loaded " + counter + " triples");
			cxn.commit();
		} catch (Exception e) {
			LOG.error("error inserting statement: " + stmt, e);
			cxn.rollback();
		} finally {
			cxn.close();
		}

	}

	public static void readSomeData(Repository repo) throws Exception {

		RepositoryConnection cxn = repo.getConnection();
		int counter = 0;
		try {

			RepositoryResult<Statement> stmts = cxn.getStatements(null, null, null, true /*
																						* include
																						* inferred
																						*/);
			while (stmts.hasNext()) {
				Statement stmt = stmts.next();
				Resource s = stmt.getSubject();
				URI p = stmt.getPredicate();
				Value o = stmt.getObject();
				// do something with the statement
				LOG.info(stmt);

				// cast to BigdataStatement to get at additional information
				BigdataStatement bdStmt = (BigdataStatement) stmt;
				if (bdStmt.isExplicit()) {
					// do one thing
				} else if (bdStmt.isInferred()) {
					// do another thing
				} else { // bdStmt.isAxiom()
					// do something else
				}
				LOG.info(bdStmt.getStatementType());
				counter++;
			}

		} finally {
			// close the repository connection
			cxn.close();

			LOG.info("Number of Triples: " + counter);
		}

	}

	public static void executeSelectQuery(Repository repo, String query) throws Exception {

		RepositoryConnection cxn = repo.getConnection();
		int counter = 0;
		try {

			final TupleQuery tupleQuery = cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
			tupleQuery.setIncludeInferred(true /* includeInferred */);
			TupleQueryResult result = tupleQuery.evaluate();
			// do something with the results
			while (result.hasNext()) {
				BindingSet bindingSet = result.next();
				LOG.info(bindingSet);
				counter++;
			}

		} finally {
			// close the repository connection
			cxn.close();
//			LOG.info
            System.err.println
            ("Number of results: " + counter);
		}

	}
}
