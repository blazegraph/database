package com.bigdata.rdf.sail.bench;

import info.aduna.iteration.CloseableIteration;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.sail.SailException;

import com.bigdata.journal.Journal;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Hard codes LUBM U14, which is a statement index scan.
 * 
 * <pre>
 * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 * PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
 * SELECT ?x
 * WHERE{ ?x a ub:UndergraduateStudent . }
 * </pre>
 * 
 * <pre>
 * http://www.w3.org/1999/02/22-rdf-syntax-ns#type (TermId(8U))
 * 
 * http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent (TermId(324U))
 * </pre>
 */
public class Q14Test {

	public static void main(String[] args) throws IOException, SailException {

		final String namespace = "LUBM_U1000";
		final String propertyFile = "/root/workspace/bigdata-journal-HA/bigdata-perf/lubm/ant-build/bin/RWStore.properties";
		final String journalFile = "/data/lubm/U1000/bigdata-lubm.RW.jnl";

		final Properties properties = new Properties();
		{
			// Read the properties from the file.
			final InputStream is = new BufferedInputStream(new FileInputStream(
					propertyFile));
			try {
				properties.load(is);
			} finally {
				is.close();
			}
			if (System.getProperty(BigdataSail.Options.FILE) != null) {
				// Override/set from the environment.
				properties.setProperty(BigdataSail.Options.FILE, System
						.getProperty(BigdataSail.Options.FILE));
			}
			if(properties.getProperty(BigdataSail.Options.FILE)==null) {
				properties.setProperty(BigdataSail.Options.FILE, journalFile);
			}
		}

		final Journal jnl = new Journal(properties);
		try {

			final AbstractTripleStore database = (AbstractTripleStore) jnl
					.getResourceLocator().locate(namespace,
							jnl.getLastCommitTime());
			
			if (database == null)
				throw new RuntimeException("Not found: " + namespace);
			
			/*
			 * Resolve terms against the lexicon.
			 */
			final BigdataValueFactory f = database.getLexiconRelation()
					.getValueFactory();
			
			final BigdataURI rdfType = f
					.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
			
			final BigdataURI undergraduateStudent = f
					.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent");
			
			database.getLexiconRelation().addTerms(
					new BigdataValue[] { rdfType, undergraduateStudent }, 2,
					true/* readOnly */);

			/*
			 * Run the index scan without materializing anything from the lexicon. 
			 */
			if(true){
				System.out.println("Running SPO only access path.");
				final long begin = System.currentTimeMillis();
				final IAccessPath<ISPO> accessPath = database.getAccessPath(
						null/* s */, rdfType, undergraduateStudent);
				final IChunkedOrderedIterator<ISPO> itr = accessPath.iterator();
				try {
					while (itr.hasNext()) {
						itr.next();
					}
				} finally {
					itr.close();
				}
				final long elapsed = System.currentTimeMillis() - begin;
				System.err.println("Materialize SPOs      : elapsed=" + elapsed
						+ "ms");
			}

			/*
			 * Open the sail and run Q14.
			 */
			if(true){
				final BigdataSail sail = new BigdataSail(database);
				sail.initialize();
				final BigdataSailConnection conn = sail.getReadOnlyConnection();
				try {
					System.out.println("Materializing statements.");
					final long begin = System.currentTimeMillis();
					final CloseableIteration<? extends Statement, SailException> itr = conn
							.getStatements(null/* s */, rdfType,
									undergraduateStudent,
									true/* includeInferred */);
					try {
						while (itr.hasNext()) {
							itr.next();
						}
					} finally {
						itr.close();
					}
					final long elapsed = System.currentTimeMillis() - begin;
					System.err.println("Materialize statements: elapsed="
							+ elapsed + "ms");
				} finally {
					conn.close();
				}
				sail.shutDown();
			}
		} finally {
			jnl.close();
		}

	}

}
