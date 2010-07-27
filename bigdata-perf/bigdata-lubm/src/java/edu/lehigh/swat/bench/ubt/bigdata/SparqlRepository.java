/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
 * Created on Apr 17, 2007
 */

package edu.lehigh.swat.bench.ubt.bigdata;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResultHandlerBase;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLParserFactory;

import com.bigdata.util.concurrent.DaemonThreadFactory;

import edu.lehigh.swat.bench.ubt.api.Query;
import edu.lehigh.swat.bench.ubt.api.QueryResult;
import edu.lehigh.swat.bench.ubt.api.Repository;

/**
 * SPARQL end point repository (read-only).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: BigdataRepository.java 2547 2010-03-24 20:44:07Z thompsonbry $
 */
public class SparqlRepository implements Repository {

	protected static final Logger log = Logger.getLogger(SparqlRepository.class);
	
	/**
	 * A SPARQL results set in XML.
	 */
	static final String MIME_SPARQL_RESULTS_XML = "application/sparql-results+xml";
	
	/**
	 * RDF/XML.
	 */
	static final String MIME_RDF_XML = "application/rdf+xml";

	/**
	 * The URL of the SPARQL service.
	 * 
	 * @see #open(String)
	 */
	private String serviceURL;

	private static final SPARQLResultsXMLParserFactory parserFactory = new SPARQLResultsXMLParserFactory();

	/**
	 * Runs a task for handling a request. This is initialized by
	 * {@link #open(String)} and shutdown by {@link #close()}.
	 */
	private ExecutorService queryService;

	public void clear() {
		throw new UnsupportedOperationException();
	}

	public void close() {

		if (queryService != null) {

			queryService.shutdownNow();

			queryService = null;
			
		}
		
	}

	public boolean load(String dataDir) {
		throw new UnsupportedOperationException();
	}

	public void open(String database) {

		this.serviceURL = database;

		if (queryService == null) {

			queryService = Executors
					.newSingleThreadExecutor(DaemonThreadFactory
							.defaultThreadFactory());

		}

	}

	public void setOntology(String ontology) {
		// NOP
	}

	public QueryResult issueQuery(Query query) {
		QueryOptions opts = new QueryOptions();
		opts.serviceURL = serviceURL;
		opts.queryStr = query.getString();
		try {
			return new SparqlQuery(opts).call();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * The default connection timeout (ms).  This needs to be large enough so that queries which take
	 *  a while to materialize their initial/next binding set do not timeout. 
	 */
	static private final int DEFAULT_TIMEOUT = Integer.MAX_VALUE;

	/**
	 * Options for the query.
	 */
	private static class QueryOptions {

		/** The URL of the SPARQL endpoint. */
		public String serviceURL = null;
		public String username = null;
		public String password = null;
		/** The SPARQL query. */
		public String queryStr = null;
		/** The default graph URI (optional). */
		public String defaultGraphUri = null;
		/** The connection timeout (ms). */
		public int timeout = DEFAULT_TIMEOUT;
		public boolean showQuery = false;

	}
	
	/**
	 * Used to indicate the end of the queue.
	 */
	private static final BindingSet poisonPill = new EmptyBindingSet();

	/**
	 * Class submits a SPARQL query using httpd and writes the result on stdout.
	 * 
	 * poison 
	 */
	private class SparqlQuery implements Callable<QueryResult> {

		final QueryOptions opts;

		/**
		 * 
		 * @param opts The query options.
		 */
		public SparqlQuery(final QueryOptions opts) {

			if (opts == null)
				throw new IllegalArgumentException();
			
			this.opts = opts;

		}

		public QueryResult call() throws Exception {

			/*
			 * Parse the query so we can figure out how it will need to be
			 * executed.
			 * 
			 * Note: This will fail a query on its syntax. However, the logic
			 * used in the tasks to execute a query will not fail a bad query
			 * for some reason which I have not figured out yet.
			 */
			final QueryParser engine = new SPARQLParserFactory().getParser();
			
			final ParsedQuery q = engine.parseQuery(opts.queryStr, null/*baseURI*/);

			if (opts.showQuery) {
				System.err.println("---- Original Query ----");
				System.err.println(opts.queryStr);
				System.err.println("----- Parsed Query -----");
				System.err.println(q.toString());
			}
			
			// Fully formed and encoded URL @todo use */* for ASK.
			final String urlString = opts.serviceURL
					+ "?query="
					+ URLEncoder.encode(opts.queryStr, "UTF-8")
					+ (opts.defaultGraphUri == null ? ""
							: ("&default-graph-uri=" + URLEncoder.encode(
									opts.defaultGraphUri, "UTF-8")));
			
			final URL url = new URL(urlString);
			HttpURLConnection conn = null;
			try {

				conn = (HttpURLConnection) url.openConnection();
				conn.setRequestMethod("GET");
				conn.setDoOutput(true);
				conn.setUseCaches(false);
				conn.setReadTimeout(opts.timeout);

				/*
				 * Set an appropriate Accept header for the query.
				 */
				conn.setRequestProperty("Accept", MIME_SPARQL_RESULTS_XML);

				// write out the request headers
				if (log.isDebugEnabled()) {
					log.debug("*** Request ***");
					log.debug(opts.serviceURL);
					log.debug(opts.queryStr);
				}

				try {
					// connect.
					conn.connect();

					final int rc = conn.getResponseCode();
					if (rc < 200 || rc >= 300) {
						throw new IOException(conn.getResponseMessage());
					}

					if (log.isDebugEnabled()) {
						/*
						 * write out the response headers
						 */
						log.debug("*** Response ***");
						log.debug("Status Line: " + conn.getResponseMessage());
					}

					/*
					 * The LUBM test harness requires us to return a
					 * QueryResult. That interface looks pretty much like an
					 * iterator. The openrdf platform provides us with a parser
					 * for the SPARQL results, which does not look like an
					 * iterator. In order to parse the results and have those
					 * results drive the iterator, we wrap the [is] reading on
					 * the connection with the parser, pump the results into a
					 * blocking queue, and then have the QueryResult drain that
					 * queue. If the QueryResult is closed, then we close the
					 * URL Connection [conn]. Likewise, we close the [conn] if
					 * the parser task throws an exception.
					 * 
					 * Note: If the client closes the connection, then the
					 * InputStream passed into the Response will be closed and
					 * the task will terminate rather than running on in the
					 * background with a disconnected client.
					 */

					final BlockingQueue<BindingSet> queue = new LinkedBlockingQueue<BindingSet>(
							1000/* capacity */);

					final FutureTask<Void> ft = new FutureTask<Void>(
							new ParserTask(conn, queue));

					// Begin executing the query (asynchronous).
					queryService.execute(ft);

					return new MyQueryResult(conn, queue);
					
				} catch (Throwable t) {
					/*
					 * If something goes wrong before we have the parser setup,
					 * then close the http connection. Otherwise, the connection
					 * will be closed by either the parser or the QueryResult.
					 */
					try {
						// clean up the connection resources
						if (conn != null)
							conn.disconnect();
					} catch (Throwable t2) {
						// ignored.
					}
					throw new RuntimeException(t);
				}

			} finally {

			}

		}

		/**
		 * Parses the SPARQL result, writing the {@link BindingSet}s onto a queue.
		 */
		private class ParserTask implements Callable<Void> {

			private final HttpURLConnection conn;
			private final BlockingQueue<BindingSet> queue;

			public ParserTask(final HttpURLConnection conn,
					final BlockingQueue<BindingSet> queue) {

				this.conn = conn;
				this.queue = queue;

			}

			public Void call() throws Exception {
				try {
					final TupleQueryResultParser parser = parserFactory
							.getParser();
					parser
							.setTupleQueryResultHandler(new TupleQueryResultHandlerBase() {
								// Indicates the end of a sequence of solutions.
								public void endQueryResult() {
									// handled in finally{}
								}

								// Handles a solution.
								public void handleSolution(final BindingSet bindingSet) {
									if(log.isTraceEnabled()) {
										log.trace(bindingSet.toString());
									}
									try {
										queue.put(bindingSet);
									} catch (InterruptedException e) {
										throw new RuntimeException(e);
									}
								}

								// Indicates the start of a sequence of
								// Solutions.
								public void startQueryResult(
										List<String> bindingNames) {
								}
							});
					log.debug("Parsing...");
					parser.parse(conn.getInputStream());
					log.debug("Parsing...done");
					// done.
					return null;
				} catch (Throwable t) {
					log.error(t, t);
					throw new RuntimeException(t);
				} finally {
					try {
						// mark the end of the binding sets.
						log.debug("Poison pill put");
						queue.put(poisonPill);
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					} finally {
						// terminate the http connection.
						log.debug("Closing http connection");
						conn.disconnect();
					}
				}

			}

		} // class ParserTask
		
	    /**
	     * Counts the #of results from the query and closes the connection.
	     */
		private class MyQueryResult implements QueryResult {

	    	private boolean open = true;
			private final HttpURLConnection conn;

			private final BlockingQueue<?extends BindingSet> queue;

			private long nsolutions = 0;

			public MyQueryResult(
					final HttpURLConnection conn,
					final BlockingQueue<?extends BindingSet> queue) {

				if (conn == null)
					throw new IllegalStateException();

				if (queue == null)
					throw new IllegalStateException();

				this.conn = conn;

	            this.queue = queue;

			}

			protected void finalize() throws Throwable {

				close();

				super.finalize();
				
			}

			synchronized private void close() {

				if(!open) return;
				
				log.debug("Closing http connection");
				
				open = false;

				try {
	
					conn.disconnect();

				} catch (Throwable t) {
					
					log.error(t, t);
					
				}

			}

			public long getNum() {

				return nsolutions;

			}

			public boolean next() {

				try {

					final BindingSet bset = queue.take();
					
					if(bset == poisonPill) {
						
						log.debug("Poison pill taken");
						
						close();
						
						return false;
						
					}

					nsolutions++;

					return true;

				} catch (InterruptedException e) {

					close();

					throw new RuntimeException(e);

				}

			}

		} // class MyQueryResult

	} // class SparqlQuery

}
