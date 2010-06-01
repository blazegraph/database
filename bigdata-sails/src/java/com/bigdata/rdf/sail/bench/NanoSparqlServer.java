/*

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on May 29, 2010
 */
package com.bigdata.rdf.sail.bench;

import info.aduna.xml.XMLWriter;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import org.openrdf.sail.SailException;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailGraphQuery;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.bench.NanoSparqlClient.QueryType;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.AbstractResource;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.httpd.AbstractHTTPD;
import com.bigdata.util.httpd.NanoHTTPD;

/**
 * A flyweight SPARQL endpoint using HTTP.
 * 
 * @author thompsonbry@users.sourceforge.net
 * 
 * @todo Allow configuration options for the sparql endpoint either as URI
 *       parameters, in the property file, as request headers, or as query hints
 *       using the PREFIX mechanism.
 * 
 * @todo Allow timestamp for server reads as protocol parameter (URL query
 *       parameter or header).
 * 
 * @todo Add an "EXPLAIN" query type and show the execution plan and costs.
 */
public class NanoSparqlServer extends AbstractHTTPD {

	/**
	 * The logger for the concrete {@link NanoSparqlServer} class.  The {@link NanoHTTPD}
	 * class has its own logger.
	 */
	static protected final Logger log = Logger.getLogger(NanoSparqlServer.class); 
	
	/**
	 * A SPARQL results set in XML.
	 */
	static private final String MIME_SPARQL_RESULTS_XML = "application/sparql-results+xml";

	/**
	 * RDF/XML.
	 */
	static private final String MIME_RDF_XML = "application/rdf+xml";

	/**
	 * The character set used for the response (not negotiated).
	 */
    static private final String charset = "UTF-8";
    
	/**
	 * The target repository.
	 */
	final BigdataSailRepository repo;

	/**
	 * @todo use to decide ASK, DESCRIBE, CONSTRUCT, SELECT, EXPLAIN, etc.
	 */
	private final QueryParser engine;
	
    /**
     * Runs a pool of threads for handling requests.
     */
    private final ExecutorService queryService;

	public NanoSparqlServer(final int port, final String namespace,
			final IIndexManager indexManager) throws IOException,
			SailException, RepositoryException {

		super(port);

		// resolve the kb instance of interest.
		final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
				.getResourceLocator().locate(namespace, ITx.UNISOLATED);

		if (tripleStore == null) {

			throw new RuntimeException("No such kb: " + namespace);

		}
		
		// since the kb exists, wrap it as a sail.
		final BigdataSail sail = new BigdataSail(tripleStore);

		// Log some information about the kb (#of statements, etc).
		System.out.println(getKBInfo(sail)); // @todo log @ info?

		repo = new BigdataSailRepository(sail);
		repo.initialize();

		// used to parse qeries.
        engine = new SPARQLParserFactory().getParser();

        // @todo parameter and configuration of same.
        final int requestServicePoolSize = 0;

        if (requestServicePoolSize == 0) {

            queryService = (ThreadPoolExecutor) Executors
                    .newCachedThreadPool(new DaemonThreadFactory
                            (getClass().getName()+".queryService"));

        } else {

            queryService = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                    requestServicePoolSize, new DaemonThreadFactory
                    (getClass().getName()+".queryService"));

        }

	}

    /**
     * Return various interesting metadata about the KB state.
     */
	protected StringBuilder getKBInfo(final BigdataSail sail) {

		final StringBuilder sb = new StringBuilder();

		try {

			final AbstractTripleStore tripleStore = sail.getDatabase();

			sb.append("class\t = " + tripleStore.getClass().getName() + "\n");

			sb
					.append("indexManager\t = "
							+ tripleStore.getIndexManager().getClass()
									.getName() + "\n");

			sb.append("namespace\t = " + tripleStore.getNamespace() + "\n");

			sb.append("timestamp\t = "
					+ TimestampUtility.toString(tripleStore.getTimestamp())
					+ "\n");

			sb.append("statementCount\t = " + tripleStore.getStatementCount()
					+ "\n");

			sb.append("termCount\t = " + tripleStore.getTermCount() + "\n");

			sb.append("uriCount\t = " + tripleStore.getURICount() + "\n");

			sb.append("literalCount\t = " + tripleStore.getLiteralCount() + "\n");

			sb.append("bnodeCount\t = " + tripleStore.getBNodeCount() + "\n");

			sb.append(IndexMetadata.Options.BTREE_BRANCHING_FACTOR
					+ "="
					+ tripleStore.getSPORelation().getPrimaryIndex()
							.getIndexMetadata().getBranchingFactor() + "\n");

			sb.append(IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY
					+ "="
					+ tripleStore.getSPORelation().getPrimaryIndex()
							.getIndexMetadata()
							.getWriteRetentionQueueCapacity() + "\n");

			sb.append(BigdataSail.Options.STAR_JOINS + "=" + sail.isStarJoins()
					+ "\n");

			sb.append(AbstractResource.Options.MAX_PARALLEL_SUBQUERIES + "="
					+ tripleStore.getMaxParallelSubqueries() + "\n");

			/*
			 * Stuff which is specific to a local/embedded database.
			 */
			if (tripleStore.getIndexManager() instanceof IJournal) {

				final AbstractJournal jnl = (AbstractJournal) sail.getDatabase()
						.getIndexManager();

				sb.append("file\t= " + jnl.getFile()+"\n");

				sb.append("BufferMode\t= " + jnl.getBufferStrategy().getBufferMode()+"\n");

				sb.append("nextOffset\t= "
						+ jnl.getRootBlockView().getNextOffset() + "\n");

				/*
				 * @todo The rest of this is all metadata that is only
				 * interesting after the server has been running for a while.
				 * It could be issued in response to a STATUS message.
				 */
				
//				if (LRUNexus.INSTANCE != null)
//					sb.append(LRUNexus.INSTANCE.toString() + "\n");
				
				// if (false) {
				//
				// // show the disk access details.
				//
				// sb.append(((Journal) sail.getDatabase().getIndexManager())
				// .getBufferStrategy().getCounters().toString());
				//
				// }

			}

			// sb.append(tripleStore.predicateUsage());

		} catch (Throwable t) {

			log.warn(t.getMessage(), t);

		}

		return sb;

    }
    
    synchronized public void shutdown() {
    	queryService.shutdown();
    	super.shutdown();
		synchronized (alive) {
			alive.set(false);
			alive.notifyAll();
		}
    }

    synchronized public void shutdownNow() {
    	queryService.shutdownNow();
    	super.shutdown();
		synchronized (alive) {
			alive.set(false);
			alive.notifyAll();
		}
    }
    
    private final AtomicBoolean alive = new AtomicBoolean(true);

//    @Override
//    public Response serve(final String uri, final String method,
//            final Properties header,
//            final LinkedHashMap<String, Vector<String>> parms) {
//    
//        if ("STOP".equalsIgnoreCase(method)) {
//
//        	queryService.execute(new Runnable(){
//        		public void run() {
//        			shutdown();
//        		}
//        	});
//        	
//            return new Response(HTTP_OK, MIME_TEXT_PLAIN, "Shutting down.");
//            
//        }
//        
//    	return super.serve(uri, method, header, parms);
//    	
//    }

	@Override
	public Response doPost(final String uri, final String method,
			final Properties header,
			final LinkedHashMap<String, Vector<String>> params) throws Exception {

		if (log.isInfoEnabled()) {
			log.info("uri=" + uri);
			log.info("method=" + method);
			log.info("headser=" + header);
			log.info("params=" + params);
		}
		
		if("/stop".equals(uri)) {

			queryService.execute(new Runnable() {
				public void run() {
					System.err.println("Will shutdown.");
					try {
						/*
						 * Sleep for a bit so the Response will be delivered
						 * before we shutdown the server.
						 */
						Thread.sleep(100/* ms */);
					} catch (InterruptedException ex) {
						// ignore
					}
					// Shutdown the server.
					shutdown();
				}
			});

//			// Shutdown.
//			shutdown();
//			// Note: Client probably will not see this response.
			return new Response(HTTP_OK, MIME_TEXT_PLAIN, "Shutting down.");

		}
		
		return new Response(HTTP_NOTIMPLEMENTED, MIME_TEXT_PLAIN,
				"SPARQL POST QUERY not implemented.");

	}
	
    /**
	 * Accepts SPARQL queries.
	 */
	@Override
	public Response doGet(final String uri, final String method,
			final Properties header,
			final LinkedHashMap<String, Vector<String>> params) throws Exception {

		if (log.isInfoEnabled()) {
			log.info("uri=" + uri);
			log.info("method=" + method);
			log.info("headser=" + header);
			log.info("params=" + params);
		}

		final String queryStr = params.get("query").get(0);

		if (log.isInfoEnabled())
			log.info("query: " + queryStr);

		/*
		 * Setup pipes. The [os] will be passed into the task that executes the
		 * query. The [is] will be passed into the Response. The task is
		 * executed on a thread pool.
		 * 
		 * Note: If the client closes the connection, then the InputStream
		 * passed into the Response will be closed and the task will terminate
		 * rather than running on in the background with a disconnected client.
		 */
		final PipedOutputStream os = new PipedOutputStream();
		final InputStream is = new PipedInputStream(os);//Bytes.kilobyte32*8/*pipeSize*/);
		final FutureTask<Void> ft = new FutureTask<Void>(getQueryTask(queryStr,
				os));
		try {

			// Begin executing the query (asynchronous).
			queryService.execute(ft);
			
			// Choose an appropriate MIME type.
			final String mimeType;
			final QueryType queryType = QueryType.fromQuery(queryStr);
			switch(queryType) {
			case DESCRIBE:
			case CONSTRUCT:
				mimeType = MIME_RDF_XML;
				break;
			case ASK:
			case SELECT:
				mimeType = MIME_SPARQL_RESULTS_XML;
				break;
			default:
				throw new RuntimeException("Unknown query type: "+queryType);
			}

			// Setup the response.
			final Response r = new Response(HTTP_OK, mimeType + "; charset='"
					+ charset + "'", is);

			/*
			 * Sets the cache behavior.
			 */
			// r.addHeader("Cache-Control",
			// "max-age=60, must-revalidate, public");
			// to disable caching.
			// r.addHeader("Cache-Control", "no-cache");

			return r;

		} catch (Throwable e) {

			throw launderThrowable(e, os);

		}

	}

	static private RuntimeException launderThrowable(final Throwable t,
			final OutputStream os) throws Exception {
		if (os != null) {
			try {
				/*
				 * Write the stack trace onto the output stream. This will show
				 * up in the client's response. This code path will only be
				 * taken if we have already begun writing the response.
				 * Otherwise, an HTTP error status will be used instead.
				 */
				t.printStackTrace(new PrintWriter(os));
				// flush the output stream.
				os.flush();
			} finally {
				// ignore any problems here.
			}
			try {
				// ensure output stream is closed.
				os.close();
			} catch (Throwable t2) {
				// ignore any problems here.
			}
		}
		try {
			// log an error for the service.
			log.error(t, t);
		} finally {
			// ignore any problems here.
		}
		if (t instanceof RuntimeException) {
			return (RuntimeException) t;
		} else if (t instanceof Error) {
			throw (Error) t;
		} else if (t instanceof Exception) {
			throw (Exception) t;
		} else
			throw new RuntimeException(t);
	}

	/**
	 * Return the task which will execute the query.
	 * 
	 * @param queryStr
	 *            The query.
	 * @param os
	 *            Where the task will write its output.
	 *            
	 * @return The task.
	 * 
	 * @throws MalformedQueryException 
	 */
	private Callable<Void> getQueryTask(final String queryStr,
			final PipedOutputStream os) throws MalformedQueryException {

		/*
		 * Parse the query so we can figure out how it will need to be executed.
		 * 
		 * Note: This will fail a query on its syntax. However, the logic used
		 * in the tasks to execute a query will not fail a bad query for some
		 * reason which I have not figured out yet. Therefore, we are in the
		 * position of having to parse the query here and then again when it is
		 * executed.
		 */
		final ParsedQuery q = engine.parseQuery(queryStr, null/*baseURI*/);

		final NanoSparqlClient.QueryType queryType = NanoSparqlClient.QueryType
				.fromQuery(queryStr);

		switch (queryType) {
		//case ASK:
		// @todo ASK as boolean - which task to run?
		case DESCRIBE:
		case CONSTRUCT:
			return new GraphQueryTask(queryStr, os);
		case SELECT:
			return new TupleQueryTask(queryStr, os);
		}

		throw new RuntimeException("Unknown query type: " + queryType);

	}

	/**
	 * Note: A read-only connection from the lastCommitTime
	 * 
	 * @throws RepositoryException
	 */
	protected SailRepositoryConnection getQueryConnection()
			throws RepositoryException {

		return repo.getReadOnlyConnection();

    }

	/**
	 * Executes a tuple query.
	 */
	private class TupleQueryTask implements Callable<Void> {

		private final String queryStr;
		private final PipedOutputStream os;

		public TupleQueryTask(final String queryStr, final PipedOutputStream os) {

			this.queryStr = queryStr;
			this.os = os;

		}

		public Void call() throws Exception {
			final SailRepositoryConnection cxn = getQueryConnection();
			try {
				final TupleQuery query = cxn.prepareTupleQuery(
						QueryLanguage.SPARQL, queryStr);
				query.evaluate(new SPARQLResultsXMLWriter(new XMLWriter(os)));
				os.close();
				return null;
			} catch (Throwable t) {
				// launder and rethrow the exception.
				throw launderThrowable(t,os);
			} finally {
				cxn.close();
			}
		}

	}

	/**
	 * Executes a graph query.
	 */
	private class GraphQueryTask implements Callable<Void> {

		private final String queryStr;
		private final PipedOutputStream os;

		public GraphQueryTask(final String queryStr, final PipedOutputStream os) {

			this.queryStr = queryStr;
			this.os = os;

		}

		public Void call() throws Exception {
			final SailRepositoryConnection cxn = getQueryConnection();
			try {
				final BigdataSailGraphQuery query = (BigdataSailGraphQuery) cxn
						.prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
				/*
				 * FIXME BSBM constructs statements with values not actually in
				 * the database, which breaks the native construct iterator.
				 */
				query.setUseNativeConstruct(false);
				query.evaluate(new RDFXMLWriter(os));
				os.close();
				return null;
			} catch (Throwable t) {
				throw launderThrowable(t, os);
			} finally {
				cxn.close();
			}
		}
	
	}

	/**
	 * Send a STOP message to the service
	 *
	 * @param port The port for that service.
	 * 
	 * @throws IOException 
	 * 
	 * @todo This winds up warning <pre> java.net.SocketTimeoutException: Read timed out</pre>
	 * even though the shutdown request was accepted and processed by the server.  I'm not
	 * sure why. 
	 */
	public static void sendStop(int port) throws IOException {
		
		final URL url = new URL("http://localhost:" + port+"/stop");
		HttpURLConnection conn = null;
		try {

			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setDoInput(true); // true to read from the server.
			conn.setDoOutput(true); // true to write to the server.
			conn.setUseCaches(false);
			conn.setReadTimeout(2000/* ms */);
			conn.setRequestProperty("Content-Type",
					"application/x-www-form-urlencoded");
			conn.setRequestProperty("Content-Length", "" + Integer.toString(0));
			conn.setRequestProperty("Content-Language", "en-US");

			// Send request
			conn.getOutputStream().close();

			// connect.
			try {

				conn.connect();

				final int rc = conn.getResponseCode();
				
				if (rc < 200 || rc >= 300) {
					
					log.error(conn.getResponseMessage());
					
				}

				System.out.println(conn.getResponseMessage());

			} catch (IOException ex) {

				log.warn(ex);
				
			}

		} finally {

			// clean up the connection resources
			if (conn != null)
				conn.disconnect();

		}

	}
	
	private static void usage() {
		
		System.err.println("port (-stop | <i>namespace</i> (propertyFile|configFile) )");
		
	}
	
	/**
	 * Run an httpd service exposing a SPARQL endpoint. The service will respond
	 * at the root path for the specified port.
	 * 
	 * @param args
	 *            USAGE:
	 *            <code>port (-stop | <i>namespace</i> (propertyFile|configFile) )</code>
	 *            where
	 *            <dl>
	 *            <dt>port</dt>
	 *            <dd>The port on which the service will respond.</dd>
	 *            <dt>namespace</dt>
	 *            <dd>The namespace of the target KB instance ("kb" is the
	 *            default namespace).</dd>
	 *            <dt>propertyFile</dt>
	 *            <dd>A java properties file for a standalone {@link Journal}</dd>
	 *            <dt>configFile</dt>
	 *            <dd>A jini configuration file for a bigdata federation</dd>
	 *            </dl>
	 * 
	 * @todo introduce "-jnl" or "-fed" options to specify the standalone
	 *       journal or a jini federation?
	 */
	public static void main(final String[] args) {
		int port = 80;
		Journal jnl = null;
		JiniClient<?> jiniClient = null;
		NanoSparqlServer server = null;
		try {
			if (args.length == 2) {
				port = Integer.valueOf(args[0]);
				if("-stop".equals(args[1])) {
					// Send stop to server.
					sendStop(port);
					// Normal exit.
					System.exit(0);
				}
			}
			if (args.length != 3) {
				usage();
				System.exit(1);
			}
			port = Integer.valueOf(args[0]);
			final String namespace = args[1];
			final String propertyFile = args[2];
			final File file = new File(propertyFile);
			if (!file.exists()) {
				throw new RuntimeException("Could not find file: " + file);
			}
			boolean isJini = false;
			if (propertyFile.endsWith(".config")) {
				isJini = true;
			} else if (propertyFile.endsWith(".properties")) {
				isJini = false;
			} else {

				/*
				 * Note: This is a hack, but we are recognizing the jini
				 * configuration file with a .config extension and the journal
				 * properties file with a .properties extension.
				 */
				System.err
						.println("File should have '.config' or '.properties' extension: "
								+ file);
				System.exit(1);
			}
			System.out.println("port: " + port);
			System.out.println("namespace: " + namespace);
			System.out.println("file: " + file.getAbsoluteFile());

			/*
			 * Connect to the database.
			 */
			final IIndexManager indexManager;
			{

				if (isJini) {

					/*
					 * A bigdata federation.
					 */

					jiniClient = new JiniClient(new String[] { propertyFile });

					indexManager = jiniClient.connect();

				} else {

					/*
					 * Note: we only need to specify the FILE when re-opening a
					 * journal containing a pre-existing KB.
					 */
					final Properties properties = new Properties();
					{
						// Read the properties from the file.
						final InputStream is = new BufferedInputStream(
								new FileInputStream(propertyFile));
						try {
							properties.load(is);
						} finally {
							is.close();
						}
						if (System.getProperty(BigdataSail.Options.FILE) != null) {
							// Override/set from the environment.
							properties
									.setProperty(
											BigdataSail.Options.FILE,
											System
													.getProperty(BigdataSail.Options.FILE));
						}
					}

					indexManager = jnl = new Journal(properties);

				}

			}

			server = new NanoSparqlServer(port, namespace, indexManager);

			/*
			 * Wait until the server is terminated.
			 */
			synchronized (server.alive) {

				while (server.alive.get()) {

					server.alive.wait();

				}

				System.out.println("Service is down.");

			}

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (server != null)
				server.shutdownNow();
			if (jnl != null) {
				jnl.close();
			}
			if (jiniClient != null) {
				jiniClient.disconnect(true/* immediateShutdown */);
			}
		}

	}

}
