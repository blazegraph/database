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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.openrdf.model.Graph;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResultHandlerBase;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLParserFactory;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.rdfxml.RDFXMLParser;

import com.bigdata.counters.CAT;
import com.bigdata.jsr166.LinkedBlockingQueue;

/**
 * A flyweight utility for issuing queries to an http SPARQL endpoint.
 * 
 * @author thompsonbry@users.sourceforge.net
 */
public class NanoSparqlClient {

	private static final Logger log = Logger.getLogger(NanoSparqlClient.class);
	
	/**
	 * A SPARQL results set in XML.
	 */
	static final String MIME_SPARQL_RESULTS_XML = "application/sparql-results+xml";
	/**
	 * RDF/XML.
	 */
	static final String MIME_RDF_XML = "application/rdf+xml";

	/**
	 * The default connection timeout (ms).
	 */
	static private final int DEFAULT_TIMEOUT = 60*1000;

	/**
	 * Helper class to figure out the type of a query.
	 */
	public static enum QueryType {
		
		ASK(0),
		DESCRIBE(1),
		CONSTRUCT(2),
		SELECT(3);
		
		private final int order;
		
		private QueryType(final int order) {
			this.order = order;
		}

		private static QueryType getQueryType(final int order) {
			switch (order) {
			case 0:
				return ASK;
			case 1:
				return DESCRIBE;
			case 2:
				return CONSTRUCT;
			case 3:
				return SELECT;
			default:
				throw new IllegalArgumentException("order=" + order);
			}
		}

		/**
		 * Used to note the offset at which a keyword was found. 
		 */
		static private class P implements Comparable<P> {

			final int offset;
			final QueryType queryType;

			public P(final int offset, final QueryType queryType) {
				this.offset = offset;
				this.queryType = queryType;
			}
			/** Sort into descending offset. */
			public int compareTo(final P o) {
				return o.offset - offset;
			}
		}

		/**
		 * Hack returns the query type based on the first occurrence of the
		 * keyword for any known query type in the query.
		 * 
		 * @param queryStr
		 *            The query.
		 * 
		 * @return The query type.
		 */
		static public QueryType fromQuery(final String queryStr) {

			// force all to lower case.
			final String s = queryStr.toUpperCase();

			final int ntypes = QueryType.values().length;

			final P[] p = new P[ntypes];

			int nmatch = 0;
			for (int i = 0; i < ntypes; i++) {

				final QueryType queryType = getQueryType(i);
				
				final int offset = s.indexOf(queryType.toString());

				if (offset == -1)
					continue;

				p[nmatch++] = new P(offset, queryType); 
				
			}

			if (nmatch == 0) {

				throw new RuntimeException(
						"Could not determine the query type: " + queryStr);

			}

			Arrays.sort(p, 0/* fromIndex */, nmatch/* toIndex */);

			final P tmp = p[0];

//			System.out.println("QueryType: offset=" + tmp.offset + ", type="
//					+ tmp.queryType);

			return tmp.queryType;

		}
		
	}
	
	/**
	 * Class runs a SPARQL query against an HTTP endpoint.
	 */
	static public class QueryTask implements Callable<Void> {

//		private final HttpClient client;
		final QueryOptions opts;

		/**
		 * 
		 * @param opts The query options.
		 */
		public QueryTask(/* HttpClient client, */ final QueryOptions opts) {

			if (opts == null)
				throw new IllegalArgumentException();
			
			// this.client = client;
			this.opts = opts;

		}

		public Void call() throws Exception {

			// used to measure the total execution time.
			final long begin = System.nanoTime();

			/*
			 * Parse the query so we can figure out how it will need to be
			 * executed.
			 * 
			 * Note: This will fail a query on its syntax. However, the logic
			 * used in the tasks to execute a query will not fail a bad query
			 * for some reason which I have not figured out yet.
			 */
			final QueryParser engine = new SPARQLParserFactory().getParser();
			
			final ParsedQuery q = engine.parseQuery(opts.queryStr, opts.baseURI);

			if (opts.showQuery) {
				System.err.println("---- " + Thread.currentThread().getName()
						+ " : Query "
						+ (opts.source == null ? "" : " : " + opts.source)
						+ "----");
				System.err.println(opts.queryStr);
			}
			if (opts.showParseTree) {
				System.err.println("----- Parse Tree "
						+ (opts.source == null ? "" : " : " + opts.source)
						+ "-----");
				System.err.println(q.toString());
			}
			
			// Fully formed and encoded URL @todo use */* for ASK.
			final String urlString = opts.serviceURL
					+ "?query="
					+ URLEncoder.encode(opts.queryStr, "UTF-8")
					+ (opts.defaultGraphUri == null ? ""
							: ("&default-graph-uri=" + URLEncoder.encode(
									opts.defaultGraphUri, "UTF-8")));
			
//			final HttpMethod method = new GetMethod(url);
			final URL url = new URL(urlString);
			HttpURLConnection conn = null;
			try {

				/*
				 * Setup connection properties.
				 * 
				 * Note:In general GET caches but is more transparent while POST
				 * does not cache.
				 */
				conn = (HttpURLConnection) url.openConnection();
				conn.setRequestMethod(opts.method);
				conn.setDoOutput(true);
				conn.setUseCaches(opts.useCaches);
				conn.setReadTimeout(opts.timeout);

				/*
				 * Set an appropriate Accept header for the query.
				 */
				final QueryType queryType = opts.queryType = QueryType
						.fromQuery(opts.queryStr);

				switch(queryType) {
				case DESCRIBE:
				case CONSTRUCT:
					conn.setRequestProperty("Accept", MIME_RDF_XML);
					break;
				case ASK:
				case SELECT:
					conn.setRequestProperty("Accept", MIME_SPARQL_RESULTS_XML);
					break;
				default:
					throw new UnsupportedOperationException("QueryType: "
							+ queryType);
				}

				// write out the request headers
				if (log.isDebugEnabled()) {
					log.debug("*** Request ***");
					log.debug(opts.serviceURL);
					log.debug(opts.queryStr);
				}

//				System.out.println("Request Path: " + url);
//				System.out.println("Request Query: " + method.getQueryString());
//				Header[] requestHeaders = method.getRequestHeaders();
//				for (int i = 0; i < requestHeaders.length; i++) {
//					System.out.print(requestHeaders[i]);
//				}
//
//				// execute the method
//				client.executeMethod(method);

				// connect.
				conn.connect();

				final int rc = conn.getResponseCode();
					if(rc < 200 || rc >= 300) {
                    throw new IOException(rc + " : "
                            + conn.getResponseMessage()+" : "+url);
				}

				if (log.isDebugEnabled()) {
					/*
					 * write out the response headers
					 * 
					 * @todo options to show the headers (in/out),
					 */
					log.debug("*** Response ***");
					log.debug("Status Line: " + conn.getResponseMessage());
				}

				if (opts.showResults) {

					// Write the response body onto stdout.
					showResults(conn);
					
					// Note: results not counted!
					opts.nresults = -1L;

				} else {

					/*
					 * Write the #of solutions onto stdout.
					 */
					final long nresults;
					switch (queryType) {
					case DESCRIBE:
					case CONSTRUCT:
						nresults = buildGraph(conn).size();
						break;
					case ASK: // I think that there are some alternative mime types for ask...
					case SELECT:
						nresults = countResults(conn);
						break;
					default:
						throw new UnsupportedOperationException("QueryType: "
								+ queryType);
					}

					opts.nresults = nresults;

				}

				return (Void) null;
				
			} finally {

				opts.elapsedNanos = System.nanoTime() - begin;

				// clean up the connection resources
				// method.releaseConnection();
				if (conn != null)
					conn.disconnect();

			}

		} // call()

		/**
		 * Write the response body on stdout.
		 * 
		 * @param conn
		 *            The connection.
		 *            
		 * @throws Exception
		 */
		protected void showResults(final HttpURLConnection conn)
				throws Exception {

			final LineNumberReader r = new LineNumberReader(
					new InputStreamReader(conn.getInputStream(), conn
							.getContentEncoding() == null ? "ISO-8859-1" : conn
							.getContentEncoding()));
			try {
				String s;
				while ((s = r.readLine()) != null) {
					System.out.println(s);
				}
			} finally {
				r.close();
//				conn.disconnect();
			}

		}
		
	    /**
	     * Counts the #of results in a SPARQL result set.
	     * 
	     * @param conn
	     *            The connection from which to read the results.
	     * 
	     * @return The #of results.
	     * 
	     * @throws Exception
	     *             If anything goes wrong.
	     */
	    protected long countResults(final HttpURLConnection conn) throws Exception {

	        final AtomicLong nsolutions = new AtomicLong();

	        try {

	            final TupleQueryResultParser parser = new SPARQLResultsXMLParserFactory()
	                    .getParser();

	            parser
	                    .setTupleQueryResultHandler(new TupleQueryResultHandlerBase() {
	                        // Indicates the end of a sequence of solutions.
	                        public void endQueryResult() {
	                            // connection close is handled in finally{}
	                        }

	                        // Handles a solution.
	                        public void handleSolution(final BindingSet bset) {
	                            if (log.isDebugEnabled())
	                                log.debug(bset.toString());
	                            nsolutions.incrementAndGet();
	                        }

	                        // Indicates the start of a sequence of Solutions.
	                        public void startQueryResult(List<String> bindingNames) {
	                        }
	                    });

	            parser.parse(conn.getInputStream());

	            if (log.isInfoEnabled())
	                log.info("nsolutions=" + nsolutions);

	            // done.
	            return nsolutions.longValue();

	        } finally {

//	            // terminate the http connection.
//	            conn.disconnect();

	        }

	    } // countResults

	    /**
	     * Builds a graph from an RDF result set (statements, not binding sets).
	     * 
	     * @param conn
	     *            The connection from which to read the results.
	     * 
	     * @return The graph
	     * 
	     * @throws Exception
	     *             If anything goes wrong.
	     */
	    protected Graph buildGraph(final HttpURLConnection conn) throws Exception {

	        final Graph g = new GraphImpl();

	        try {

	            final String baseURI = "";
	            
	            final RDFXMLParser rdfParser = new RDFXMLParser(
	                    new ValueFactoryImpl());

	            rdfParser.setVerifyData(true);
	            
	            rdfParser.setStopAtFirstError(true);
	            
	            rdfParser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);
	            
	            rdfParser.setRDFHandler(new StatementCollector(g));
	            
	            rdfParser.parse(conn.getInputStream(), baseURI);

	            return g;

	        } finally {

//	            // terminate the http connection.
//	            conn.disconnect();

	        }

	    } // buildGraph
	    
	} // class Query

	/**
	 * Read the contents of a file.
	 * <p>
	 * Note: This makes default platform assumptions about the encoding of the
	 * file.
	 * 
	 * @param file
	 *            The file.
	 * 
	 * @return The file's contents.
	 * 
	 * @throws IOException
	 */
	static private String readFromFile(final File file)
			throws IOException {

		if (file.isDirectory())
			throw new IllegalArgumentException();

			final LineNumberReader r = new LineNumberReader(
					new FileReader(file));

			try {

				final StringBuilder sb = new StringBuilder();

				String s;
				while ((s = r.readLine()) != null) {

					if (r.getLineNumber() > 1)
						sb.append("\n");

					sb.append(s);

				}

				return sb.toString();

			} finally {

				r.close();

			}
		
    }

    /**
     * Read from stdin.
     * <p>
     * Note: This makes default platform assumptions about the encoding of the
     * data being read.
     * 
     * @return The data read.
     * 
     * @throws IOException
     */
    static private String readFromStdin() throws IOException {

        final LineNumberReader r = new LineNumberReader(new InputStreamReader(System.in));

        try {

            final StringBuilder sb = new StringBuilder();

            String s;
            while ((s = r.readLine()) != null) {

                if (r.getLineNumber() > 1)
                    sb.append("\n");

                sb.append(s);

            }

            return sb.toString();

        } finally {

            r.close();

        }

    }

	/**
	 * Populate the list with the plain text files (recursive search of a file
	 * or directory).
	 * 
	 * @param fileOrDir
	 *            The file or directory.
	 * @param fileList
	 *            The list to be populated.
	 */
	static private void getFiles(final File fileOrDir,
			final List<File> fileList) {

		if (fileOrDir.isHidden())
			return;

		if (fileOrDir.isDirectory()) {

			final File dir = fileOrDir;

			final File[] files = dir.listFiles();

			for (int i = 0; i < files.length; i++) {

				final File f = files[i];

				// recursion.
				getFiles(f, fileList);

			}

		} else {

			fileList.add(fileOrDir);

		}

    }

	/**
	 * Read queries from each file in the given list.
	 * 
	 * @param fileList
	 *            The list of files.
	 * @param delim
	 *            When non-<code>null</code>, the delimiter between query
	 *            strings within each file. For example, this can match a
	 *            newline if there is one query per line in the file.
	 * 
	 * @return An map from the sources to the queries. When there is more than
	 *         one query per file (delim is non <code>null</code>), the queries
	 *         within each file will be numbered sequentially (origin ONE (1)).
	 * 
	 * @throws IOException
	 */
	static private final Map<String/* src */, String/* query */> readQueries(
			final List<File> fileList, final Pattern delim) throws IOException {

		final Map<String/* src */, String/* query */> map = new LinkedHashMap<String, String>();

		for (File file : fileList) {

			final String s = readFromFile(file);

			if (delim == null) {

				map.put(file.toString(), s);

			} else {

				final String[] a = delim.split(s);

				int i = 1; // Note: Origin ONE (1).

				for (String queryStr : a) {

					if(queryStr.trim().length() == 0) {
						// Skip blank lines.
						continue;
					}
					
//					// FIXME This is ignoring search queries!
//					if (x.contains("#search"))
						map.put(file.toString() + "#" + i, queryStr);
						
					if (log.isDebugEnabled())
						log.debug("Read query: file=" + file + ", index=" + i
								+ ", query=" + queryStr);

					i++;

				}

			}

		}

		return map;

	}

	/**
	 * Helper produces a random sequence of indices in the range [0:n-1]
	 * suitable for visiting the elements of an array of n elements in a random
	 * order. This is useful when you want to randomize the presentation of
	 * elements from two or more arrays. For example, known keys and values can
	 * be generated and their presentation order randomized by indexing with the
	 * returned array.
	 */
	private static int[] getRandomOrder(final long seed, final int n) {

		final Random rnd = new Random(seed);
		
		final class Pair implements Comparable<Pair> {
			public double r = rnd.nextDouble();
			public int val;

			public Pair(int val) {
				this.val = val;
			}

			public int compareTo(final Pair other) {
				if (this == other)
					return 0;
				if (this.r < other.r)
					return -1;
				else
					return 1;
			}

		}

		final Pair[] pairs = new Pair[n];

		for (int i = 0; i < n; i++) {

			pairs[i] = new Pair(i);

		}

		java.util.Arrays.sort(pairs);

		final int order[] = new int[n];

		for (int i = 0; i < n; i++) {

			order[i] = pairs[i].val;

		}

		return order;

	}
    
    /**
	 * Options for the query.
	 */
	public static class QueryOptions implements Cloneable {

		/** The URL of the SPARQL endpoint. */
		public String serviceURL = null;
		public String username = null;
		public String password = null;
		/**
		 * The source for this query (e.g., the file from which it was read)
		 * (optional).
		 */
		public String source;
		/** The SPARQL query. */
		public String queryStr;
		/** The baseURI (optional). */
		public String baseURI;
		/** The default graph URI (optional). */
		public String defaultGraphUri = null;
		/** The connection timeout (ms). */
		public int timeout = DEFAULT_TIMEOUT;
		/**
		 * Either GET or POST. In general, GET is more transparent while POST is
		 * not cached.
		 */
		public String method = "GET";
		/**
		 * When <code>false</code>, the http connection will be directed to
		 * ignore caches.
		 */
		public boolean useCaches = true;
		/** When <code>true</code>, show the original query string. */
		public boolean showQuery = false;
		/** When <code>true</code>, show the parsed operator tree (on the client side). */
		public boolean showParseTree = false;
		/** When <code>true</code>, show the results of the query (on stdout). */
		public boolean showResults = false;
        public boolean verbose = false;
        public boolean quiet = false;
        
        /*
         * Outputs.
         */
        public QueryType queryType = null;
		public long nresults = 0;
		public long elapsedNanos = 0;

		/**
		 * The query is not specified to the constructor must be set explicitly
		 * by the caller.
		 */
		public QueryOptions() {

			this(null/* serviceURL */, null/* queryStr */);

		}

        /**
         * @param serviceURL
         *            The SPARQL end point URL.
         * @param queryStr
         *            The SPARQL query.
         */
        public QueryOptions(final String serviceURL, final String queryStr) {

            this.serviceURL = serviceURL;
            
            this.queryStr = queryStr;

        }
        
        public QueryOptions clone() {
        	
        	try {
				
        		return (QueryOptions) super.clone();

        	} catch (CloneNotSupportedException e) {
        		
				throw new RuntimeException(e);
				
			}
        	
        }
        
	}

	/**
	 * Metadata about a single presentation of a SPARQL query.
	 * 
	 * @author thompsonbry
	 */
	private static class QueryTrial {
	
		private final long elapsedNanos;
		private final long resultCount;
		private final Throwable cause;
		public QueryTrial(final long elapsedTime,final long resultCount) {
			this.elapsedNanos = elapsedTime;
			this.resultCount = resultCount;
			this.cause = null;
		}
		public QueryTrial(final Throwable cause) {
			this.elapsedNanos = -1;
			this.resultCount = -1;
			this.cause = cause;
		}
		
	}

	/**
	 * A SPARQL query together with its {@link Score}s and utility methods to
	 * submit the query, aggregate across its {@link Score}s, and report on the
	 * aggregated query performance.
	 * 
	 * TODO Now that we keep the elapsed time and result count for each query
	 * trial, we can report on min/max/stdev and changes in the #of results
	 * across trials (which indicates a query which is not stable in its result
	 * set size).
	 */
	private static class Query {
	
		/** The source query identifier. */
		public final String source;
		
		/** The query. */
		public final String queryStr;

		/** Metadata about each query presentation. */
		public LinkedBlockingQueue<QueryTrial> trials = new LinkedBlockingQueue<QueryTrial>(/* unbounded */);

		/**
		 * Total elapsed nanoseconds over all {@link QueryTrial}s for this
		 * {@link Query}.
		 */
		public final CAT elapsedNanos = new CAT();
		
		public Query(final String source, final String queryStr) {

			this.source = source;

			this.queryStr = queryStr;

		}

		public QueryTrial runQuery(QueryOptions opts) throws Exception {

			opts = opts.clone();

			opts.queryStr = this.queryStr;

			opts.source = this.source;

			try {

				// Run the query
				new QueryTask(/* client, */opts).call();

				final QueryTrial trial = new QueryTrial(opts.elapsedNanos,
						opts.nresults);

				trials.add(trial);

				elapsedNanos.add(opts.elapsedNanos);

				return trial;

			} catch (Throwable t) {

				trials.add(new QueryTrial(t));

				throw new Exception(t);
				
			}

		}
		
	}

	/**
	 * Class models a aggregated score for a specific query.
	 */
	private static class Score implements Comparable<Score> {

		/** The query. */
		public final Query query;

		/** Total elapsed time (nanos). */
		public final long elapsedNanos;

		public Score(final Query query) {

			this.query = query;

			// average elapsed nanos for this query across all trials.
			this.elapsedNanos = query.elapsedNanos.get() / query.trials.size();

		}

		/**
		 * Order by increasing elapsed time (slowest queries are last).
		 */
		public int compareTo(Score o) {
			if (elapsedNanos < o.elapsedNanos)
				return -1;
			if (elapsedNanos > o.elapsedNanos)
				return 1;
			return 0;
		}
		
	}

	/**
	 * Return the order in which the queries will be evaluated. The order in
	 * which those queries will be executed is determined by the
	 * <code>seed</code>. When non-zero, the queries evaluation order will be
	 * randomized. Otherwise the queries are evaluated in the given order. The
	 * elements of the returned <code>order[]</code> are indices into the
	 * <code>query[]</code>. The length of the returned <code>order[]</code> is
	 * the #of queries given times the <i>repeat</i> count.
	 * 
	 * @param seed
	 *            The random seed -or- ZERO (0L) if the queries will be
	 *            evaluated in the given order.
	 * @param repeat
	 *            The repeat count.
	 * @param nqueries
	 *            The #of queries.
	 * 
	 * @return The evaluation order. The indices in the array are in
	 *         <code>[0:nqueries)</code>. Each index appears <i>repeat</i> times
	 *         in the array.
	 */
	private static int[] getQueryOrder(final long seed, final int repeat,
			final int nqueries) {

		final int[] order;

		// Total #of trials to execute.
		final int ntrials = nqueries * repeat;

		// Determine the query presentation order.
		if (seed == 0) {
		
			// Run queries in the given order.
			order = new int[ntrials];
			
			for (int i = 0; i < ntrials; i++) {
			
				order[i] = i;
				
			}
			
		} else {
			
			// Run queries in a randomized order.
			order = getRandomOrder(seed, ntrials);
		}

		// Now normalize the query index into [0:nqueries).
		for (int i = 0; i < ntrials; i++) {
		
			order[i] = order[i] % nqueries;
			
		}

		return order;

	}
	
//	/**
//	 * Runs the queries in the evaluation order.
//	 * 
//	 * @param order
//	 *            The evaluation order. This is an array of indices into the
//	 *            <i>queries</i> array. Indices for the same query may appear
//	 *            more than once.
//	 * @param queries
//	 *            The queries.
//	 * @param opts
//	 *            The configured query options.
//	 * @param nerrors
//	 *            The #of errors is reported via this variable as a side effect.
//	 */
//	private static void runQueriesSingleThreaded(final int[] order,
//			final Query[] queries, final QueryOptions opts,
//			final AtomicLong nerrors) {
//
//		for (int i = 0; i < order.length; i++) {
//
//			final int queryId = order[i];
//
//			final Query query = queries[queryId];
//
//			new RunQueryTask(query, opts, nerrors).run();
//
//		} // next query in the evaluation order
//
//	}

	/**
	 * Run a query.
	 */
	static class RunQueryTask implements Runnable {

		private final Query query;
		private final QueryOptions opts;
		private final AtomicLong nerrors;

		/**
		 * @param query
		 *            The query.
		 * @param opts
		 *            The configured query options (will be cloned).
		 * @param nerrors
		 *            The #of errors is reported via this variable as a side
		 *            effect.
		 */
		public RunQueryTask(final Query query, final QueryOptions opts,
				final AtomicLong nerrors) {

			if (query == null)
				throw new IllegalArgumentException();

			if (opts == null)
				throw new IllegalArgumentException();

			if (nerrors == null)
				throw new IllegalArgumentException();

			this.query = query;

			this.opts = opts;

			this.nerrors = nerrors;

		}

		public void run() {

			try {

				final QueryTrial trial = query.runQuery(opts);

				if (!opts.quiet) {
					// Show the query run time, #of results, source, etc.
					System.err.println("resultCount="
							+ (trial.resultCount == -1 ? "N/A"
									: trial.resultCount) + ", elapsed="
							+ TimeUnit.NANOSECONDS.toMillis(trial.elapsedNanos)
							+ "ms, source=" + query.source);
				}

			} catch (Throwable t) {

				nerrors.incrementAndGet();

				log.error("nerrors=" + nerrors + ", source=" + query.source
						+ ", query=" + query.queryStr + ", cause=" + t);// , t);

			}

		}
		
	} // RunQueryTask
	
	/**
	 * Return the {@link Score}s for a collection of queries.
	 * 
	 * @param queries
	 *            The queries (after they have been evaluated).
	 *            
	 * @return The {@link Score}s.
	 */
	private static Score[] getScores(final Query[] queries) {

		final Score[] a = new Score[queries.length];

		for (int i = 0; i < queries.length; i++) {

			final Query query = queries[i];

			a[i] = new Score(query);

		}

		return a;
		
	}

	/**
	 * Report the average running time for each query on <code>stdout</code>.
	 * 
	 * @param a
	 *            The query scores.
	 * @param minMillisLatencyToReport
	 *            The minimum latency (in milliseconds) to report.
	 */
	private static void reportScores(final Score[] a,
			final long minMillisLatencyToReport) {

		// Place into order (ascending average query evaluation time).
		Arrays.sort(a);

		System.out.println("average(ms)\tsource\tquery");

		for (int i = 0; i < a.length; i++) {

			final Score s = a[i];

			final long elapsedMillis = TimeUnit.NANOSECONDS
					.toMillis(s.elapsedNanos);

			if (elapsedMillis >= minMillisLatencyToReport)
				System.out.println(elapsedMillis + "\t" + s.query.source + "\t"
						+ s.query.queryStr);

		}

	}

	private static void usage() {

		System.err.println("usage: (option)* [serviceURL] (query)");
		
	}

	/**
	 * Issue a query against a SPARQL endpoint. By default, the client will read
	 * from stdin. It will write on stdout.
	 * 
	 * @param args
	 *            <code>(option)* [serviceURL] (query)</code>
	 *            <p>
	 *            where
	 *            <dl>
	 *            <dt>serviceURL</dt>
	 *            <dd>The URL of the SPARQL endpoint.</dd>
	 *            <dt>query</dt>
	 *            <dd>The SPARQL query (required unless <code>-f</code> is used)
	 *            </dd>
	 *            <p>
	 *            where <i>option</i> is any of
	 *            <dl>
	 *            <dt>-u</dt>
	 *            <dd>username</dd>
	 *            <dt>-p</dt>
	 *            <dd>password</dd>
	 *            <dt>-timeout</dt>
	 *            <dd>The http connection timeout in milliseconds (default
	 *            {@value #DEFAULT_TIMEOUT}) -or- ZERO (0) for an infinite
	 *            timeout.</dd>
	 *            <dt>-method (GET|POST)</dt>
	 *            <dd>The HTTP method for the requests (default GET).</dd>
	 *            <dt>-useCaches (true|false)</dt>
	 *            <dd>Set to <code>false</code> to explicitly disable the use of
	 *            HTTP connection caches along the route to the http endpoint
	 *            (default <code>true</code>).</dd>
	 *            <dt>-showQuery</dt>
	 *            <dd>Show the query string.</dd>
	 *            <dt>-showParseTree</dt>
	 *            <dd>Show the SPARQL parse tree (on the client).</dd>
	 *            <dt>-showResults</dt>
	 *            <dd>Show the query results (on stdout).</dd>
	 *            <dt>-verbose</dt>
	 *            <dd>Be verbose.</dd>
	 *            <dt>-quiet</dt>
	 *            <dd>Be quiet.</dd>
	 *            <dt>-f</dt>
	 *            <dd>A file (or directory) containing the query(s) to be run.
	 *            Each file may contain a single SPARQL query.</dd>
	 *            <dt>-delim</dt>
	 *            <dd>An optional regular expression which delimits query
	 *            strings within each file. For example, this can match a
	 *            newline if there is one query per line in the file. When not
	 *            specified, it is assumed that there is one query per file.</dd>
	 *            <dt>-query</dt>
	 *            <dd>The query follows immediately on the command line (be sure
	 *            to quote the query).</dd>
	 *            <dt>-clients</dt>
	 *            <dd>The #of client threads which will issue queries (default
	 *            ONE (1)).</dd>
	 *            <dt>-repeat #</dt>
	 *            <dd>The #of times to present each query. A seed of ZERO (0)
	 *            will disable the randomized presentation of the queries. The
	 *            default seed is based on the System clock.</dd>
	 *            <dt>-seed seed</dt>
	 *            <dd>Randomize the presentation of the queries, optionally
	 *            using the specified seed for the random number generator.</dd>
	 *            <dt>-defaultGraph</dt>
	 *            <dd>The URI of the default graph to use for the query.</dd>
	 *            <dt>-baseURI</dt>
	 *            <dd>The baseURI of the query (used when parsing the query).</dd>
	 *            <dt>-help</dt>
	 *            <dd>Display help.</dd>
	 *            <dt>--?</dt>
	 *            <dd>Display help.</dd>
	 *            </dl>
	 * @throws Exception
	 * 
	 * @todo username/password not supported.
	 */
	public static void main(final String[] args) throws Exception {

		if (args.length == 0) {
			usage();
			System.exit(1);
		}

		/*
         * Parse the command line, overriding various properties.
         */
		long seed = System.nanoTime(); // Note: 0L means not randomized.
		int repeat = 1; // repeat count.
		long minLatencyToReport = 100; // only queries with at least this much latency are reported.
		File file = null; // When non-null, file or directory containing query(s).
		Pattern delim = null; // When non-null, this delimits queries within a file.
		String queryStr = null; // A query given directly on the command line.
		int nclients = 1; // The #of clients.
		int threadsPerClient = 1; // TODO The #of threads per client IFF groupQueriesBySource is true.
		boolean groupQueriesBySource = false; // TODO When true, each source represents a batch of queries.
		long interGroupDelayMillis = 0L; // Latency by the client between query batches. 
		final QueryOptions opts = new QueryOptions();
        {

            int i = 0;
            for (; i < args.length && args[i].startsWith("-"); i++) {

                final String arg = args[i];

                if (arg.equals("-u")) {

                	opts.username = args[++i];
                    
                } else if (arg.equals("-p")) {

                	opts.password = args[++i];

                } else if (arg.equals("-f")) {

                    file = new File(args[++i]);
                    
//                    opts.queryStr = readFromFile(new File(file));

                } else if (arg.equals("-delim")) {

                    delim = Pattern.compile(args[++i]);

				} else if (arg.equals("-showQuery")) {

					opts.showQuery = true;

				} else if (arg.equals("-showParseTree")) {

					opts.showParseTree = true;

				} else if (arg.equals("-showResults")) {

					opts.showResults = true;

                } else if (arg.equals("-verbose")) {
                    
                    opts.verbose = true;
                    opts.quiet = false;

                } else if (arg.equals("-quiet")) {
                    
                    opts.verbose = false;
                    opts.quiet = true;
                    
                } else if (arg.equals("-query")) {

                    queryStr = args[++i];

        		} else if (arg.equals("-clients")) {

                    if ((nclients = Integer.valueOf(args[++i])) < 1) {

                        throw new IllegalArgumentException("Bad clients.");
                        
				    }
        		
        		} else if (arg.equals("-repeat")) {

                    if ((repeat = Integer.valueOf(args[++i])) < 1) {

                        throw new IllegalArgumentException("Bad repeat.");
                        
				    }

				} else if (arg.equals("-seed")) {

                    seed = Long.valueOf(args[++i]);
                    
				} else if (arg.equals("-method")) {

					opts.method = args[++i].trim();

					if (!"POST".equals(opts.method)
							&& !"GET".equals(opts.method)) {

						throw new IllegalArgumentException("Bad method: "
								+ opts.method);
						
					}

				} else if (arg.equals("-useCaches")) {

					opts.useCaches = Boolean.valueOf(args[i++]);
					
				} else if (arg.equals("-timeout")) {

                    if ((opts.timeout = Integer.valueOf(args[++i])) < 0) {

                        throw new IllegalArgumentException("Bad timeout.");
                        
				    }

                    if (opts.verbose)
                        System.err.println("timeout: "
                                + (opts.timeout == 0 ? "infinite" : (""
                                        + opts.timeout + "ms")));

				} else if (arg.equals("-defaultGraph")) {

					opts.defaultGraphUri = args[++i];

                    if (opts.verbose)
                        System.err.println("defaultGraph: "
                                + opts.defaultGraphUri);

				} else if (arg.equals("-baseURI")) {

					opts.baseURI = args[++i];

					if (opts.verbose)
						System.err.println("baseURI: " + opts.baseURI);

				} else if (arg.equals("-help") || arg.equals("--?")) {

					usage();

					System.exit(1);

				} else {

					throw new UnsupportedOperationException("Unknown option: "
							+ arg);

				}

			} // next arg.

			// The next argument is the serviceURL, which is required.
			if (i < args.length) {
				opts.serviceURL = args[i++];
                if (opts.verbose)
                    System.err.println("serviceURL: " + opts.serviceURL);
			} else {
				usage();
				System.exit(1);
			}

        } // parse command line.

//		// create a singular HttpClient object
//		final HttpClient client = new HttpClient();
//
//		{
//
//			final HttpConnectionManagerParams params = client
//					.getHttpConnectionManager().getParams();
//			
//			// Set timeout until a connection is established (ms).
//			params.setConnectionTimeout(5000/* timeout(ms) */);
//			
//		}
//		
//		// Set default credentials.
//		if (username != null) {
//
//			final Credentials creds = new UsernamePasswordCredentials(username,
//					password);
//
//			client.getState().setCredentials(AuthScope.ANY, creds);
//
//		}

        final Query[] queries;
		if (file != null) {

			/*
			 * Read the query(s) from the file system.
			 */
			
			if (opts.verbose)
				System.err.println("Reading query(s) from file: " + file);
			
			// Figure out which files will be read.
			final List<File> fileList = new LinkedList<File>();

			// Get the list of files to be read.
			getFiles(file, fileList);

			// Read the query(s) from the file or directory.
			final Map<String/* src */, String/* query */> map = readQueries(fileList,
					delim);

			final int nqueries = map.size();
			
			if (!opts.quiet)
				System.err.println("Read " + nqueries + " queries from "
						+ fileList.size() + " sources in " + file);

			queries = new Query[nqueries];

			int i = 0;

			for (Map.Entry<String,String> e : map.entrySet()) {

				queries[i++] = new Query(e.getKey(), e.getValue());
				
			}

		} else {

			/*
			 * Run a single query. Either the query was given as a command line
			 * argument or we will read it from stdin now.
			 */
			
			final String source;
			
			if (queryStr == null) {

				if (opts.verbose)
					System.err.println("Reading query from stdin...");

				queryStr = readFromStdin();
				
				source = "stdin";

			} else {

				source = "command line";

			}

			// An array with just the one query.
			queries = new Query[] { new Query(queryStr, source) };

		}

		/*
		 * Run trials.
		 */
		
		// total elapsed milliseconds for all trials.
		final long beginTrials = System.currentTimeMillis();

		// total #of errors across all query presentations.
		final AtomicLong nerrors = new AtomicLong();
		
		if (nclients == 1 && !groupQueriesBySource) {

			/*
			 * Run the queries in a single thread.
			 */

			System.err.println("Running queries with with a single client");

			final int[] order = getQueryOrder(seed, repeat, queries.length);

			for (int i = 0; i < order.length; i++) {

				final int queryId = order[i];

				final Query query = queries[queryId];

				new RunQueryTask(query, opts, nerrors).run();

			}

		} else if(!groupQueriesBySource) {
	
			/*
			 * Run the queries using N clients.
			 */

			System.err
					.println("Running queries with parallel clients: nclients="
							+ nclients);

			// The evaluation order is used to assign tasks to clients.
			final int[] order = getQueryOrder(seed, repeat, queries.length);

			// The tasks to be run.
			final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();

			for (int i = 0; i < order.length; i++) {

				final RunQueryTask runnable = new RunQueryTask(
						queries[order[i]], opts, nerrors);

				tasks.add(new Callable<Void>() {
					public Void call() throws Exception {
						runnable.run();
						return (Void) null;
					}
				});

			}

			final ExecutorService clientService = Executors
					.newFixedThreadPool(nclients);

			try {

				// Run the tasks.
				clientService.invokeAll(tasks);
				
			} finally {

				clientService.shutdownNow();
				
			}
			
		} else {

			/*
			 * FIXME group queries by source and impose latency between batches.
			 * Randomization is dependent on whether or not the seed was set to
			 * ZERO (0L).
			 */
			throw new UnsupportedOperationException();
			
		}

		/*
		 * Report the average query latency for queries with at least a
		 * specified latency.
		 */
		reportScores(getScores(queries), minLatencyToReport);

		System.out.println("Total elapsed time: "
				+ (System.currentTimeMillis() - beginTrials) + "ms for "
				+ queries.length + " queries with " + repeat
				+ " trials each and " + nclients
				+ " clients.  Reporting only queries with at least "
				+ minLatencyToReport + "ms latency.");

		// Normal exit.
		System.exit(0);

	}

//	/**
//	 * A model of the query workload to be imposed on the SPARQL end point. The
//	 * model allows you to group queries from the same "source" into a batch, to
//	 * specify the latency between queries within a batch, and to specify the
//	 * latency between one batch and the next. You can also specify the number
//	 * of independent clients which will work their way through the available
//	 * queries and the size of the per-client thread pool.
//	 * <p>
//	 * This workload model is sufficient to model the workload of N concurrent
//	 * users operating against a shared SPARQL end point, including applications
//	 * where each user action results in a set of SPARQL queries, such as when
//	 * painting an HTML page.
//	 * <p>
//	 * There are some degenerate cases which are also useful. For example, it is
//	 * easy to specify a workload model N clients run queries in a randomized
//	 * order.
//	 * 
//	 * @author thompsonbry
//	 */
//	static class WorkloadModel implements Callable<Void> {
//
//		private final int nclients;
//		private final int threadsPerClient;
//		private final AtomicLong nerrors = new AtomicLong();
//
//		public WorkloadModel(final int nclients, final int threadsPerClient) {
//
//			if (nclients < 1)
//				throw new IllegalArgumentException();
//
//			if (threadsPerClient < 1)
//				throw new IllegalArgumentException();
//
//			this.nclients = nclients;
//			
//			this.threadsPerClient = threadsPerClient;
//			
//			
//		}
//
//		public Void call() throws Exception {
//			
//			if (nclients == 1 && threadsPerClient == 1) {
//
//				runQueriesSingleThreaded(order, queries, opts, nerrors);
//				
//			}
//			
//		}
//		
//	}
	
}
