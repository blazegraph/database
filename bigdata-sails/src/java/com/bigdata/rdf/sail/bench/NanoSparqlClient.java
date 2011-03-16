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
	static public class Query implements Callable<Void> {

//		private final HttpClient client;
		final QueryOptions opts;

		/**
		 * 
		 * @param opts The query options.
		 */
		public Query(/* HttpClient client, */ final QueryOptions opts) {

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
				System.err.println("---- Query "
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
					case ASK:
						// TODO parse the response.
						nresults = 1L;
						break;
					case DESCRIBE:
					case CONSTRUCT:
						nresults = buildGraph(conn).size();
						break;
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

				for (String x : a) {

					// FIXME This is ignoring search queries!
					if (!x.contains("#search"))
						map.put(file.toString() + "#" + i, x);

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
	public static class QueryOptions {

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

        final String[] queries; // The query(s) to run.
        final String[] sources; // The source for each query (stdin|filename).
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
						+ file);

			queries = new String[nqueries];
			sources = new String[nqueries];

			int i = 0;

			for (Map.Entry<String,String> e : map.entrySet()) {

				sources[i] = e.getKey();
				
				queries[i] = e.getValue();
				
				i++;
				
			}

		} else {

			/*
			 * Run a single query. Either the query was given as a command line
			 * argument or we will read it from stdin now.
			 */
			
			if (queryStr == null) {

				if (opts.verbose)
					System.err.println("Reading query from stdin...");

				queryStr = readFromStdin();
				
				sources = new String[] { "stdin" };

			} else {

				sources = new String[] { "command line" };

			}

			// An array with just the one query.
			queries = new String[] { queryStr };
			
		}

		/*
		 * The order in which those queries will be executed. The elements of
		 * this array are indices into the query[]. The length of the array is
		 * the #of queries given times the repeat count.
		 */
		final int[] order;
		{
			// Total #of trials to execute.
			final int ntrials = queries.length * repeat;

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
				order[i] = order[i] % queries.length;
			}
		}

		/*
		 * Run trials.
		 * 
		 * @todo option for concurrent clients (need N copies of opts).
		 * 
		 * @todo keep elapsed time for each presentation and result count so we
		 * can report on min/max/stdev and changes in the #of results across
		 * trials (which indicates a query which is not stable in its result set
		 * size). This can be indexed by the trial# for a query. Each client
		 * should be assigned a mixture from the pool, including a trial# for
		 * each query.
		 */

		//		System.err.println("order="+Arrays.toString(order));
		
		// total elapsed milliseconds for all trials.
		final long beginTrials = System.currentTimeMillis();
		
		// cumulative elapsed nanos for each presentation of each query
		final long[] elapsedTimes = new long[queries.length];
		
		long nerrors = 0;
		
		for (int i = 0; i < order.length; i++) {

			final int queryId = order[i];

			opts.queryStr = queries[queryId];

			final String source = opts.source = sources[queryId];

			try {

				// Run the query
				new Query(/* client, */opts).call();

				elapsedTimes[queryId] += opts.elapsedNanos;

				if (!opts.quiet) {
					// Show the query run time, #of results, source, etc.
					System.err.println("queryId=" + queryId + ", resultCount="
							+ opts.nresults + ", elapsed="
							+ TimeUnit.NANOSECONDS.toMillis(opts.elapsedNanos)
							+ "ms" + ", source=" + source);
				}

			} catch (Throwable t) {

				nerrors++;
				
				log.error("nerrors=" + nerrors + ", source=" + source
						+ ", cause=" + t, t);
				
			}

		}

		{
			
			/*
			 * Report the average running time for each query.
			 */

			final Score[] a = new Score[queries.length];
			
			for (int i = 0; i < queries.length; i++) {

				final long elapsedNanos = elapsedTimes[i] / repeat;

				a[i] = new Score(sources[i], queries[i], elapsedNanos);
				
			}
			
			// Place into order
			Arrays.sort(a);

			System.out.println("average(ms), source");

			for (int i = 0; i < a.length; i++) {

				final Score s = a[i];

				final long elapsedMillis = TimeUnit.NANOSECONDS
						.toMillis(s.elapsedNanos);

				if (elapsedMillis >= minLatencyToReport)
					System.out.println(elapsedMillis + ", " + s.source);

			}

		}

		System.out.println("Total elapsed time: "
				+ (System.currentTimeMillis() - beginTrials) + "ms for "
				+ queries.length + " queries with " + repeat
				+ " trials each.  Reporting only queries with at least "
				+ minLatencyToReport + "ms latency.");
		
		// Normal exit.
		System.exit(0);

	}

	/**
	 * Class models scores for a specific query.
	 */
	private static class Score implements Comparable<Score> {
		
		/** The source query identifier. */
		public final String source;
		
		/** The query. */
		public final String queryStr;
		
		/** Total elapsed time (nanos). */
		public final long elapsedNanos;
		
		public Score(final String source, final String queryStr, final long elapsedNanos) {

			this.source = source;
			
			this.queryStr = queryStr;
			
			this.elapsedNanos = elapsedNanos;
			
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
	
}
