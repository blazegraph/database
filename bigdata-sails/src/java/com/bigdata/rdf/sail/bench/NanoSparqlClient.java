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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;

/**
 * A flyweight utility for issuing queries to an http SPARQL endpoint.
 * 
 * @author thompsonbry@users.sourceforge.net
 */
public class NanoSparqlClient {

	protected static final Logger log = Logger.getLogger(NanoSparqlClient.class);
	
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
	static private final int DEFAULT_TIMEOUT = 5000;

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
	 * Class submits a SPARQL query using httpd and writes the result on stdout.
	 */
	static public class Query implements Callable<Long> {

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

		public Long call() throws Exception {

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
			
//			// Note:In general GET caches but is more transparent while POST
//			// does not cache. @todo disable caching ?
//			final HttpMethod method = new GetMethod(url);
			final URL url = new URL(urlString);
			HttpURLConnection conn = null;
			try {

				/*
				 * @todo review connection properties.
				 */
				conn = (HttpURLConnection) url.openConnection();
				conn.setRequestMethod("GET");
				conn.setDoOutput(true);
				conn.setUseCaches(false);
				conn.setReadTimeout(opts.timeout);

				/*
				 * Set an appropriate Accept header for the query.
				 */
				final QueryType queryType = QueryType.fromQuery(opts.queryStr);
				
				switch(queryType) {
				case DESCRIBE:
				case CONSTRUCT:
					conn.setRequestProperty("Accept", MIME_RDF_XML);
					break;
				case SELECT:
					conn.setRequestProperty("Accept", MIME_SPARQL_RESULTS_XML);
					break;
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
					throw new IOException(conn.getResponseMessage());
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

				/*
				 * Write out the response body
				 * 
				 * @todo option to write the results, count the results, etc.
				 */
				{

					final LineNumberReader r = new LineNumberReader(
							new InputStreamReader(
									conn.getInputStream(),
									conn.getContentEncoding() == null ? "ISO-8859-1"
											: conn.getContentEncoding()));
					try {
						String s;
						while ((s = r.readLine()) != null) {
							System.out.println(s);
						}
					} finally {
						r.close();
					}

				}

				final long elapsed = System.nanoTime() - begin;

				return Long.valueOf(elapsed);

			} finally {

				// clean up the connection resources
				// method.releaseConnection();
				if (conn != null)
					conn.disconnect();

			}

		}

	}

    /**
     * Read the contents of a file.
     * <p>
     * Note: This makes default platform assumptions about the encoding of the
     * file.
     * 
     * @param file
     *            The file.
     * @return The file's contents.
     * 
     * @throws IOException
     */
    static private String readFromFile(final File file) throws IOException {

        final LineNumberReader r = new LineNumberReader(new FileReader(file));

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
		public boolean quiet = false;

	}

	private static void usage() {

		System.err.println("usage: (option)* [serviceURL] (query)");
		
	}

	/**
	 * Issue a query against a SPARQL endpoint.  By default, the client will
	 * read from stdin.  It will write on stdout.
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
	 *            <dd>The connection timeout in milliseconds (default
	 *            {@value #DEFAULT_TIMEOUT}).</dd>
     *            <dt>-show</dt>
     *            <dd>Show the parser query (operator tree).</dd>
     *            <dt>-quiet</dt>
     *            <dd>Run quietly.</dd>
     *            <dt>-f</dt>
     *            <dd>A file containing the query.</dd>
     *            <dt>-q</dt>
     *            <dd>The query follows immediately on the command line (be sure to quote the query).</dd>
     *            <dt>-t</dt>
     *            <dd>The http connection timeout in milliseconds.</dd>
	 *            <dt>-defaultGraph</dt>
	 *            <dd>The URI of the default graph to use for the query.</dd>
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

                    opts.queryStr = readFromFile(new File(args[++i]));

                } else if (arg.equals("-quiet")) {
                    
                    opts.quiet = true;
                    
                } else if (arg.equals("-q")) {

                    opts.queryStr = args[++i];

				} else if (arg.equals("-defaultGraph")) {

					opts.defaultGraphUri = args[++i];

				} else if (arg.equals("-show")) {

					opts.showQuery = true;
					
				} else if (arg.equals("-t")) {

					opts.timeout = Integer.valueOf(args[++i]);

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
			} else {
				usage();
				System.exit(1);
			}

			if (opts.queryStr == null) {

			    readFromStdin();
			    
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

		// Run the query, writes on stdout.
        final long elapsed = new Query(/* client, */opts).call();

        if (!opts.quiet) {
            // Show the query run time.
            System.err.println("elapsed="
                    + TimeUnit.NANOSECONDS.toMillis(elapsed) + "ms");
        }
		
		// Normal exit.
		System.exit(0);

	}

}
