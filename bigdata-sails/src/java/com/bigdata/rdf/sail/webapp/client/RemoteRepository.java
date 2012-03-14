package com.bigdata.rdf.sail.webapp.client;

import info.aduna.io.IOUtil;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.openrdf.model.Graph;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandlerBase;
import org.openrdf.query.impl.TupleQueryResultBuilder;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.BooleanQueryResultParser;
import org.openrdf.query.resultio.BooleanQueryResultParserFactory;
import org.openrdf.query.resultio.BooleanQueryResultParserRegistry;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.query.resultio.TupleQueryResultParserFactory;
import org.openrdf.query.resultio.TupleQueryResultParserRegistry;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.RDFWriterRegistry;
import org.openrdf.rio.helpers.StatementCollector;
import org.xml.sax.Attributes;
import org.xml.sax.ext.DefaultHandler2;

import com.bigdata.rdf.sail.webapp.BigdataRDFServlet;
import com.bigdata.rdf.sail.webapp.EncodeDecodeValue;
import com.bigdata.rdf.sail.webapp.MiniMime;

/**
 * Java API to the NanoSparqlServer. See 
 * 
 * <a href="https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=NanoSparqlServer">this page</a> 
 * 
 * for more information on the HTTP API.
 */
public class RemoteRepository {

	private static final transient Logger log = Logger.getLogger(RemoteRepository.class);
	
    /**
     * The name of the <code>UTF-8</code> character encoding.
     */
    static private final String UTF8 = "UTF-8";
    
    private final String serviceURL;

    public RemoteRepository(final String serviceURL) {
    	this.serviceURL = serviceURL;
    }
    

	/**
	 * Prepare a tuple (select) query.
	 * 
	 * @param query
	 * 			the query string
	 * @return
	 * 			the {@link TupleQuery}
	 */
	public TupleQuery prepareTupleQuery(final String query) throws Exception {
		return new TupleQuery(UUID.randomUUID(), query);
	}

	/**
	 * Prepare a graph query.
	 * 
	 * @param query
	 * 			the query string
	 * @return
	 * 			the {@link GraphQuery}
	 */
	public GraphQuery prepareGraphQuery(final String query) throws Exception {
		return new GraphQuery(UUID.randomUUID(), query);
	}

	/**
	 * Prepare a boolean (ask) query.
	 * 
	 * @param query
	 * 			the query string
	 * @return
	 * 			the {@link BooleanQuery}
	 */
	public BooleanQuery prepareBooleanQuery(final String query) throws Exception {
		return new BooleanQuery(UUID.randomUUID(), query);
	}

	/**
	 * Cancel a query running remotely on the server.
	 * 
	 * @param queryID
	 * 			the UUID of the query to cancel
	 */
	public void cancel(final UUID queryId) throws Exception {
		
        final ConnectOptions opts = new ConnectOptions(serviceURL+"/status");

        opts.addRequestParam("queryId", queryId.toString());
        opts.addRequestParam("cancel");

        HttpURLConnection cxn = null;
        try {
        	
        	checkResponseCode(cxn = doConnect(opts));
        	
        } finally {
        	
        	if (cxn != null)
        		cxn.disconnect();
        	
        }
		
	}

	/**
	 * Perform a fast range count on the statement indices for a given
	 * triple (quad) pattern.
	 * 
	 * @param s
	 * 			the subject (can be null)
	 * @param p
	 * 			the predicate (can be null)
	 * @param o
	 * 			the object (can be null)
	 * @param c
	 * 			the context (can be null)
	 * @return
	 * 			the range count
	 */
	public long rangeCount(final URI s, final URI p, final Value o, final URI c) 
			throws Exception {

        final ConnectOptions opts = new ConnectOptions(serviceURL+"/sparql");

        opts.addRequestParam("ESTCARD");
        if (s != null) {
        	opts.addRequestParam("s", EncodeDecodeValue.encodeValue(s));
        }
        if (p != null) {
        	opts.addRequestParam("p", EncodeDecodeValue.encodeValue(p));
        }
        if (o != null) {
        	opts.addRequestParam("o", EncodeDecodeValue.encodeValue(o));
        }
        if (c != null) {
        	opts.addRequestParam("c", EncodeDecodeValue.encodeValue(c));
        }

        HttpURLConnection cxn = null;
        try {
        	
        	checkResponseCode(cxn = doConnect(opts));
        	
        	final RangeCountResult result = rangeCountResults(cxn);
        	
        	return result.rangeCount;
        	
        } finally {
        	
        	if (cxn != null)
        		cxn.disconnect();
        	
        }

	}

	/**
	 * Adds RDF data to the remote repository.
	 * 
	 * @param add
	 *        The RDF data to be added.
	 */
	public long add(AddOp add) throws Exception {
		
        final ConnectOptions opts = new ConnectOptions(serviceURL+"/sparql");
        
        opts.method = "POST";
        
        if (add.uri != null) {
	        // set the resource to load.
	        opts.addRequestParam("uri", add.uri);
        }
        
        if (add.context != null) {
	        // set the default context.
	        opts.addRequestParam("context-uri", add.context);
        }
        
        if (add.format != null) {
        	// set the content type
        	opts.contentType = add.format.getDefaultMIMEType();
        }
        
        if (add.data != null) {
        	// set the data
        	opts.data = add.data;
        }
        
        if (add.file != null) {
        	// set the data
        	opts.data = IOUtil.readBytes(add.file);
        }
        
        if (add.stmts != null) {
        	
        	// set the data and content type (TRIG by default)
        	final RDFFormat format = RDFFormat.TRIG;
		    opts.contentType = add.format.getDefaultMIMEType();
		    opts.data = serialize(add.stmts, format);

        }
        
        HttpURLConnection cxn = null;
        try {
        	
        	checkResponseCode(cxn = doConnect(opts));
        	
        	final MutationResult result = mutationResults(cxn);
        	
        	return result.mutationCount;
        	
        } finally {
        	
        	if (cxn != null)
        		cxn.disconnect();
        	
        }
        
	}
        	
	/**
	 * Removes RDF data from the remote repository.
	 * 
	 * @param remove
	 *        The RDF data to be removed.
	 */
	public long remove(RemoveOp remove) throws Exception {
		
        final ConnectOptions opts = new ConnectOptions(serviceURL+"/sparql");
        
        if (remove.format == null && remove.stmts == null) {
        	
            opts.method = "DELETE";
            
        } else {
        	
        	opts.method = "POST";
        	opts.addRequestParam("delete");
        	
        }
        
        if (remove.query != null) {
	        // set the resource to load.
	        opts.addRequestParam("query", remove.query);
        }
        
        if (remove.s != null) {
        	opts.addRequestParam("s", EncodeDecodeValue.encodeValue(remove.s));
        }
        
        if (remove.p != null) {
        	opts.addRequestParam("p", EncodeDecodeValue.encodeValue(remove.p));
        }
        
        if (remove.o != null) {
        	opts.addRequestParam("o", EncodeDecodeValue.encodeValue(remove.o));
        }
        
        if (remove.c != null) {
        	opts.addRequestParam("c", EncodeDecodeValue.encodeValue(remove.c));
        }
        
        if (remove.format != null) {
        	// set the content type
        	opts.contentType = remove.format.getDefaultMIMEType();
        }
        
        if (remove.data != null) {
        	// set the data
        	opts.data = remove.data;
        }
        
        if (remove.file != null) {
        	// set the data
        	opts.data = IOUtil.readBytes(remove.file);
        }
        
        if (remove.stmts != null) {
        	
        	// set the data and content type (TRIG by default)
        	final RDFFormat format = RDFFormat.TRIG;
		    opts.contentType = remove.format.getDefaultMIMEType();
		    opts.data = serialize(remove.stmts, format);

        }
        
        HttpURLConnection cxn = null;
        try {
        	
        	checkResponseCode(cxn = doConnect(opts));
        	
        	final MutationResult result = mutationResults(cxn);
        	
        	return result.mutationCount;
        	
        } finally {
        	
        	if (cxn != null)
        		cxn.disconnect();
        	
        }
		
	}

	/**
	 * Perform an ACID update (delete+insert) per the semantics of
	 * <a href="https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=NanoSparqlServer#UPDATE_.28DELETE_.2B_INSERT.29">
	 * the NanoSparqlServer.
	 * </a>
	 * <p> 
	 * Currently, the only combination supported is delete by query with add
	 * by post (Iterable<Statement> and File). You can embed statements you
	 * want to delete inside a construct query without a where clause.
	 * 
	 * TODO write utility to generate construct query without a where clause
	 * 		from an iteration of statements
	 * 
	 * @param remove
	 *        The RDF data to be removed.
	 * @param add
	 * 		  The RDF data to be added.        
	 */
	public long update(final RemoveOp remove, final AddOp add) throws Exception {
		
        final ConnectOptions opts = new ConnectOptions(serviceURL+"/sparql");
        
        opts.method = "PUT";
        
        if (remove.query != null) {
	        // set the resource to load.
	        opts.addRequestParam("query", remove.query);
        }
        
        if (remove.stmts != null || remove.format != null) {
        	
        	/*
        	 * Need to implement reverse serialization from statements into a 
        	 * construct query string like this:
        	 * 
        	 * prefix bd: <http://bigdata.com/rdf#>
             * construct { bd:Mike bd:likes bd:RDF } {}
             * 
        	 */
        	throw new UnsupportedOperationException();
        	
        }
        
        if (remove.s != null || remove.p != null || remove.o != null || remove.c != null) {
        	
        	throw new UnsupportedOperationException();
        	
        }
        
        if (add.uri != null) {
        	
        	throw new UnsupportedOperationException();
        	
        }
        
        if (add.context != null) {
	        // set the default context.
	        opts.addRequestParam("context-uri", add.context);
        }
        
        if (add.format != null) {
        	// set the content type
        	opts.contentType = add.format.getDefaultMIMEType();
        }
        
        if (add.data != null) {
        	// set the data
        	opts.data = add.data;
        }
        
        if (add.file != null) {
        	// set the data
        	opts.data = IOUtil.readBytes(add.file);
        }
        
        if (add.stmts != null) {
        	
        	// set the data and content type (TRIG by default)
        	final RDFFormat format = RDFFormat.TRIG;
		    opts.contentType = add.format.getDefaultMIMEType();
		    opts.data = serialize(add.stmts, format);

        }
        
        HttpURLConnection cxn = null;
        try {
        	
        	checkResponseCode(cxn = doConnect(opts));
        	
        	final MutationResult result = mutationResults(cxn);
        	
        	return result.mutationCount;
        	
        } finally {
        	
        	if (cxn != null)
        		cxn.disconnect();
        	
        }
		
	}
	
	/**
	 * A prepared query will hold metadata for a particular query instance.
	 * <p>
	 * Right now, the only metadata is the query ID.
	 */
	private abstract class Query {
		
		protected final UUID id;
		
		protected final String query;
		
		public Query(final UUID id, final String query) {
			this.id = id;
			this.query = query;
		}
		
		public UUID getQueryId() {
			return id;
		}
		
		protected HttpURLConnection doRemoteQuery() throws Exception {
			
	        final ConnectOptions opts = new ConnectOptions(serviceURL+"/sparql");

	        opts.method = "GET";
	        opts.addRequestParam("query", query);
	        opts.addRequestParam("queryId", getQueryId().toString());

	        HttpURLConnection cxn = null;
	        try {
	        	
	        	return checkResponseCode(cxn = doConnect(opts));
	        	
	        } catch (Exception ex) {
	        	
	        	if (cxn != null)
	        		cxn.disconnect();
	        	
	        	throw ex;
	        	
	        }
			
		}
		
	}
	
	public final class TupleQuery extends Query {
		
		public TupleQuery(final UUID id, final String query) {
			super(id, query);
		}
		
		public TupleQueryResult evaluate() throws Exception {
            return tupleResults(doRemoteQuery());
		}
		
	}

	public final class GraphQuery extends Query {
		
		public GraphQuery(final UUID id, final String query) {
			super(id, query);
		}
		
		public Graph evaluate() throws Exception {
            return graphResults(doRemoteQuery());
		}
		
	}

	public final class BooleanQuery extends Query {
		
		public BooleanQuery(final UUID id, final String query) {
			super(id, query);
		}
		
		public boolean evaluate() throws Exception {
            return booleanResults(doRemoteQuery());
		}
		
	}

	/**
	 * Add by URI, statements, or file.
	 */
	public static class AddOp {

		private final String uri;
		private final Iterable<Statement> stmts;
		private final byte[] data;
		private final File file;
		private final RDFFormat format;
		
		private String context;
		
		public AddOp(final String uri) {
			this.uri = uri;
			this.stmts = null;
			this.data = null;
			this.file = null;
			this.format = null;
		}
		
		public AddOp(final Iterable<Statement> stmts) {
			this.uri = null;
			this.stmts = stmts;
			this.data = null;
			this.file = null;
			this.format = null;
		}
		
		public AddOp(final File file, final RDFFormat format) {
			this.uri = null;
			this.stmts = null;
			this.data = null;
			this.file = file;
			this.format = format;
		}
		
		/**
		 * This ctor is for the test cases.
		 */
		public AddOp(final byte[] data, final RDFFormat format) {
			this.uri = null;
			this.stmts = null;
			this.data = data;
			this.file = null;
			this.format = format;
		}
		
		public void setContext(final String context) {
			this.context = context;
		}
		
	}
	
	/**
	 * Remove by query, access path, statements, or file.
	 */
	public static class RemoveOp {
		
		private final String query;
		
		private final Iterable<Statement> stmts;
		
		private final Value s, p, o, c;
		
		private final byte[] data;
		
		private final File file;
		
		private final RDFFormat format;
		
		public RemoveOp(final String query) {
			this.query = query;
			this.stmts = null;
			this.s = this.p = this.o = this.c = null;
			this.data = null;
			this.file = null;
			this.format = null;
		}
		
		public RemoveOp(final Iterable<Statement> stmts) {
			this.query = null;
			this.stmts = stmts;
			this.s = this.p = this.o = this.c = null;
			this.data = null;
			this.file = null;
			this.format = null;
		}
		
		public RemoveOp(final URI s, final URI p, final Value o, final URI c) {
			this.query = null;
			this.stmts = null;
			this.s = s;
			this.p = p;
			this.o = o;
			this.c = c;
			this.data = null;
			this.file = null;
			this.format = null;
		}

		public RemoveOp(final File file, final RDFFormat format) {
			this.query = null;
			this.stmts = null;
			this.s = this.p = this.o = this.c = null;
			this.data = null;
			this.file = file;
			this.format = format;
		}
		
		/**
		 * This ctor is for the test cases.
		 */
		public RemoveOp(final byte[] data, final RDFFormat format) {
			this.query = null;
			this.stmts = null;
			this.s = this.p = this.o = this.c = null;
			this.data = data;
			this.file = null;
			this.format = format;
		}
		
	}
	
    /**
     * Options for the HTTP connection.
     */
    private static class ConnectOptions {

        /** The URL of the SPARQL end point. */
        public String serviceURL = null;
        
        /** The HTTP method (GET, POST, etc). */
        public String method = "GET";

		/** The accept header. */
        public String acceptHeader = //
	        BigdataRDFServlet.MIME_SPARQL_RESULTS_XML + ";q=1" + //
	        "," + //
	        RDFFormat.RDFXML.getDefaultMIMEType() + ";q=1"//
	        ;
        
        /** Request parameters to be formatted as URL query parameters. */
        public Map<String,String[]> requestParams;
        
        /**
         * The Content-Type (iff there will be a request body).
         */
        public String contentType = null;
        
        /**
         * The data to send as the request body (optional).
         */
        public byte[] data = null;
        
        /** The connection timeout (ms) -or- ZERO (0) for an infinite timeout. */
        public int timeout = 0;

        public ConnectOptions(final String serviceURL) {
        
            this.serviceURL = serviceURL;
            
        }
        
        public void addRequestParam(final String name, final String[] vals) {
        	
        	if (requestParams == null) {
        	
        		this.requestParams = new LinkedHashMap<String, String[]>();
        		
        	}
        	
        	requestParams.put(name, vals);
        	
        }
        
        public void addRequestParam(final String name, final String val) {
        	
        	addRequestParam(name, new String[] { val });
        	
        }
        
        public void addRequestParam(final String name) {
        	
        	addRequestParam(name, (String[]) null);
        	
        }
        
        public String getRequestParam(final String name) {
        	
        	return requestParams != null ? requestParams.get(name)[0] : null;
        	
        }
        
    }

    protected static String getResponseBody(final HttpURLConnection conn)
			throws IOException {

		final Reader r = new InputStreamReader(conn.getInputStream());

		try {

			final StringWriter w = new StringWriter();

			int ch;
			while ((ch = r.read()) != -1) {

				w.append((char) ch);

			}

			return w.toString();

		} finally {

			r.close();

		}

	}

    /**
     * Connect to a SPARQL end point (GET or POST query only).
     * 
     * @param opts
     *            The query request.
     * @param requestPath
     *            The request path, including the leading "/".
     * 
     * @return The connection.
     */
    protected HttpURLConnection doConnect(final ConnectOptions opts)
            throws Exception {

        /*
         * Generate the fully formed and encoded URL.
         */

        final StringBuilder urlString = new StringBuilder(opts.serviceURL);

        addQueryParams(urlString, opts.requestParams);

        if (log.isDebugEnabled()) {
            log.debug("*** Request ***");
            log.debug(serviceURL);
            log.debug(opts.method);
            log.debug("query="+opts.getRequestParam("query"));
        }

        HttpURLConnection conn = null;
        try {

            // conn = doConnect(urlString.toString(), opts.method);
            final URL url = new URL(urlString.toString());
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(opts.method);
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setUseCaches(false);
            conn.setReadTimeout(opts.timeout);
            conn.setRequestProperty("Accept", opts.acceptHeader);
            if (log.isDebugEnabled())
                log.debug("Accept: " + opts.acceptHeader);
            
            if (opts.contentType != null) {

                if (opts.data == null)
                    throw new AssertionError();

                final String contentLength = Integer.toString(opts.data.length);
                
                conn.setRequestProperty("Content-Type", opts.contentType);

                conn.setRequestProperty("Content-Length", contentLength);

                if (log.isDebugEnabled()) {
                    log.debug("Content-Type: " + opts.contentType);
                    log.debug("Content-Length: " + contentLength);
                }

                final OutputStream os = conn.getOutputStream();
                try {
                    os.write(opts.data);
                    os.flush();
                } finally {
                    os.close();
                }

            }

            // connect.
            conn.connect();

            return conn;

        } catch (Throwable t) {
            /*
             * If something goes wrong, then close the http connection.
             * Otherwise, the connection will be closed by the caller.
             */
            try {
                // clean up the connection resources
                if (conn != null)
                    conn.disconnect();
            } catch (Throwable t2) {
                // ignored.
            }
            throw new RuntimeException(toString() + " : " + t, t);
        }

    }

    protected HttpURLConnection checkResponseCode(final HttpURLConnection conn)
            throws IOException {
        final int rc = conn.getResponseCode();
        if (rc < 200 || rc >= 300) {
            // conn.disconnect();
            throw new IOException("Status Code=" + rc + ", Status Line="
                    + conn.getResponseMessage() + ", Response="
                    + getResponseBody(conn));
        }

        if (log.isDebugEnabled()) {
            /*
             * write out the status list, headers, etc.
             */
            log.debug("*** Response ***");
            log.debug("Status Line: " + conn.getResponseMessage());
        }
        return conn;
    }

    /**
     * Add any URL query parameters.
     */
    private void addQueryParams(final StringBuilder urlString,
            final Map<String, String[]> requestParams)
            throws UnsupportedEncodingException {
    	if (requestParams == null)
    		return;
        boolean first = true;
        for (Map.Entry<String, String[]> e : requestParams.entrySet()) {
            urlString.append(first ? "?" : "&");
            first = false;
            final String name = e.getKey();
            final String[] vals = e.getValue();
            if (vals == null) {
                urlString.append(URLEncoder.encode(name, UTF8));
            } else {
                for (String val : vals) {
                    urlString.append(URLEncoder.encode(name, UTF8));
                    urlString.append("=");
                    urlString.append(URLEncoder.encode(val, UTF8));
                }
            }
        } // next Map.Entry

    }

    /**
     * Extracts the solutions from a SPARQL query.
     * 
     * @param conn
     *            The connection from which to read the results.
     * 
     * @return The results.
     * 
     * @throws Exception
     *             If anything goes wrong.
     */
    protected TupleQueryResult tupleResults(final HttpURLConnection conn)
            throws Exception {

    	try {
    		
	        final String contentType = conn.getContentType();
	
	        final MiniMime mimeType = new MiniMime(contentType);
	        
	        final TupleQueryResultFormat format = TupleQueryResultFormat
	                .forMIMEType(mimeType.getMimeType());
	
	        if (format == null)
	            throw new IOException(
	                    "Could not identify format for service response: serviceURI="
	                            + serviceURL + ", contentType=" + contentType
	                            + " : response=" + getResponseBody(conn));
	
	        final TupleQueryResultParserFactory parserFactory = TupleQueryResultParserRegistry
	                .getInstance().get(format);
	
	        final TupleQueryResultParser parser = parserFactory.getParser();
	
	        final TupleQueryResultBuilder handler = new TupleQueryResultBuilder();
	
	        parser.setTupleQueryResultHandler(handler);
	
	        parser.parse(conn.getInputStream());
	
	        // done.
	        return handler.getQueryResult();
	        
    	} finally {
    		
			// terminate the http connection.
    		conn.disconnect();
    		
    	}

    }
    
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
	protected Graph graphResults(final HttpURLConnection conn) throws Exception {

		try {

			final String baseURI = "";

			final String contentType = conn.getContentType();

            if (contentType == null)
                throw new RuntimeException("Not found: Content-Type");
            
            final MiniMime mimeType = new MiniMime(contentType);
            
            final RDFFormat format = RDFFormat
                    .forMIMEType(mimeType.getMimeType());

            if (format == null)
                throw new IOException(
                        "Could not identify format for service response: serviceURI="
                                + serviceURL + ", contentType=" + contentType
                                + " : response=" + getResponseBody(conn));

			final RDFParserFactory factory = RDFParserRegistry.getInstance().get(format);

            if (factory == null)
                throw new RuntimeException("RDFParserFactory not found: Content-Type=" + contentType
                        + ", format=" + format);
            
            final Graph g = new GraphImpl();

			final RDFParser rdfParser = factory.getParser();
			
			rdfParser.setValueFactory(new ValueFactoryImpl());

			rdfParser.setVerifyData(true);

			rdfParser.setStopAtFirstError(true);

			rdfParser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

			rdfParser.setRDFHandler(new StatementCollector(g));

			rdfParser.parse(conn.getInputStream(), baseURI);

//			return new GraphQueryResultImpl(Collections.EMPTY_MAP, g);
			return g;

		} finally {

			// terminate the http connection.
			conn.disconnect();

		}

	}

    /**
     * Parse a SPARQL result set for an ASK query.
     * 
     * @param conn
     *            The connection from which to read the results.
     * 
     * @return <code>true</code> or <code>false</code> depending on what was
     *         encoded in the SPARQL result set.
     * 
     * @throws Exception
     *             If anything goes wrong, including if the result set does not
     *             encode a single boolean value.
     */
    protected boolean booleanResults(final HttpURLConnection conn) throws Exception {

        try {

            final String contentType = conn.getContentType();

            final MiniMime mimeType = new MiniMime(contentType);
            
            final BooleanQueryResultFormat format = BooleanQueryResultFormat
                    .forMIMEType(mimeType.getMimeType());

            if (format == null)
                throw new IOException(
                        "Could not identify format for service response: serviceURI="
                                + serviceURL + ", contentType=" + contentType
                                + " : response=" + getResponseBody(conn));

            final BooleanQueryResultParserFactory factory = BooleanQueryResultParserRegistry
                    .getInstance().get(format);

            if (factory == null)
                throw new RuntimeException("No factory for Content-Type: " + contentType);

            final BooleanQueryResultParser parser = factory.getParser();

            final boolean result = parser.parse(conn.getInputStream());
            
            return result;

        } finally {

            // terminate the http connection.
            conn.disconnect();

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

        try {

            final String contentType = conn.getContentType();

            final MiniMime mimeType = new MiniMime(contentType);
            
            final TupleQueryResultFormat format = TupleQueryResultFormat
                    .forMIMEType(mimeType.getMimeType());

            if (format == null)
                throw new IOException(
                        "Could not identify format for service response: serviceURI="
                                + serviceURL + ", contentType=" + contentType
                                + " : response=" + getResponseBody(conn));

            final TupleQueryResultParserFactory factory = TupleQueryResultParserRegistry
                    .getInstance().get(format);

            if (factory == null)
                throw new RuntimeException("No factory for Content-Type: " + contentType);

			final TupleQueryResultParser parser = factory.getParser();

	        final AtomicLong nsolutions = new AtomicLong();

			parser.setTupleQueryResultHandler(new TupleQueryResultHandlerBase() {
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

			// terminate the http connection.
			conn.disconnect();

		}

	}

    /**
     * Class representing the result of a mutation operation against the REST
     * API.
     * 
     * TODO Refactor into the non-test code base along with the XML generation
     * and XML parsing?
     */
    private static class MutationResult {

        /** The mutation count. */
        public final long mutationCount;

        /** The elapsed time for the operation. */
        public final long elapsedMillis;

        public MutationResult(final long mutationCount, final long elapsedMillis) {
            this.mutationCount = mutationCount;
            this.elapsedMillis = elapsedMillis;
        }

    }

    protected MutationResult mutationResults(final HttpURLConnection conn) throws Exception {

        try {

            final String contentType = conn.getContentType();

            if (!contentType.startsWith(BigdataRDFServlet.MIME_APPLICATION_XML)) {

                throw new RuntimeException("Expecting Content-Type of "
                        + BigdataRDFServlet.MIME_APPLICATION_XML + ", not "
                        + contentType);

            }

            final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
            
            final AtomicLong mutationCount = new AtomicLong();
            final AtomicLong elapsedMillis = new AtomicLong();

            /*
             * For example: <data modified="5" milliseconds="112"/>
             */
            parser.parse(conn.getInputStream(), new DefaultHandler2(){

                public void startElement(final String uri,
                        final String localName, final String qName,
                        final Attributes attributes) {

                    if (!"data".equals(qName))
                        throw new RuntimeException("Expecting: 'data', but have: uri=" + uri
                                + ", localName=" + localName + ", qName="
                                + qName);

                    mutationCount.set(Long.valueOf(attributes
                            .getValue("modified")));

                    elapsedMillis.set(Long.valueOf(attributes
                            .getValue("milliseconds")));
                           
                }
                
            });
            
            // done.
            return new MutationResult(mutationCount.get(), elapsedMillis.get());

        } finally {

            // terminate the http connection.
            conn.disconnect();

        }

    }

    /**
     * Class representing the result of a fast range count operation against 
     * the REST API.
     * 
     * TODO Refactor into the non-test code base along with the XML generation
     * and XML parsing?
     */
    private static class RangeCountResult {

        /** The range count. */
        public final long rangeCount;

        /** The elapsed time for the operation. */
        public final long elapsedMillis;

        public RangeCountResult(final long rangeCount, final long elapsedMillis) {
            this.rangeCount = rangeCount;
            this.elapsedMillis = elapsedMillis;
        }

    }

    protected RangeCountResult rangeCountResults(final HttpURLConnection conn) throws Exception {

        try {

            final String contentType = conn.getContentType();

            if (!contentType.startsWith(BigdataRDFServlet.MIME_APPLICATION_XML)) {

                throw new RuntimeException("Expecting Content-Type of "
                        + BigdataRDFServlet.MIME_APPLICATION_XML + ", not "
                        + contentType);

            }

            final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
            
            final AtomicLong rangeCount = new AtomicLong();
            final AtomicLong elapsedMillis = new AtomicLong();

            /*
             * For example: <data rangeCount="5" milliseconds="112"/>
             */
            parser.parse(conn.getInputStream(), new DefaultHandler2(){

                public void startElement(final String uri,
                        final String localName, final String qName,
                        final Attributes attributes) {

                    if (!"data".equals(qName))
                        throw new RuntimeException("Expecting: 'data', but have: uri=" + uri
                                + ", localName=" + localName + ", qName="
                                + qName);

                    rangeCount.set(Long.valueOf(attributes
                            .getValue("rangeCount")));

                    elapsedMillis.set(Long.valueOf(attributes
                            .getValue("milliseconds")));
                           
                }
                
            });
            
            // done.
            return new RangeCountResult(rangeCount.get(), elapsedMillis.get());

        } finally {

            // terminate the http connection.
            conn.disconnect();

        }

    }
	
	/**
	 * Serialize an iteration of statements into a byte[] to send across the
	 * wire.
	 */
	protected static byte[] serialize(
			final Iterable<Statement> stmts, final RDFFormat format) 
				throws Exception {
        
		final RDFWriterFactory writerFactory = 
        	RDFWriterRegistry.getInstance().get(format);
	    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
	    final RDFWriter writer = writerFactory.getWriter(baos);
	    writer.startRDF();
	    for (Statement stmt : stmts) {
	        writer.handleStatement(stmt);
	    }
	    writer.endRDF();
	    final byte[] data = baos.toByteArray();
	    
	    return data;

	}

}
