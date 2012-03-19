package com.bigdata.rdf.sail.webapp.client;

import info.aduna.io.IOUtil;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.mime.FormBodyPart;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.ByteArrayBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.util.EntityUtils;
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

	private final DefaultHttpClient http;
	
    public RemoteRepository(final String serviceURL) {
    	this.serviceURL = serviceURL;
    	
    	final SchemeRegistry schemeRegistry = new SchemeRegistry();
    	schemeRegistry.register(
    	         new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
    	schemeRegistry.register(
    	         new Scheme("https", 443, SSLSocketFactory.getSocketFactory()));

    	final ThreadSafeClientConnManager cm = new ThreadSafeClientConnManager(schemeRegistry);
    	// Increase max total connection to 200
    	cm.setMaxTotal(200);
    	// Increase default max connection per route to 20
    	cm.setDefaultMaxPerRoute(20);
    	// Increase max connections for localhost to 50
    	final HttpHost localhost = new HttpHost("locahost");
    	cm.setMaxForRoute(new HttpRoute(localhost), 50);
    	 
    	this.http = new DefaultHttpClient(cm);
    }
    
    public void shutdown() {
    	this.http.getConnectionManager().shutdown();
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

    	checkResponseCode(doConnect(opts));
		
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

        opts.method = "GET";
        
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

        HttpResponse resp = null;
        try {
        	
        	checkResponseCode(resp = doConnect(opts));
        	
        	final RangeCountResult result = rangeCountResults(resp);
        	
        	return result.rangeCount;
        	
        } finally {
        	
        	try {
        	
        		if (resp != null)
        			EntityUtils.consume(resp.getEntity());
        		
        	} catch (Exception ex) { }
        	
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
        
        add.prepareForWire();
        
        if (add.format != null) {
        	
            final ByteArrayEntity entity = new ByteArrayEntity(add.data);
            entity.setContentType(add.format.getDefaultMIMEType());
            
            opts.entity = entity;
            
        }
        	
        if (add.uri != null) {
	        // set the resource to load.
	        opts.addRequestParam("uri", add.uri);
        }
        
        if (add.context != null) {
	        // set the default context.
	        opts.addRequestParam("context-uri", add.context);
        }
        
        HttpResponse response = null;
        try {
        	
        	checkResponseCode(response = doConnect(opts));
        	
        	final MutationResult result = mutationResults(response);
        	
        	return result.mutationCount;
        	
        } finally {
        	
        	try {
            	
        		if (response != null)
        			EntityUtils.consume(response.getEntity());
        		
        	} catch (Exception ex) { }
        	
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

        remove.prepareForWire();
        
        if (remove.format != null) {
        	
        	opts.method = "POST";
        	opts.addRequestParam("delete");
        	
            final ByteArrayEntity entity = new ByteArrayEntity(remove.data);
            entity.setContentType(remove.format.getDefaultMIMEType());
            
            opts.entity = entity;
            
        } else {
        	
            opts.method = "DELETE";
            
            if (remove.query != null) {
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
            
        }
        
        HttpResponse response = null;
        try {
        	
        	checkResponseCode(response = doConnect(opts));
        	
        	final MutationResult result = mutationResults(response);
        	
        	return result.mutationCount;
        	
        } finally {
        	
        	try {
            	
        		if (response != null)
        			EntityUtils.consume(response.getEntity());
        		
        	} catch (Exception ex) { }
        	
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
        
        remove.prepareForWire();
        add.prepareForWire();
        
        if (remove.format != null) {
        	
        	opts.method = "POST";
        	opts.addRequestParam("update");

        	final MultipartEntity entity = new MultipartEntity();
        	entity.addPart(new FormBodyPart("remove", 
        			new ByteArrayBody(
        					remove.data, 
        					remove.format.getDefaultMIMEType(), 
        					"remove")));
        	entity.addPart(new FormBodyPart("add", 
        			new ByteArrayBody(
        					add.data, 
        					add.format.getDefaultMIMEType(), 
        					"add")));
        	
        	opts.entity = entity;
        	
        } else {
        	
            opts.method = "PUT";
	        opts.addRequestParam("query", remove.query);

	        final ByteArrayEntity entity = new ByteArrayEntity(add.data);
	        entity.setContentType(add.format.getDefaultMIMEType());
	        
	        opts.entity = entity;
	        
        }
        
        if (add.context != null) {
	        // set the default context.
	        opts.addRequestParam("context-uri", add.context);
        }
        
        HttpResponse response = null;
        try {
        	
        	checkResponseCode(response = doConnect(opts));
        	
        	final MutationResult result = mutationResults(response);
        	
        	return result.mutationCount;
        	
        } finally {
        	
        	try {
            	
        		if (response != null)
        			EntityUtils.consume(response.getEntity());
        		
        	} catch (Exception ex) { }
        	
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
		
		protected ConnectOptions getConnectOpts() throws Exception {
			
	        final ConnectOptions opts = new ConnectOptions(serviceURL+"/sparql");

	        opts.method = "POST";
	        opts.addRequestParam("query", query);
	        opts.addRequestParam("queryId", getQueryId().toString());

        	return opts;
	        	
		}
		
	}
	
	public final class TupleQuery extends Query {
		
		public TupleQuery(final UUID id, final String query) {
			super(id, query);
		}
		
		public TupleQueryResult evaluate() throws Exception {
			
	        HttpResponse response = null;
	        try {
	        	
	        	checkResponseCode(response = doConnect(getConnectOpts()));
	        	
	        	return tupleResults(response);
	        	
	        } finally {
	        	
	        	try {
	            	
	        		if (response != null)
	        			EntityUtils.consume(response.getEntity());
	        		
	        	} catch (Exception ex) { }
	        	
	        }
			
		}
		
	}

	public final class GraphQuery extends Query {
		
		public GraphQuery(final UUID id, final String query) {
			super(id, query);
		}
		
		public Graph evaluate() throws Exception {

	        HttpResponse response = null;
	        try {
	        	
	        	checkResponseCode(response = doConnect(getConnectOpts()));
	        	
	        	return graphResults(response);
	        	
	        } finally {
	        	
	        	try {
	            	
	        		if (response != null)
	        			EntityUtils.consume(response.getEntity());
	        		
	        	} catch (Exception ex) { }
	        	
	        }
			
		}
		
	}

	public final class BooleanQuery extends Query {
		
		public BooleanQuery(final UUID id, final String query) {
			super(id, query);
		}
		
		public boolean evaluate() throws Exception {
            
	        HttpResponse response = null;
	        try {
	        	
	        	checkResponseCode(response = doConnect(getConnectOpts()));
	        	
	        	return booleanResults(response);
	        	
	        } finally {
	        	
	        	try {
	            	
	        		if (response != null)
	        			EntityUtils.consume(response.getEntity());
	        		
	        	} catch (Exception ex) { }
	        	
	        }
	        
		}
		
	}

	/**
	 * Add by URI, statements, or file.
	 */
	public static class AddOp {

		private String uri;
		private Iterable<Statement> stmts;
		private byte[] data;
		private File file;
		private RDFFormat format;
		
		private String context;
		
		public AddOp(final String uri) {
			this.uri = uri;
		}
		
		public AddOp(final Iterable<Statement> stmts) {
			this.stmts = stmts;
		}
		
		public AddOp(final File file, final RDFFormat format) {
			this.file = file;
			this.format = format;
		}
		
		/**
		 * This ctor is for the test cases.
		 */
		public AddOp(final byte[] data, final RDFFormat format) {
			this.data = data;
			this.format = format;
		}
		
		public void setContext(final String context) {
			this.context = context;
		}
		
		private void prepareForWire() throws Exception {
			
	        if (file != null) {

	        	// set the data
	        	data = IOUtil.readBytes(file);
	        	
	        } else if (stmts != null) {
	        	
	        	// set the data and content type (TRIG by default)
	        	format = RDFFormat.TRIG;
			    data = serialize(stmts, format);

	        }
	        
		}
		
	}
	
	/**
	 * Remove by query, access path, statements, or file.
	 */
	public static class RemoveOp {
		
		private String query;
		
		private Iterable<Statement> stmts;
		
		private Value s, p, o, c;
		
		private byte[] data;
		
		private File file;
		
		private RDFFormat format;
		
		public RemoveOp(final String query) {
			this.query = query;
		}
		
		public RemoveOp(final Iterable<Statement> stmts) {
			this.stmts = stmts;
		}
		
		public RemoveOp(final URI s, final URI p, final Value o, final URI c) {
			this.s = s;
			this.p = p;
			this.o = o;
			this.c = c;
		}

		public RemoveOp(final File file, final RDFFormat format) {
			this.file = file;
			this.format = format;
		}
		
		/**
		 * This ctor is for the test cases.
		 */
		public RemoveOp(final byte[] data, final RDFFormat format) {
			this.data = data;
			this.format = format;
		}
		
		private void prepareForWire() throws Exception {
			
	        if (file != null) {

	        	// set the data
	        	data = IOUtil.readBytes(file);
	        	
	        } else if (stmts != null) {
	        	
	        	// set the data and content type (TRIG by default)
	        	format = RDFFormat.TRIG;
			    data = serialize(stmts, format);

	        }
	        
		}
		
	}
	
    /**
     * Options for the HTTP connection.
     */
    private static class ConnectOptions {

        /** The URL of the SPARQL end point. */
        public String serviceURL = null;
        
        /** The HTTP method (GET, POST, etc). */
        public String method = "POST";

		/** The accept header. */
        public String acceptHeader = //
	        BigdataRDFServlet.MIME_SPARQL_RESULTS_XML + ";q=1" + //
	        "," + //
	        RDFFormat.RDFXML.getDefaultMIMEType() + ";q=1"//
	        ;
        
        /** Request parameters to be formatted as URL query parameters. */
        public Map<String,String[]> requestParams;

        /** Request entity. */
        public HttpEntity entity = null;
        
        /** The connection timeout (ms) -or- ZERO (0) for an infinite timeout. */
        public int timeout = 0;

        public ConnectOptions(final String serviceURL) {
        
            this.serviceURL = serviceURL;
            
        }
        
        public void addRequestParam(final String name, final String[] vals) {
        	
        	if (requestParams == null) {
        		requestParams = new LinkedHashMap<String, String[]>();
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
        	
        	return (requestParams != null && requestParams.containsKey(name))
        				? requestParams.get(name)[0] : null;
        	
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
    protected HttpResponse doConnect(final ConnectOptions opts)
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

        HttpUriRequest request = null;
        try {

            request = newRequest(urlString.toString(), opts.method);
            
            request.addHeader("Accept", opts.acceptHeader);
            if (log.isDebugEnabled())
                log.debug("Accept: " + opts.acceptHeader);
            
//        	// conn = doConnect(urlString.toString(), opts.method);
//            final URL url = new URL(urlString.toString());
//            conn = (HttpURLConnection) url.openConnection();
//            conn.setRequestMethod(opts.method);
//            conn.setDoOutput(true);
//            conn.setDoInput(true);
//            conn.setUseCaches(false);
//            conn.setReadTimeout(opts.timeout);
//            conn.setRequestProperty("Accept", opts.acceptHeader);
//            if (log.isDebugEnabled())
//                log.debug("Accept: " + opts.acceptHeader);
            
            if (opts.entity != null) {

//                if (opts.data == null)
//                    throw new AssertionError();

//                final String contentLength = Integer.toString(opts.data.length);

//                conn.setRequestProperty("Content-Type", opts.contentType);
//                conn.setRequestProperty("Content-Length", contentLength);

//                if (log.isDebugEnabled()) {
//                    log.debug("Content-Type: " + opts.contentType);
//                    log.debug("Content-Length: " + contentLength);
//                }

//                final ByteArrayEntity entity = new ByteArrayEntity(opts.data);
//            	entity.setContentType(opts.contentType);

            	((HttpEntityEnclosingRequestBase) request).setEntity(opts.entity);
                
//                final OutputStream os = conn.getOutputStream();
//                try {
//                    os.write(opts.data);
//                    os.flush();
//                } finally {
//                    os.close();
//                }

            }

            final HttpResponse response = http.execute(request);
            
            return response;
            
//            // connect.
//            conn.connect();
//
//            return conn;

        } catch (Throwable t) {
            /*
             * If something goes wrong, then close the http connection.
             * Otherwise, the connection will be closed by the caller.
             */
            try {
            	
            	if (request != null)
            		request.abort();
            	
//                // clean up the connection resources
//                if (conn != null)
//                    conn.disconnect();
                
            } catch (Throwable t2) {
                // ignored.
            }
            throw new RuntimeException(toString() + " : " + t, t);
        }

    }
    
    protected HttpUriRequest newRequest(final String uri, final String method) {
    	if (method.equals("GET")) {
    		return new HttpGet(uri);
    	} else if (method.equals("POST")) {
    		return new HttpPost(uri);
    	} else if (method.equals("DELETE")) {
    		return new HttpDelete(uri);
    	} else if (method.equals("PUT")) {
    		return new HttpPut(uri);
    	} else {
    		throw new IllegalArgumentException();
    	}
    }

    protected HttpResponse checkResponseCode(final HttpResponse response)
            throws IOException {
        final int rc = response.getStatusLine().getStatusCode();
        if (rc < 200 || rc >= 300) {
            // conn.disconnect();
            throw new IOException("Status Code=" + rc + ", Status Line="
                    + response.getStatusLine() + ", Response="
                    + getResponseBody(response));
        }

        if (log.isDebugEnabled()) {
            /*
             * write out the status list, headers, etc.
             */
            log.debug("*** Response ***");
            log.debug("Status Line: " + response.getStatusLine());
        }
        return response;
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

    protected static String getResponseBody(final HttpResponse response)
			throws IOException {

		final Reader r = new InputStreamReader(response.getEntity().getContent());

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
     * Extracts the solutions from a SPARQL query.
     * 
     * @param response
     *            The connection from which to read the results.
     * 
     * @return The results.
     * 
     * @throws Exception
     *             If anything goes wrong.
     */
    protected TupleQueryResult tupleResults(final HttpResponse response)
            throws Exception {

    	HttpEntity entity = null;
    	try {
    		
    		entity = response.getEntity();
    		
	        final String contentType = entity.getContentType().getValue();
	
	        final MiniMime mimeType = new MiniMime(contentType);
	        
	        final TupleQueryResultFormat format = TupleQueryResultFormat
	                .forMIMEType(mimeType.getMimeType());
	
	        if (format == null)
	            throw new IOException(
	                    "Could not identify format for service response: serviceURI="
	                            + serviceURL + ", contentType=" + contentType
	                            + " : response=" + getResponseBody(response));
	
	        final TupleQueryResultParserFactory parserFactory = TupleQueryResultParserRegistry
	                .getInstance().get(format);
	
	        final TupleQueryResultParser parser = parserFactory.getParser();
	
	        final TupleQueryResultBuilder handler = new TupleQueryResultBuilder();
	
	        parser.setTupleQueryResultHandler(handler);
	
	        parser.parse(entity.getContent());
	
	        // done.
	        return handler.getQueryResult();
	        
    	} finally {
    		
//			// terminate the http connection.
//    		response.disconnect();
    		EntityUtils.consume(entity);
    		
    	}

    }
    
	/**
	 * Builds a graph from an RDF result set (statements, not binding sets).
	 * 
	 * @param response
	 *            The connection from which to read the results.
	 * 
	 * @return The graph
	 * 
	 * @throws Exception
	 *             If anything goes wrong.
	 */
	protected Graph graphResults(final HttpResponse response) throws Exception {

    	HttpEntity entity = null;
		try {

    		entity = response.getEntity();
    		
			final String baseURI = "";

	        final String contentType = entity.getContentType().getValue();

            if (contentType == null)
                throw new RuntimeException("Not found: Content-Type");
            
            final MiniMime mimeType = new MiniMime(contentType);
            
            final RDFFormat format = RDFFormat
                    .forMIMEType(mimeType.getMimeType());

            if (format == null)
                throw new IOException(
                        "Could not identify format for service response: serviceURI="
                                + serviceURL + ", contentType=" + contentType
                                + " : response=" + getResponseBody(response));

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

			rdfParser.parse(entity.getContent(), baseURI);

//			return new GraphQueryResultImpl(Collections.EMPTY_MAP, g);
			return g;

		} finally {

//			// terminate the http connection.
//			response.disconnect();
			EntityUtils.consume(entity);

		}

	}

    /**
     * Parse a SPARQL result set for an ASK query.
     * 
     * @param response
     *            The connection from which to read the results.
     * 
     * @return <code>true</code> or <code>false</code> depending on what was
     *         encoded in the SPARQL result set.
     * 
     * @throws Exception
     *             If anything goes wrong, including if the result set does not
     *             encode a single boolean value.
     */
    protected boolean booleanResults(final HttpResponse response) throws Exception {

    	HttpEntity entity = null;
        try {

    		entity = response.getEntity();
    		
	        final String contentType = entity.getContentType().getValue();

            final MiniMime mimeType = new MiniMime(contentType);
            
            final BooleanQueryResultFormat format = BooleanQueryResultFormat
                    .forMIMEType(mimeType.getMimeType());

            if (format == null)
                throw new IOException(
                        "Could not identify format for service response: serviceURI="
                                + serviceURL + ", contentType=" + contentType
                                + " : response=" + getResponseBody(response));

            final BooleanQueryResultParserFactory factory = BooleanQueryResultParserRegistry
                    .getInstance().get(format);

            if (factory == null)
                throw new RuntimeException("No factory for Content-Type: " + contentType);

            final BooleanQueryResultParser parser = factory.getParser();

            final boolean result = parser.parse(entity.getContent());
            
            return result;

        } finally {

//            // terminate the http connection.
//            response.disconnect();
        	EntityUtils.consume(entity);

        }

    }

	/**
	 * Counts the #of results in a SPARQL result set.
	 * 
	 * @param response
	 *            The connection from which to read the results.
	 * 
	 * @return The #of results.
	 * 
	 * @throws Exception
	 *             If anything goes wrong.
	 */
	protected long countResults(final HttpResponse response) throws Exception {

		HttpEntity entity = null;
        try {

    		entity = response.getEntity();
    		
	        final String contentType = entity.getContentType().getValue();

            final MiniMime mimeType = new MiniMime(contentType);
            
            final TupleQueryResultFormat format = TupleQueryResultFormat
                    .forMIMEType(mimeType.getMimeType());

            if (format == null)
                throw new IOException(
                        "Could not identify format for service response: serviceURI="
                                + serviceURL + ", contentType=" + contentType
                                + " : response=" + getResponseBody(response));

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

			parser.parse(entity.getContent());

			if (log.isInfoEnabled())
				log.info("nsolutions=" + nsolutions);

			// done.
			return nsolutions.longValue();

		} finally {

//			// terminate the http connection.
//			response.disconnect();
			EntityUtils.consume(entity);

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

    protected MutationResult mutationResults(final HttpResponse response) throws Exception {

		HttpEntity entity = null;
        try {

    		entity = response.getEntity();
    		
	        final String contentType = entity.getContentType().getValue();

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
            parser.parse(entity.getContent(), new DefaultHandler2(){

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

//            // terminate the http connection.
//            response.disconnect();
        	EntityUtils.consume(entity);

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

    protected RangeCountResult rangeCountResults(final HttpResponse response) throws Exception {

		HttpEntity entity = null;
        try {

    		entity = response.getEntity();
    		
	        final String contentType = entity.getContentType().getValue();

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
            parser.parse(entity.getContent(), new DefaultHandler2(){

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

//            // terminate the http connection.
//            response.disconnect();
        	EntityUtils.consume(entity);

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
