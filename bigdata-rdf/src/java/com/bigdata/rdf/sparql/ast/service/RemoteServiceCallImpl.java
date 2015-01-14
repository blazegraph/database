/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Mar 1, 2012
 */

package com.bigdata.rdf.sparql.ast.service;

import java.util.UUID;

import org.eclipse.jetty.client.HttpClient;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import com.bigdata.rdf.sail.Sesame2BigdataIterator;
import com.bigdata.rdf.sail.webapp.client.ConnectOptions;
import com.bigdata.rdf.sail.webapp.client.AutoCloseHttpClient;
import com.bigdata.rdf.sail.webapp.client.DefaultClientConnectionManagerFactory;
import com.bigdata.rdf.sail.webapp.client.JettyRemoteRepositoryManager;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * This class handles vectored remote service invocation by generating an
 * appropriate SPARQL query (with BINDINGS) and an appropriate HTTP request. The
 * behavior of this class may be configured in the {@link ServiceRegistry} by
 * adjusting the {@link RemoteServiceOptions} for the service URI.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class RemoteServiceCallImpl implements RemoteServiceCall {

//    private static final Logger log = Logger
//            .getLogger(RemoteServiceCallImpl.class);

//    /**
//     * The name of the <code>UTF-8</code> character encoding.
//     */
//    static private final String UTF8 = "UTF-8";
    
    private final ServiceCallCreateParams params;
//    private final URI serviceURI;
//    private final ServiceNode serviceNode;
//    private final RemoteServiceOptions serviceOptions;

    @Override
    public String toString() {

        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName());
        sb.append("{params=" + params);
        sb.append("}");
        return sb.toString();
        
    }
    
    public RemoteServiceCallImpl(final ServiceCallCreateParams params) {

        if (params == null)
            throw new IllegalArgumentException();

        this.params = params;

    }
    
    @Override
    public RemoteServiceOptions getServiceOptions() {
        
        return (RemoteServiceOptions) params.getServiceOptions();
        
    }

    @Override
    public ICloseableIterator<BindingSet> call(final BindingSet[] bindingSets)
            throws Exception {

        final String uriStr = params.getServiceURI().stringValue();
        
        final RemoteServiceOptions serviceOptions = getServiceOptions();

        final ConnectOptions o = new ConnectOptions(uriStr);

        {

            final String acceptHeader = serviceOptions.getAcceptHeader();

            if (acceptHeader != null) {

                o.setAcceptHeader(acceptHeader);
                
            } else {
                
                o.setAcceptHeader(ConnectOptions.DEFAULT_SOLUTIONS_ACCEPT_HEADER);

            }
            
        }
        
        o.method = serviceOptions.isGET() ? "GET" : "POST";
        
//        final QueryOptions opts = new QueryOptions(serviceURI.stringValue());
//
//        opts.acceptHeader = serviceOptions.getAcceptHeader();
        
        /*
         * Note: This uses a factory pattern to handle each of the possible ways
         * in which we have to vector solutions to the service end point.
         */
        final IRemoteSparqlQueryBuilder queryBuilder = RemoteSparqlBuilderFactory
                .get(serviceOptions, params.getServiceNode(), bindingSets);
        
        final String queryStr = queryBuilder.getSparqlQuery(bindingSets);
        
//        opts.queryStr = queryStr;
        
        final UUID queryId = UUID.randomUUID();
        
        o.addRequestParam("query", queryStr);
        
        o.addRequestParam("queryId", queryId.toString());
        
       	final HttpClient client = DefaultClientConnectionManagerFactory.getInstance().newInstance();
        final JettyRemoteRepositoryManager repo = new JettyRemoteRepositoryManager(//
                uriStr,//
                params.getServiceOptions().isBigdataLBS(),// useLBS
                client,
                params.getTripleStore().getExecutorService()
                );
        
        /*
         * Note: This does not stream chunks back. The ServiceCallJoin currently
         * materializes all solutions from the service in a single chunk, so
         * there is no point doing something incremental here unless it is
         * coordinated with the ServiceCallJoin.
         */

        final TupleQueryResult queryResult;

        try {

//            final HttpResponse resp = repo.doConnect(o);
//            
//            RemoteRepository.checkResponseCode(resp);
//            
//            queryResult = repo.tupleResults(resp);
//            
////            queryResult = parseResults(checkResponseCode(doSparqlQuery(opts)));

            queryResult = repo.tupleResults(o, queryId, null);
            
        } finally {

            repo.close();
            
            client.stop();

        }

        return new Sesame2BigdataIterator<BindingSet, QueryEvaluationException>(
                        queryResult);

    }
        
//    /**
//     * Extracts the solutions from a SPARQL query.
//     * 
//     * @param conn
//     *            The connection from which to read the results.
//     * 
//     * @return The results.
//     * 
//     * @throws Exception
//     *             If anything goes wrong.
//     */
//    protected TupleQueryResult parseResults(final HttpURLConnection conn)
//            throws Exception {
//
//        final String contentType = conn.getContentType();
//
//        final MiniMime mimeType = new MiniMime(contentType);
//        
//        final TupleQueryResultFormat format = TupleQueryResultFormat
//                .forMIMEType(mimeType.getMimeType());
//
//        if (format == null)
//            throw new IOException(
//                    "Could not identify format for service response: serviceURI="
//                            + serviceURI + ", contentType=" + contentType
//                            + " : response=" + getResponseBody(conn));
//
//        final TupleQueryResultParserFactory parserFactory = TupleQueryResultParserRegistry
//                .getInstance().get(format);
//
//        final TupleQueryResultParser parser = parserFactory.getParser();
//
//        final TupleQueryResultBuilder handler = new TupleQueryResultBuilder();
//
//        parser.setTupleQueryResultHandler(handler);
//
//        parser.parse(conn.getInputStream());
//
//        // done.
//        return handler.getQueryResult();
//
//    }
    
//    protected static String getResponseBody(final HttpURLConnection conn)
//            throws IOException {
//
//        final Reader r = new InputStreamReader(conn.getInputStream());
//    
//        try {
//    
//            final StringWriter w = new StringWriter();
//    
//            int ch;
//            while ((ch = r.read()) != -1) {
//    
//                w.append((char) ch);
//    
//            }
//    
//            return w.toString();
//        
//        } finally {
//            
//            r.close();
//            
//        }
//        
//    }

//    /**
//     * Options for the query.
//     */
//    private static class QueryOptions {
//
//        /** The URL of the SPARQL end point. */
//        public String serviceURL = null;
//        
//        /** The HTTP method (GET, POST, etc). */
//        public String method = "GET";
//
//        /** The accept header. */
//        public String acceptHeader;
//        
//        /**
//         * The SPARQL query (this is a short hand for setting the
//         * <code>query</code> URL query parameter).
//         */
//        public String queryStr = null;
//        
//        /** Request parameters to be formatted as URL query parameters. */
//        public Map<String,String[]> requestParams;
//        
//        /**
//         * The Content-Type (iff there will be a request body).
//         */
//        public String contentType = null;
//        
//        /**
//         * The data to send as the request body (optional).
//         */
//        public byte[] data = null;
//        
//        /** The connection timeout (ms) -or- ZERO (0) for an infinite timeout. */
//        public int timeout = 0;
//
//        public QueryOptions(final String serviceURL) {
//        
//            this.serviceURL = serviceURL;
//            
//        }
//        
//    }
//
//    /**
//     * Connect to a SPARQL end point (GET or POST query only).
//     * 
//     * @param opts
//     *            The query request.
//     * @param requestPath
//     *            The request path, including the leading "/".
//     * 
//     * @return The connection.
//     */
//    protected HttpURLConnection doSparqlQuery(final QueryOptions opts)
//            throws Exception {
//
//        /*
//         * Generate the fully formed and encoded URL.
//         */
//
//        final StringBuilder urlString = new StringBuilder(opts.serviceURL);
//
//        if (opts.queryStr != null) {
//
//            if (opts.requestParams == null) {
//
//                opts.requestParams = new LinkedHashMap<String, String[]>();
//
//            }
//            
//            opts.requestParams.put("query", new String[] { opts.queryStr });
//
//        }
//
//        addQueryParams(urlString, opts.requestParams);
//
//        if (log.isDebugEnabled()) {
//            log.debug("*** Request ***");
//            log.debug(serviceURI);
//            log.debug(opts.queryStr);
//        }
//
//        HttpURLConnection conn = null;
//        try {
//
//            // conn = doConnect(urlString.toString(), opts.method);
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
//            
//            if (opts.contentType != null) {
//
//                if (opts.data == null)
//                    throw new AssertionError();
//
//                final String contentLength = Integer.toString(opts.data.length);
//                
//                conn.setRequestProperty("Content-Type", opts.contentType);
//
//                conn.setRequestProperty("Content-Length", contentLength);
//
//                if (log.isDebugEnabled()) {
//                    log.debug("Content-Type: " + opts.contentType);
//                    log.debug("Content-Length: " + contentLength);
//                }
//
//                final OutputStream os = conn.getOutputStream();
//                try {
//                    os.write(opts.data);
//                    os.flush();
//                } finally {
//                    os.close();
//                }
//
//            }
//
//            // connect.
//            conn.connect();
//
//            return conn;
//
//        } catch (Throwable t) {
////            /*
////             * If something goes wrong, then close the http connection.
////             * Otherwise, the connection will be closed by the caller.
////             */
////            try {
//////                // clean up the connection resources
//////                if (conn != null)
//////                    conn.disconnect();
////            } catch (Throwable t2) {
////                // ignored.
////            }
//            throw new RuntimeException(toString() + " : " + t, t);
//        }
//
//    }
//
//    protected HttpURLConnection checkResponseCode(final HttpURLConnection conn)
//            throws IOException {
//        final int rc = conn.getResponseCode();
//        if (rc < 200 || rc >= 300) {
//            // conn.disconnect();
//            throw new IOException("Status Code=" + rc + ", Status Line="
//                    + conn.getResponseMessage() + ", Response="
//                    + getResponseBody(conn));
//        }
//
//        if (log.isDebugEnabled()) {
//            /*
//             * write out the status list, headers, etc.
//             */
//            log.debug("*** Response ***");
//            log.debug("Status Line: " + conn.getResponseMessage());
//        }
//        return conn;
//    }
//
//    /**
//     * Add any URL query parameters.
//     */
//    private void addQueryParams(final StringBuilder urlString,
//            final Map<String, String[]> requestParams)
//            throws UnsupportedEncodingException {
//        boolean first = true;
//        for (Map.Entry<String, String[]> e : requestParams.entrySet()) {
//            urlString.append(first ? "?" : "&");
//            first = false;
//            final String name = e.getKey();
//            final String[] vals = e.getValue();
//            if (vals == null) {
//                urlString.append(URLEncoder.encode(name, UTF8));
//            } else {
//                for (String val : vals) {
//                    urlString.append(URLEncoder.encode(name, UTF8));
//                    urlString.append("=");
//                    urlString.append(URLEncoder.encode(val, UTF8));
//                }
//            }
//        } // next Map.Entry
//
//    }

}
