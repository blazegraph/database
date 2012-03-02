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

package com.bigdata.rdf.sparql.ast;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.TupleQueryResultBuilder;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.query.resultio.TupleQueryResultParserFactory;
import org.openrdf.query.resultio.TupleQueryResultParserRegistry;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sail.Sesame2BigdataIterator;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.striterator.ICloseableIterator;

/**
 * This class handleS vectored remote service invocation by generating an
 * appropriate SPARQL query (with BINDINGS) and an appropriate HTTP request.
 * 
 * TODO Do a version of this which does not vector solutions through the
 * BINDINGS clause? The purpose would be compatibility with service end points
 * which do not support BINDINGS, e.g., SPARQL 1.0 end points or non-conforming
 * SPARQL 1.1 end points).
 * 
 * TODO Annotations for additional URL query parameters (defaultGraph, etc),
 * authentication, HTTP METHOD (GET/POST), preferred result format (SPARQL,
 * BINARY, etc.), etc.  Those things are most extensibly expressed in SPARQL
 * using query hints.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RemoteServiceCallImpl implements RemoteServiceCall {

    private static final Logger log = Logger
            .getLogger(RemoteServiceCallImpl.class);

    /**
     * The name of the <code>UTF-8</code> character encoding.
     */
    static private final String UTF8 = "UTF-8";
    
//    private final AbstractTripleStore store;
    private final IGroupNode<IGroupMemberNode> groupNode;
    private final URI serviceURI;
    private final String exprImage;
    private final Map<String,String> prefixDecls;
    private final Map<String,String> namespaces;
    
    public RemoteServiceCallImpl(final AbstractTripleStore store,
            final IGroupNode<IGroupMemberNode> groupNode, final URI serviceURI,
            final String exprImage, final Map<String, String> prefixDecls) {

//        if (store == null)
//            throw new IllegalArgumentException();

        if (groupNode == null)
            throw new IllegalArgumentException();

        if (serviceURI == null)
            throw new IllegalArgumentException();

        if (exprImage == null)
            throw new IllegalArgumentException();

//        this.store = store;
        
        this.groupNode = groupNode;
        
        this.serviceURI = serviceURI;
        
        this.exprImage = exprImage;
        
        this.prefixDecls = prefixDecls;
        
        if (prefixDecls != null) {
            /*
             * Build up a reverse map from namespace to prefix.
             */
            namespaces = new HashMap<String, String>();
            for (Map.Entry<String, String> e : prefixDecls.entrySet()) {
                namespaces.put(e.getValue(), e.getKey());
            }
        } else {
            namespaces = null;
        }

    }

    /**
     * Return the distinct variables "projected" by the SERVICE group graph
     * pattern. The order of this set is not important, but the variables must
     * be distinct.
     * 
     * FIXME This is not quite correct. It will find variables inside of
     * subqueries which are not visible. We should compute the projected
     * variables in the query plan generator and just attach them to the
     * ServiceCall JOIN and then pass them into this class (yet another
     * attribute for the service call and the {@link ServiceFactory}).
     */
    private Set<IVariable<?>> getProjectedVars() {

        final LinkedHashSet<IVariable<?>> projectedVars = new LinkedHashSet<IVariable<?>>();
        
        final Iterator<IVariable<?>> itr = BOpUtility
                .getSpannedVariables((BOp) groupNode);
        
        while (itr.hasNext()) {
        
            projectedVars.add(itr.next());
            
        }

        return projectedVars;
        
    }

    /**
     * Return an ordered collection of the distinct variable names used in the
     * given caller's solution set.
     * 
     * @param bindingSets
     *            The solution set.
     * 
     * @return The distinct, ordered collection of variables used.
     */
    private LinkedHashSet<String> getDistinctVars(final BindingSet[] bindingSets) {

        final LinkedHashSet<String> vars = new LinkedHashSet<String>();

        for (BindingSet bindingSet : bindingSets) {

            for (Binding binding : bindingSet) {

                vars.add(binding.getName());

            }

        }

        return vars;

    }

    /**
     * Return the SPARQL query that will be sent to the remote SPARQL end point.
     * 
     * @param bindingSets
     *            The source solutions. These will be used to create a BINDINGS
     *            clause for the query.
     *            
     * @return The query.
     */
    public String getSparqlQuery(final BindingSet[] bindingSets) {
        
        final StringBuilder sb = new StringBuilder();

        /*
         * Prefix declarations.
         * 
         * Note: The prefix declarations need to be harvested and passed along
         * since we are using the text image of the SERVICE group graph pattern
         * in the WHERE clause.
         */
        if (prefixDecls != null) {

            for (Map.Entry<String, String> e : prefixDecls.entrySet()) {

                sb.append("\n");
                sb.append("prefix ");
                sb.append(e.getKey());
                sb.append(":");
                sb.append(" <");
                sb.append(e.getValue());
                sb.append(">");
                sb.append("\n");
                
            }
            
        }

        /*
         * SELECT clause.
         */
        {
            final Set<IVariable<?>> projectedVars = getProjectedVars();
            sb.append("SELECT ");
            for (IVariable<?> v : projectedVars) {
                sb.append(" ?");
                sb.append(v.getName());
            }
            sb.append("\n");
        }

        /*
         * WHERE clause.
         * 
         * Note: This uses the actual SPARQL text image for the SERVICE's graph
         * pattern.
         */
        {
            // clip out just the graph pattern from the SERVICE clause.
            final int beginIndex = exprImage.indexOf("{");
            if (beginIndex == -1)
                throw new RuntimeException();
            final int endIndex = exprImage.lastIndexOf("}") + 1;
            if (endIndex < beginIndex)
                throw new RuntimeException();
            final String tmp = exprImage.substring(beginIndex, endIndex);
            sb.append("WHERE\n");
            sb.append(tmp);
            sb.append("\n");
        }

        /*
         * BINDINGS clause.
         * 
         * Note: The BINDINGS clause is used to vector the SERVICE request.
         * 
         * BINDINGS ?book ?title { (:book1 :title1) (:book2 UNDEF) }
         */
        {
            // Variables in a known stable order.
            final LinkedHashSet<String> vars = getDistinctVars(bindingSets);

            sb.append("BINDINGS");

            // Variable declarations.
            {

                for (String v : vars) {
                    sb.append(" ?");
                    sb.append(v);
                }
            }

            // Bindings.
            sb.append(" {\n"); //
            for (BindingSet bindingSet : bindingSets) {
                sb.append("(");
                for (String v : vars) {
                    sb.append(" ");
                    final Binding b = bindingSet.getBinding(v);
                    if (b == null) {
                        sb.append("UNDEF");
                    } else {
                        final Value val = b.getValue();
                        final String ext = toExternal(val);
                        sb.append(ext);
                    }
                }
                sb.append(" )");
                sb.append("\n");
            }
            sb.append("}\n");

        }

        return sb.toString();

    }
    
    /**
     * Return an external form for the {@link Value} suitable for direct
     * embedding into a SPARQL query.
     * 
     * @param val
     *            The value.
     * 
     * @return The external form.
     * 
     *         TODO This should be lifted out into a utility class and should
     *         have its own test suite.
     */
    private String toExternal(final Value val) {
        
        if (val instanceof URI) {

            return toExternal((URI) val);
        
        } else if (val instanceof Literal) {
        
            return toExternal((Literal)val);
            
        } else if (val instanceof BNode) {
            
            /*
             * Note: The SPARQL 1.1 GRAMMAR does not permit blank nodes in the
             * BINDINGS clause.
             */
            
            throw new UnsupportedOperationException(
                    "Blank node not permitted in BINDINGS");
            
        } else {
            
            throw new AssertionError();
            
        }

    }
    
    private String toExternal(final URI uri) {

        if (prefixDecls != null) {

            final String prefix = namespaces.get(uri.getNamespace());

            if (prefix != null) {

                return prefix + ":" + uri.getLocalName();

            }

        }

        return "<" + uri.stringValue() + ">";

    }
    
    private String toExternal(final Literal lit) {

        final String label = lit.getLabel();
        
        final String languageCode = lit.getLanguage();
        
        final URI datatypeURI = lit.getDatatype();

        final String datatypeStr = datatypeURI == null ? null
                : toExternal(datatypeURI);

        final StringBuilder sb = new StringBuilder((label.length() + 2)
                + (languageCode != null ? (languageCode.length() + 1) : 0)
                + (datatypeURI != null ? datatypeStr.length() + 2 : 0));

        sb.append('"');
        sb.append(label);
        sb.append('"');

        if (languageCode != null) {
            sb.append('@');
            sb.append(languageCode);
        }

        if (datatypeURI != null) {
            sb.append("^^");
            sb.append(datatypeStr);
        }

        return sb.toString();

    }

    @Override
    public ICloseableIterator<BindingSet> call(final BindingSet[] bindingSets)
            throws Exception {

        final QueryOptions opts = new QueryOptions(serviceURI.stringValue());

        opts.queryStr = getSparqlQuery(bindingSets);
        
        final TupleQueryResultFormat resultFormat = TupleQueryResultFormat.BINARY;

        opts.acceptHeader = //
        resultFormat.getDefaultMIMEType() + ";q=1" + //
                "," + //
                TupleQueryResultFormat.SPARQL.getDefaultMIMEType() + ";q=1"//
        ;

        /*
         * Note: This does not stream chunks back. The ServiceCallJoin currently
         * materializes all solutions from the service in a single chunk, so
         * there is no point doing something incremental here unless it is
         * coordinated with the ServiceCallJoin.
         */

        final TupleQueryResult queryResult; 
        
        try {

            queryResult = parseResults(doSparqlQuery(opts));
            
        } finally {

            /*
             * Note: HttpURLConnection.disconnect() is not a "close". close() is
             * an implicit action for this class.
             */
            
        }
        
        return new Sesame2BigdataIterator<BindingSet, QueryEvaluationException>(
                queryResult);

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
    protected TupleQueryResult parseResults(final HttpURLConnection conn)
            throws Exception {

        final String contentType = conn.getContentType();

        final TupleQueryResultFormat format = TupleQueryResultFormat
                .forMIMEType(contentType);

        if (format == null)
            throw new IOException(
                    "Could not identify format for service response: serviceURI="
                            + serviceURI + ", contentType=" + contentType);

        final TupleQueryResultParserFactory parserFactory = TupleQueryResultParserRegistry
                .getInstance().get(format);

        final TupleQueryResultParser parser = parserFactory.getParser();

        final TupleQueryResultBuilder handler = new TupleQueryResultBuilder();

        parser.setTupleQueryResultHandler(handler);

        parser.parse(conn.getInputStream());

        // done.
        return handler.getQueryResult();

    }

    /**
     * Options for the query.
     */
    private static class QueryOptions {

        /** The URL of the SPARQL end point. */
        public String serviceURL = null;
        
        /** The HTTP method (GET, POST, etc). */
        public String method = "GET";

        /** The accept header. */
        public String acceptHeader;
        
        /**
         * The SPARQL query (this is a short hand for setting the
         * <code>query</code> URL query parameter).
         */
        public String queryStr = null;
        
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

        public QueryOptions(final String serviceURL) {
        
            this.serviceURL = serviceURL;
            
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
    protected HttpURLConnection doSparqlQuery(final QueryOptions opts)
            throws Exception {

        /*
         * Generate the fully formed and encoded URL.
         */

        final StringBuilder urlString = new StringBuilder(opts.serviceURL);

        if (opts.queryStr != null) {

            if (opts.requestParams == null) {

                opts.requestParams = new LinkedHashMap<String, String[]>();

            }
            
            opts.requestParams.put("query", new String[] { opts.queryStr });

        }

        addQueryParams(urlString, opts.requestParams);

        if (log.isDebugEnabled()) {
            log.debug("*** Request ***");
            log.debug(serviceURI);
            log.debug(opts.queryStr);
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

            if (opts.contentType != null) {

                if (opts.data == null)
                    throw new AssertionError();

                conn.setRequestProperty("Content-Type", opts.contentType);

                conn.setRequestProperty("Content-Length",
                        Integer.toString(opts.data.length));

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
            throw new RuntimeException(t);
        }

    }

    protected HttpURLConnection checkResponseCode(final HttpURLConnection conn)
            throws IOException {
        final int rc = conn.getResponseCode();
        if (rc < 200 || rc >= 300) {
            conn.disconnect();
            throw new IOException(conn.getResponseMessage());
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

}