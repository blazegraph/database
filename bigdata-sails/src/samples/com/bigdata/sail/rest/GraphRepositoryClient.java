/*
 * Copyright SYSTAP, LLC 2006-2009.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package com.bigdata.sail.rest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.ProxyHost;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.query.QueryLanguage;

/**
 * A client proxy to a remote GraphRepository instance.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class GraphRepositoryClient implements GraphRepository {
    
    protected static Logger log =
            Logger.getLogger(GraphRepositoryClient.class);

    private String servletURL;

    private transient HttpClient httpClient;

    private transient ProxyHost proxyHost;

    public GraphRepositoryClient(String servletURL) {
        this.servletURL = servletURL;
        this.proxyHost = getProxyHost();
    }

    /**
     * @see {@link GraphRepository#read(String, QueryLanguage, boolean)}
     */
    public Collection<Statement> read(final String query, 
            final QueryLanguage ql, final boolean includeInferred) 
            throws Exception {

        GetMethod get = new GetMethod(servletURL);
        // just to be nice.
        get.addRequestHeader(new Header("Accept", "application/rdf+xml"));
        // and say what we want.
        get.addRequestHeader(new Header("Accept-Charset", "UTF-8"));
        
        try {
            
            // add the range and include inferred headers
            get.addRequestHeader(new Header(
                    GraphRepositoryServlet.X_INCLUDE_INFERRED, 
                    String.valueOf(includeInferred)));
            if (query != null) {
                // add the range header
                String range =
                        ql.toString().toLowerCase() + "[" + trim(query) + "]";
                get.addRequestHeader(new Header(
                        GraphRepositoryServlet.HTTP_RANGE, range));
            }
            
            // Execute the method.
            int sc = getHttpClient().executeMethod(get);
            if (sc != HttpStatus.SC_OK && sc != HttpStatus.SC_PARTIAL_CONTENT) {
                throw new IOException("HTTP-GET failed: " + get.getStatusLine());
            }
            
            // Read the response body.
            String response = IOUtils.readString(
                    get.getResponseBodyAsStream(), get.getResponseCharSet());
            return IOUtils.deserialize(response);
            
        } finally {
            // Release the connection.
            get.releaseConnection();
        }
        
    }

    /**
     * @see {@link GraphRepository#create(Collection)}
     */
    public void create(Collection<Statement> stmts) throws Exception {
        create(IOUtils.serialize(stmts));
    }
        
    /**
     * @see {@link GraphRepository#create(String)}
     */
    public void create(String rdfXml) throws Exception {
        
        // POST
        PostMethod post = new PostMethod(servletURL);
        try {
            
            // set the body
            if (rdfXml != null) {
                post.setRequestEntity(new StringRequestEntity(
                        rdfXml, // the rdf/xml body
                        GraphRepositoryServlet.RDF_XML, // includes the encoding
                        null // so we don't need to say it here.
                        ));
                post.setContentChunked(true);
            }
            
            // Execute the method.
            int sc = getHttpClient().executeMethod(post);
            if (sc != HttpStatus.SC_OK) {
                throw new IOException("HTTP-POST failed: "
                        + post.getStatusLine());
            }
            
        } finally {
            // Release the connection.
            post.releaseConnection();
        }
        
    }

    /**
     * @see {@link GraphRepository#clear()}
     */
    public void clear() throws Exception {
        delete((String) null);
    }

    /**
     * @see {@link GraphRepository#delete(Collection)}
     */
    public void delete(Collection<Statement> stmts) throws Exception {
        delete(IOUtils.serialize(stmts));
    }
        
    /**
     * @see {@link GraphRepository#delete(String)}
     */
    public void delete(String rdfXml) throws Exception {

        // DELETE
        DeleteMethod del = new DeleteMethod(servletURL);
        try {
            
            // add the range header
            if (rdfXml != null) {
                String triples = "triples[" + trim(rdfXml) + "]";
                Header range = new Header(
                        GraphRepositoryServlet.HTTP_RANGE, triples);
                del.addRequestHeader(range);
            }
            
            // Execute the method.
            int sc = getHttpClient().executeMethod(del);
            if (sc != HttpStatus.SC_OK) {
                throw new IOException("HTTP-DELETE failed: "
                        + del.getStatusLine());
            }
            
        } finally {
            // Release the connection.
            del.releaseConnection();
        }
        
    }

    /**
     * @see {@link GraphRepository#delete(String, QueryLanguage)}
     */
    public void delete(String query, QueryLanguage ql) throws Exception {

        if (query == null || ql == null) {
            return;
            
        }
        // DELETE
        DeleteMethod del = new DeleteMethod(servletURL);
        
        try {
            
            // add the header for the query
            if (query != null) {
                query = ql.toString().toLowerCase() + "[" + trim(query) + "]";
                String rangeHeader = "query[" + query + "]";
                Header range = new Header(
                        GraphRepositoryServlet.HTTP_RANGE, rangeHeader);
                del.addRequestHeader(range);
            }
            
            // Execute the method.
            int sc = getHttpClient().executeMethod(del);
            if (sc != HttpStatus.SC_OK) {
                throw new IOException("HTTP-DELETE failed: "
                        + del.getStatusLine());
            }
            
        } finally {
            // Release the connection.
            del.releaseConnection();
        }
        
    }

    private static final String twoSpaces = "  ".intern();

    private static final String oneSpace = " ".intern();

    private String trim(String s) {
        s = s.replace('\n', ' ');
        while (s.contains(twoSpaces)) {
            s = s.replace(twoSpaces, oneSpace);
        }
        return s;
    }

    private HttpClient getHttpClient() {
        if (httpClient == null) {
            httpClient =
                    new HttpClient(new MultiThreadedHttpConnectionManager());
            
            httpClient.getParams().setParameter(HttpMethodParams.SO_TIMEOUT,
                    new Integer(300000));
            // httpClient.getParams().setParameter(HttpMethodParams.HEAD_BODY_CHECK_TIMEOUT,
                    // new Integer(300000));
            httpClient.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                    new NeverRetryHandler());
            
            httpClient.getParams().setParameter(HttpClientParams.CONNECTION_MANAGER_TIMEOUT,
                    new Long(300000));
            
            httpClient.getParams().setParameter(HttpConnectionParams.CONNECTION_TIMEOUT,
                    new Integer(300000));
            httpClient.getParams().setParameter(HttpConnectionParams.SO_TIMEOUT, 
                    new Integer(300000));
        }
        if (proxyHost != null) {
            httpClient.getHostConfiguration().setProxyHost(proxyHost);
        }
        return httpClient;
    }

    private ProxyHost getProxyHost() {
        ProxyHost theProxyHost = null;
        ProxySelector ps = ProxySelector.getDefault();
        List<Proxy> p = null;
        // select the proxy for the URI of this repository
        try {
            if (ps != null) {
                // log.info( "Getting Proxy List." );
                p = ps.select(new java.net.URI(this.servletURL));
            }
        } catch (Exception e) {
            // log.warn( "Exception getting proxy: " + e.toString() );
        }
        if (p == null) {
            // log.warn( "No proxy information available." );
        } else {
            // log.info( "Received proxy list: " + p.toString() );
            Iterator<Proxy> proxies = p.iterator();
            // just take the first for now
            if (proxies != null && proxies.hasNext()) {
                Proxy theProxy = (Proxy) proxies.next();
                // log.info( "Proxy set to: " + theProxy.toString() );
                if (!Proxy.NO_PROXY.equals(theProxy)) {
                    InetSocketAddress theSock =
                            (InetSocketAddress) theProxy.address();
                    theProxyHost =
                            new ProxyHost(theSock.getHostName(), theSock
                                    .getPort());
                }
            } else {
                // log.warn( "Proxy list has zero members." );
            }
        }
        return theProxyHost;
    }

    private static class NeverRetryHandler implements HttpMethodRetryHandler {
        public boolean retryMethod(HttpMethod method, IOException ex, int count) {
            // for now, never
            return false;
        }
    }
}
