/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2014.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.rdf.sail.webapp.client;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.message.BasicNameValuePair;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.rio.RDFFormat;

public class AbstractConnectOptions implements IMimeTypes {

    /** The URL of the remote REST service. */
    public final String serviceURL;

    /** The HTTP method (GET, POST, etc). */
    public String method = "POST";
    
    /** The Request Header value */
    public static final transient String ACCEPT_HEADER = "Accept";

    /**
     * When <code>true</code>, the request is a non-idempotent operation (an
     * UPDATE request or some kind) and must be directed to the leader for HA.
     * When <code>false</code>, the request is an idempotent operation and
     * should be load balanced over the available service for HA. This option
     * is ignored for non-HA requests.
     */
    public boolean update;
    
    /**
     * Used for {@link RDFFormat} responses.
     */
    public static final String DEFAULT_GRAPH_ACCEPT_HEADER;

    /**
     * Used for {@link TupleQueryResultFormat} responses.
     */
    public static final String DEFAULT_SOLUTIONS_ACCEPT_HEADER;

    /**
     * Used for {@link BooleanQueryResultFormat} responses.
     */
    public static final String DEFAULT_BOOLEAN_ACCEPT_HEADER;

//    /**
//     * Used for NSS mutation operation responses.
//     */
//    public static final String MIME_APPLICATION_XML = "application/xml";
    
//    /**
//     * Used to interchange {@link Properties} objects.
//     */
//    public static final String MIME_PROPERTIES_XML = "application/xml";

    static {

        /**
         * Note: This has been commented out. If it is included, then a lot of
         * the total code base gets dragged into the bigdata-client JAR. If this
         * creates a problem for clients, then we will need to examine the
         * bigdata RDF model and bigdata RDF parser packages carefully and
         * relayer them in order to decouple them from the rest of the code
         * base.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/628" >
         *      Create a bigdata-client jar for the NSS REST API </a>
         */
//        ServiceProviderHook.forceLoad();

        /*
         * FIXME We really need to know whether we are talking to a triple or
         * quads mode end point before we can set this to [true]. The preference
         * winds up as BINARY, which happens to support context so we are not
         * forcing bigdata (at least) into a choice that it can not make. We
         * might run into problems with non-bigdata end points. If they support
         * SPARQL Service Description, then we could figure out whether the end
         * point is triples or quads and setup the Accept header appropriately.
         * Otherwise we might have to raise this into the application (or the
         * ServiceFactory).
         */
        final boolean requireContext = false;

        DEFAULT_GRAPH_ACCEPT_HEADER = AcceptHeaderFactory
                .getDefaultGraphAcceptHeader(requireContext);

        DEFAULT_SOLUTIONS_ACCEPT_HEADER = AcceptHeaderFactory
                .getDefaultSolutionsAcceptHeader();

        DEFAULT_BOOLEAN_ACCEPT_HEADER = AcceptHeaderFactory
                .getDefaultBooleanAcceptHeader();

    }

    /**
     * Request parameters to be formatted as URL query parameters.
     */
    public Map<String, String[]> requestParams;

    /**
     * Optional request headers.
     */
    public Map<String, String> requestHeaders;

    @Override
    public String toString() {

        return getClass().getName()//
                + "{method=" + method//
                + ",serviceURL=" + serviceURL//
                + ",update=" + update//
                + ",requestParams=" + requestParams//
                + ",requestHeaders=" + requestHeaders//
                 + ",requestHeaders=" + requestHeaders//
                + "}";//

    }
    
    public AbstractConnectOptions(final String serviceURL) {

      if (serviceURL == null)
         throw new IllegalArgumentException();

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

        return (requestParams != null && requestParams.containsKey(name)) ? requestParams
                .get(name)[0] : null;

    }

    public void setHeader(final String name, final String val) {

        if (requestHeaders == null) {
            requestHeaders = new LinkedHashMap<String, String>();
        }

        requestHeaders.put(name, val);

    }

    public void setAcceptHeader(final String value) {

        setHeader(ACCEPT_HEADER, value);

    }

    public String getAcceptHeader() {

        return getHeader(ACCEPT_HEADER);

    }

    public String getHeader(final String name) {

        if (requestHeaders == null)
            return null;

        return requestHeaders.get(name);

    }
    
    /**
     * Add any URL query parameters (for a GET request).
     * 
     * @see #1097 (Extension REST API does not support multiple context values)
     * 
     * @see #getFormEntity(Map)
     */
    static public void addQueryParams(final StringBuilder urlString,
            final Map<String, String[]> requestParams)
            throws UnsupportedEncodingException {
        if (requestParams == null)
            return;
        boolean first = true;
        if (urlString.indexOf("?") >= 0) {
            first = false;
        }
        for (Map.Entry<String, String[]> e : requestParams.entrySet()) {
            final String name = e.getKey();
            final String[] vals = e.getValue();
            if (vals == null) {
                urlString.append(first ? "?" : "&");
                first = false;
                urlString.append(URLEncoder.encode(name, RemoteRepository.UTF8));
            } else {
                for (String val : vals) {
                    urlString.append(first ? "?" : "&");
                    first = false;
                    urlString.append(URLEncoder.encode(name, RemoteRepository.UTF8));
                    urlString.append("=");
                    if(val!=null)
                       urlString.append(URLEncoder.encode(val, RemoteRepository.UTF8));
                }
            }
        } // next Map.Entry

    }
    
    /**
     * Variant of {@link #addQueryParams(StringBuilder, Map)} that returns an
     * entity for a POST request.
     * <p>
     * Add query params to an {@link IMimeTypes#MIME_APPLICATION_URL_ENCODED}
     * entity.
     * 
     * @see #addQueryParams(StringBuilder, Map)
     * 
     * TODO Rename either this or {@link #addQueryParams(StringBuilder, Map)}
     * to align the method names since they serve the same purpose but for
     * GET vs POST.
     */
    public static HttpEntity getFormEntity(
          final Map<String, String[]> requestParams) throws Exception {

       final List<NameValuePair> formparams = new ArrayList<NameValuePair>();

       if (requestParams != null) {
          for (Map.Entry<String, String[]> e : requestParams.entrySet()) {
             final String name = e.getKey();
             final String[] vals = e.getValue();

             if (vals == null) {
                formparams.add(new BasicNameValuePair(name, null));
             } else {
                for (String val : vals) {
                   formparams.add(new BasicNameValuePair(name, val));
                }
             }
          } // next Map.Entry
       }

       return new UrlEncodedFormEntity(formparams, RemoteRepository.UTF8);

    }

    /**
     * Apply a UTF8 encoding to a component of a URL.
     * 
     * @param in
     *            The text to be encoded.
     * 
     * @return The UTF8 encoding of that text.
     * 
     * @throws RuntimeException
     *             if the {@link RemoteRepository#UTF8} encoding is not
     *             available.
     * @throws NullPointerException
     *             if the argument is <code>null</code>.
     */
    public static String urlEncode(final String in) {

        try {
        
            final String out = URLEncoder.encode(in, RemoteRepository.UTF8);
            
            return out;
            
        } catch (UnsupportedEncodingException e) {
            
            throw new RuntimeException(e);
            
        }

    }

}
