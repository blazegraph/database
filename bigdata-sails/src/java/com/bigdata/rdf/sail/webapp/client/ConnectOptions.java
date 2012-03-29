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
 * Created on Mar 27, 2012
 */

package com.bigdata.rdf.sail.webapp.client;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.ServiceProviderHook;

/**
 * Options for the HTTP connection.
 */
public class ConnectOptions {

    /** The URL of the SPARQL end point. */
    public final String serviceURL;

    /** The HTTP method (GET, POST, etc). */
    public String method = "POST";

    /**
     * The accept header (NO DEFAULT).
     */
    public String acceptHeader = null;

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

    /**
     * Used for NSS mutation operation responses.
     */
    public static final String MIME_APPLICATION_XML = "application/xml";

    static {

        ServiceProviderHook.forceLoad();

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

    /** Request parameters to be formatted as URL query parameters. */
    public Map<String, String[]> requestParams;

    /** Request entity. */
    public HttpEntity entity = null;

//    /**
//     * The connection timeout (ms) -or- ZERO (0) for an infinite timeout
//     * 
//     * FIXME How is this communicated using http components? (I do not believe
//     * that it is, in which case how is this settable; if people can do it
//     * themselves then drop it otherwise hook it into http components.)
//     * 
//     * It looks like the answer setting an appropriate parameter on the
//     * {@link DefaultHttpClient}
//     * 
//     * <pre>
//     * {@link org.apache.http.params.CoreConnectionPNames#TCP_NODELAY}
//     * </pre>
//     * 
//     * If so, then this can not be overridden here. It needs to be done for the
//     * {@link HttpClient} as a whole. That would suggest an SPI mechanism for
//     * configuring that client (or maybe doing it via the
//     * {@link RemoteServiceOptions}).
//     */
//    public int timeout = 0;

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

        return (requestParams != null && requestParams.containsKey(name)) ? requestParams
                .get(name)[0] : null;

    }

}
