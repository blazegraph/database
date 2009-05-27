/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Apr 18, 2008
 */

package com.bigdata.util.httpd;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Vector;

/**
 * Overrides some methods on {@link NanoHTTPD} to (a) prevent serving files from
 * the local file system; and (b) to expose methods for handling GET, PUT, POST,
 * and DELETE requests - these methods all return a
 * {@link NanoHTTPD#HTTP_METHOD_NOT_ALLOWED} {@link Response} by default.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractHTTPD extends NanoHTTPD {
    
    public AbstractHTTPD(final int port) throws IOException {
        
        super(port);

    }

    public Response serve(final String uri, final String method,
            final Properties header,
            final LinkedHashMap<String, Vector<String>> parms) {

        if (log.isInfoEnabled())
            log.info(method + " '" + uri + "' ");

        try {

            if ("GET".equalsIgnoreCase(method)) {

                return doGet(uri, method, header, parms);

            } else if ("POST".equalsIgnoreCase(method)) {

                return doPost(uri, method, header, parms);

            } else if ("PUT".equalsIgnoreCase(method)) {

                return doPut(uri, method, header, parms);

            } else if ("DELETE".equalsIgnoreCase(method)) {

                return doDelete(uri, method, header, parms);

            } else {

                return new Response(HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
                        "" + method);

            }

        } catch (Throwable e) {

            log.error(e.getMessage(), e);

            final StringWriter w = new StringWriter();
            
            e.printStackTrace(new PrintWriter(w));
            
            /*
             * Note: if you send an HTTP_INTERNALERROR (500) the browser won't
             * display the response body so you have to send an Ok with the text
             * of the error message....
             */

            return new Response(HTTP_OK, MIME_TEXT_PLAIN, w.toString());

        }

    }

    /**
     * 
     * @param uri
     *            Percent-decoded URI without parameters, for example
     *            "/index.cgi"
     * @param method
     *            "GET", "POST" etc.
     * @param parms
     *            Parsed, percent decoded parameters from URI and, in case of
     *            POST, data. The keys are the parameter names. Each value is a
     *            {@link Collection} of {@link String}s containing the bindings
     *            for the named parameter. The order of the URL parameters is
     *            preserved.
     * @param header
     *            Header entries, percent decoded
     * @return
     * @throws Exception
     */
    public Response doGet(String uri, String method, Properties header,
            LinkedHashMap<String, Vector<String>> parms) throws Exception {

        return new Response(HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN, ""
                + method);

    }

    public Response doPost(String uri, String method, Properties header,
            LinkedHashMap<String, Vector<String>> parms) throws Exception {

        return new Response(HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN, ""
                + method);

    }

    public Response doPut(String uri, String method, Properties header,
            LinkedHashMap<String, Vector<String>> parms) throws Exception {

        return new Response(HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN, ""
                + method);

    }

    public Response doDelete(String uri, String method, Properties header,
            LinkedHashMap<String, Vector<String>> parms) throws Exception {

        return new Response(HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN, ""
                + method);

    }

}
